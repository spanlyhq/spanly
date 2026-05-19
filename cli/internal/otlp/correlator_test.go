package otlp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

// newTestTracer wires an in-memory span recorder so tests can assert
// on emitted span shape without needing a network exporter.
func newTestTracer(t *testing.T) (*tracetest.SpanRecorder, *trace.TracerProvider) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return rec, tp
}

func packetWithMCP(direction string, raw string) (spanly.SpanlyPacket, *parsedMCP) {
	pkt := spanly.SpanlyPacket{
		Direction:        direction,
		Context:          spanly.PacketContext{SpanlyMonitorId: "mid"},
		TransportContext: spanly.TransportContext{Type: "http"},
		MCPPacket:        json.RawMessage(raw),
	}
	return pkt, parseMCP(pkt.MCPPacket)
}

func TestCorrelatorEmitsOneSpanPerRequestResponse(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	reqPkt, reqParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get-weather"}}`)
	respPkt, respParsed := packetWithMCP("to-client",
		`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`)

	c.handle(context.Background(), reqPkt, reqParsed)
	if got := len(rec.Ended()); got != 0 {
		t.Fatalf("span ended before response, got %d ended spans", got)
	}
	c.handle(context.Background(), respPkt, respParsed)

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	span := ended[0]
	if span.Name() != "tools/call get-weather" {
		t.Errorf("name = %q, want %q", span.Name(), "tools/call get-weather")
	}
	if span.Status().Code != codes.Unset {
		t.Errorf("status = %v, want Unset (success response)", span.Status().Code)
	}
}

func TestCorrelatorMarksErrorResponse(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	reqPkt, reqParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x"}}`)
	respPkt, respParsed := packetWithMCP("to-client",
		`{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"method not found"}}`)

	c.handle(context.Background(), reqPkt, reqParsed)
	c.handle(context.Background(), respPkt, respParsed)

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	if ended[0].Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", ended[0].Status().Code)
	}
}

func TestCorrelatorEmitsNotificationImmediately(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	pkt, parsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`)
	c.handle(context.Background(), pkt, parsed)

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	if ended[0].Name() != "notifications/cancelled" {
		t.Errorf("name = %q", ended[0].Name())
	}
}

func TestCorrelatorOrphanedRequestStaysPendingUntilShutdown(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))

	reqPkt, reqParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":"x"}}`)
	c.handle(context.Background(), reqPkt, reqParsed)
	if got := len(rec.Ended()); got != 0 {
		t.Fatalf("expected 0 ended spans pre-shutdown, got %d", got)
	}

	c.shutdown()
	if got := len(rec.Ended()); got != 1 {
		t.Errorf("expected 1 ended span post-shutdown (orphan flush), got %d", got)
	}
}

func TestCorrelatorReapEvictsExpired(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	reqPkt, reqParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":1,"method":"x"}`)
	c.handle(context.Background(), reqPkt, reqParsed)

	// Backdate the pending entry so it's "older than TTL," then trigger reap.
	c.mu.Lock()
	for _, ps := range c.pending {
		ps.startedAt = time.Now().Add(-2 * pendingTTL)
	}
	c.mu.Unlock()

	c.reapExpired()

	if got := len(rec.Ended()); got != 1 {
		t.Errorf("expected reaper to end 1 span, got %d", got)
	}
	c.mu.Lock()
	remaining := len(c.pending)
	c.mu.Unlock()
	if remaining != 0 {
		t.Errorf("expected 0 pending entries after reap, got %d", remaining)
	}
}

func TestCorrelatorCapEvictsOldest(t *testing.T) {
	_, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	// Synthetically fill the map up to the cap.
	c.mu.Lock()
	for i := 0; i < pendingMaxEntries; i++ {
		_, span := tp.Tracer("test").Start(context.Background(), "filler")
		c.pending[pendingKey{monitorID: "mid", jsonrpcID: jsonrpcIDStr(i)}] = &pendingSpan{
			span:      span,
			startedAt: time.Now().Add(time.Duration(-i) * time.Second),
		}
	}
	c.mu.Unlock()

	reqPkt, reqParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":"new","method":"x"}`)
	c.handle(context.Background(), reqPkt, reqParsed)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) > pendingMaxEntries {
		t.Errorf("pending size %d exceeded cap %d", len(c.pending), pendingMaxEntries)
	}
	if _, ok := c.pending[pendingKey{monitorID: "mid", jsonrpcID: "new"}]; !ok {
		t.Error("newly-added request was evicted instead of an older one")
	}
}

func jsonrpcIDStr(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func TestCorrelatorRequestSpanKindReversesForToClient(t *testing.T) {
	rec, tp := newTestTracer(t)
	c := newCorrelator(tp.Tracer("test"))
	defer c.shutdown()

	// Server-initiated request (e.g. sampling/createMessage) flows
	// to-client and should produce SpanKindClient.
	reqPkt, reqParsed := packetWithMCP("to-client",
		`{"jsonrpc":"2.0","id":1,"method":"sampling/createMessage","params":{}}`)
	respPkt, respParsed := packetWithMCP("from-client",
		`{"jsonrpc":"2.0","id":1,"result":{}}`)
	c.handle(context.Background(), reqPkt, reqParsed)
	c.handle(context.Background(), respPkt, respParsed)

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	if got := ended[0].SpanKind().String(); got != "client" {
		t.Errorf("SpanKind = %q, want %q", got, "client")
	}
}
