package otlp

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

const sampledTraceparent = "00-12345678901234567890123456789012-1234567890123456-01"

func TestExtractParentContextFromHTTPHeaders(t *testing.T) {
	prop := propagation.TraceContext{}
	pkt := spanly.SpanlyPacket{
		TransportContext: spanly.TransportContext{
			Type: "http",
			Headers: map[string]string{
				"traceparent": sampledTraceparent,
			},
		},
	}
	parsed := &parsedMCP{JSONRPC: "2.0"}
	ctx := extractParentContext(context.Background(), prop, pkt, parsed)
	span := assertContextHasRemoteSpan(t, ctx)
	if got := span.TraceID().String(); got != "12345678901234567890123456789012" {
		t.Errorf("traceID = %q, want extracted from traceparent", got)
	}
}

func TestExtractParentContextFromMetaInStdio(t *testing.T) {
	prop := propagation.TraceContext{}
	pkt := spanly.SpanlyPacket{
		TransportContext: spanly.TransportContext{Type: "stdio"},
	}
	parsed := &parsedMCP{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"x","_meta":{"traceparent":"` + sampledTraceparent + `"}}`),
	}
	ctx := extractParentContext(context.Background(), prop, pkt, parsed)
	span := assertContextHasRemoteSpan(t, ctx)
	if got := span.TraceID().String(); got != "12345678901234567890123456789012" {
		t.Errorf("traceID = %q, want extracted from _meta.traceparent", got)
	}
}

func TestExtractParentContextNoMetaProducesRoot(t *testing.T) {
	prop := propagation.TraceContext{}
	pkt := spanly.SpanlyPacket{
		TransportContext: spanly.TransportContext{Type: "stdio"},
	}
	parsed := &parsedMCP{JSONRPC: "2.0", Method: "ping"}
	ctx := extractParentContext(context.Background(), prop, pkt, parsed)

	rec := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_, span := tp.Tracer("t").Start(ctx, "child")
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 span, got %d", len(ended))
	}
	if ended[0].Parent().IsValid() {
		t.Errorf("expected root span (no remote parent), got parent traceID=%s spanID=%s",
			ended[0].Parent().TraceID(), ended[0].Parent().SpanID())
	}
}

func TestHeaderCarrierLowercasesLookup(t *testing.T) {
	h := headerCarrier{"x-tenant": "abc"}
	if got := h.Get("X-Tenant"); got != "abc" {
		t.Errorf("Get(X-Tenant) = %q, want abc", got)
	}
	if got := h.Get("traceparent"); got != "" {
		t.Errorf("Get(traceparent) on missing key = %q, want empty", got)
	}
}

// assertContextHasRemoteSpan starts a child span from ctx and returns
// its parent SpanContext, failing the test if no remote parent was
// extracted.
func assertContextHasRemoteSpan(t *testing.T, ctx context.Context) (sc spanContext) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("t").Start(ctx, "child")
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 span, got %d", len(ended))
	}
	parent := ended[0].Parent()
	if !parent.IsValid() {
		t.Fatal("expected a remote parent span context, got none")
	}
	return spanContext{
		traceID: parent.TraceID().String(),
		spanID:  parent.SpanID().String(),
	}
}

type spanContext struct {
	traceID string
	spanID  string
}

func (s spanContext) TraceID() traceID { return traceID(s.traceID) }

type traceID string

func (t traceID) String() string { return string(t) }
