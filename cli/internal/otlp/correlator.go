package otlp

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

const (
	// pendingMaxEntries caps the in-memory request → span map. When
	// reached, the oldest entry is evicted to make room. Sized for
	// thousands of concurrent in-flight requests per session.
	pendingMaxEntries = 10_000

	// pendingTTL is the maximum time a pending request span is kept
	// before being force-ended (status=Unset, no response). Protects
	// against memory leaks if a response packet never arrives.
	pendingTTL = 60 * time.Second

	// reapInterval is how often the eviction goroutine wakes up to
	// scan for expired pending spans.
	reapInterval = 10 * time.Second
)

// pendingKey identifies an in-flight request. monitorID disambiguates
// across sessions; jsonrpcID disambiguates within a session.
type pendingKey struct {
	monitorID  string
	jsonrpcID  string
}

type pendingSpan struct {
	span    trace.Span
	startedAt time.Time
}

// correlator turns a stream of one-way SpanlyPackets into request/response
// span pairs. Lifecycle:
//
//   - Request packet arrives  → start span, stash by id.
//   - Response packet arrives → look up by id, attach result attributes,
//     end span. Removes the entry.
//   - Notification packet     → emit one fire-and-forget span.
//   - Pending entry exceeds TTL → reaper ends it (status Unset) and
//     drops the entry.
//
// Safe for concurrent use; the Collector's fan-out may invoke handle()
// from multiple goroutines.
type correlator struct {
	tracer trace.Tracer

	mu      sync.Mutex
	pending map[pendingKey]*pendingSpan

	stopReap chan struct{}
	reapDone chan struct{}
}

func newCorrelator(tracer trace.Tracer) *correlator {
	c := &correlator{
		tracer:   tracer,
		pending:  make(map[pendingKey]*pendingSpan),
		stopReap: make(chan struct{}),
		reapDone: make(chan struct{}),
	}
	go c.reapLoop()
	return c
}

// shutdown stops the reaper and force-ends any still-pending spans so
// the OTel BatchSpanProcessor flushes them on Shutdown.
func (c *correlator) shutdown() {
	close(c.stopReap)
	<-c.reapDone
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, ps := range c.pending {
		ps.span.End()
		delete(c.pending, k)
	}
}

// handle processes one packet, starting/ending spans as appropriate.
// parentCtx should already carry any extracted upstream trace context.
func (c *correlator) handle(parentCtx context.Context, packet spanly.SpanlyPacket, parsed *parsedMCP) {
	switch parsed.kind() {
	case kindRequest:
		c.startRequestSpan(parentCtx, packet, parsed)
	case kindResponse:
		c.endResponseSpan(packet, parsed)
	case kindNotification:
		c.emitNotificationSpan(parentCtx, packet, parsed)
	}
}

func (c *correlator) startRequestSpan(parentCtx context.Context, packet spanly.SpanlyPacket, parsed *parsedMCP) {
	kind := requestSpanKind(packet.Direction)
	_, span := c.tracer.Start(parentCtx, spanName(parsed.Method, parsed.Params),
		trace.WithSpanKind(kind),
		trace.WithAttributes(requestAttributes(packet, parsed)...),
	)

	key := pendingKey{
		monitorID: packet.Context.SpanlyMonitorId,
		jsonrpcID: parsed.idString(),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) >= pendingMaxEntries {
		c.evictOldestLocked()
	}
	c.pending[key] = &pendingSpan{span: span, startedAt: time.Now()}
}

func (c *correlator) endResponseSpan(packet spanly.SpanlyPacket, parsed *parsedMCP) {
	key := pendingKey{
		monitorID: packet.Context.SpanlyMonitorId,
		jsonrpcID: parsed.idString(),
	}
	c.mu.Lock()
	ps, ok := c.pending[key]
	if ok {
		delete(c.pending, key)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	if attrs := responseAttributes(parsed); len(attrs) > 0 {
		ps.span.SetAttributes(attrs...)
	}
	if parsed.Error != nil {
		ps.span.SetStatus(codes.Error, errorCodeString(parsed.Error.Code))
	}
	ps.span.End()
}

func (c *correlator) emitNotificationSpan(parentCtx context.Context, packet spanly.SpanlyPacket, parsed *parsedMCP) {
	_, span := c.tracer.Start(parentCtx, spanName(parsed.Method, parsed.Params),
		trace.WithSpanKind(notificationSpanKind(packet.Direction)),
		trace.WithAttributes(requestAttributes(packet, parsed)...),
	)
	span.End()
}

// requestSpanKind decides the SpanKind for a JSON-RPC request based on
// who initiated it. Per the MCP semconv: client→server requests are
// Server (we received them); server→client requests (e.g.
// sampling/createMessage) get the reversed Client kind.
func requestSpanKind(direction string) trace.SpanKind {
	if direction == "to-client" {
		return trace.SpanKindClient
	}
	return trace.SpanKindServer
}

// notificationSpanKind mirrors requestSpanKind for fire-and-forget
// notifications. Producer (we emit) when sent server→client; Consumer
// when received client→server.
func notificationSpanKind(direction string) trace.SpanKind {
	if direction == "to-client" {
		return trace.SpanKindProducer
	}
	return trace.SpanKindConsumer
}

func (c *correlator) reapLoop() {
	defer close(c.reapDone)
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopReap:
			return
		case <-ticker.C:
			c.reapExpired()
		}
	}
}

func (c *correlator) reapExpired() {
	cutoff := time.Now().Add(-pendingTTL)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, ps := range c.pending {
		if ps.startedAt.Before(cutoff) {
			ps.span.End()
			delete(c.pending, k)
		}
	}
}

// evictOldestLocked removes the single oldest pending entry to make
// room. Caller must hold c.mu.
func (c *correlator) evictOldestLocked() {
	var oldestKey pendingKey
	var oldestAt time.Time
	first := true
	for k, ps := range c.pending {
		if first || ps.startedAt.Before(oldestAt) {
			oldestKey = k
			oldestAt = ps.startedAt
			first = false
		}
	}
	if !first {
		c.pending[oldestKey].span.End()
		delete(c.pending, oldestKey)
	}
}
