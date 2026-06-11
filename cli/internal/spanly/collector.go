// Package spanly contains the shared protocol types and proxy primitives
// used by both the `run` and `proxy` subcommands.
package spanly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize = 10000
)

type TransportContext struct {
	Type       string            `json:"type"`
	HTTPMethod string            `json:"httpMethod,omitempty"`
	Path       string            `json:"path,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// PacketContext carries identity and tenant attribution for a packet.
// SpanlyClientId and SpanlyMonitorId are populated by the collector's
// defaults; per-request overrides for any field can be supplied via
// Collect's `override` argument (Phase 2: --context-header flag).
type PacketContext struct {
	SpanlyClientId  string `json:"spanlyClientId"`
	SpanlyMonitorId string `json:"spanlyMonitorId"`
	ProjectId       string `json:"projectId,omitempty"`
	EnvironmentId   string `json:"environmentId,omitempty"`
	OrganisationId  string `json:"organisationId,omitempty"`
}

type SpanlyPacket struct {
	Timestamp        int64            `json:"timestamp"`
	Direction        string           `json:"direction"`
	Context          PacketContext    `json:"context"`
	TransportContext TransportContext `json:"transportContext"`
	MCPPacket        json.RawMessage  `json:"mcpPacket"`
}

// Sink consumes packets exported from the Collector. Implementations
// may batch, retry, and ship to any backend; failures are isolated —
// a failure in one Sink does not affect others. Each Sink reports its
// own counters via Metrics(), which the Collector aggregates.
type Sink interface {
	Export(ctx context.Context, packet SpanlyPacket) error
	Shutdown(ctx context.Context) error
	Name() string
	Metrics() SinkMetrics
}

// SinkMetrics is a snapshot of per-sink counters. AttemptsTotal /
// AttemptsFailed reflect transport-level attempts including any
// internal retry; for sinks without internal retry, AttemptsTotal
// equals Sent+Failed.
type SinkMetrics struct {
	Sent           int64
	Failed         int64
	AttemptsTotal  int64
	AttemptsFailed int64
}

// CollectorOptions configures a Collector.
//
// ClientID and MonitorID populate the PacketContext defaults on every
// packet. BufferSize controls the bounded in-memory queue that decouples
// Collect() from the sink fan-out (zero falls back to a safe default).
type CollectorOptions struct {
	ClientID   string
	MonitorID  string
	BufferSize int
}

// CollectorMetrics is a snapshot of counters maintained by the Collector.
// Returned by Collector.Metrics(). Collected/DroppedFull/BufferDepth are
// queue-level; Sent/Failed/AttemptsTotal/AttemptsFailed are aggregated
// across all configured sinks.
type CollectorMetrics struct {
	Collected      int64
	DroppedFull    int64
	Sent           int64
	Failed         int64
	BufferDepth    int64
	AttemptsTotal  int64
	AttemptsFailed int64
}

type Collector struct {
	opts  CollectorOptions
	sinks []Sink

	queue chan SpanlyPacket

	started      bool
	stop         chan struct{}
	drainerDone  chan struct{}
	exportCtx    context.Context
	cancelExport context.CancelFunc

	collected   atomic.Int64
	droppedFull atomic.Int64
}

// NewCollector returns a Collector that fans packets out to one or more
// Sinks via a bounded in-memory queue. At least one Sink must be passed —
// a Collector with no sinks would silently drop everything.
func NewCollector(opts CollectorOptions, sinks ...Sink) (*Collector, error) {
	if opts.ClientID == "" || opts.MonitorID == "" {
		return nil, errEmptyIdentity
	}
	if len(sinks) == 0 {
		return nil, errNoSinks
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	exportCtx, cancelExport := context.WithCancel(context.Background())
	return &Collector{
		opts:         opts,
		sinks:        sinks,
		queue:        make(chan SpanlyPacket, opts.BufferSize),
		stop:         make(chan struct{}),
		drainerDone:  make(chan struct{}),
		exportCtx:    exportCtx,
		cancelExport: cancelExport,
	}, nil
}

// Sinks returns the configured sinks (read-only). Useful for callers
// that want to log endpoints / names at startup.
func (c *Collector) Sinks() []Sink { return c.sinks }

// Start launches the drainer goroutine that pulls packets off the queue
// and exports each to every Sink concurrently. Returns immediately.
// Pair with Close to flush and release resources.
func (c *Collector) Start() {
	c.started = true
	go c.drainLoop()
}

// Close flushes the queue and shuts every sink down. The drainer keeps
// exporting until the queue is empty; if that takes longer than grace,
// in-flight exports are aborted and remaining packets dropped. Safe to
// call exactly once, after Start.
func (c *Collector) Close(grace time.Duration) error {
	if c.started {
		close(c.stop)
		select {
		case <-c.drainerDone:
		case <-time.After(grace):
			c.cancelExport()
			<-c.drainerDone
		}
	}
	c.cancelExport()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var errs []error
	for _, s := range c.sinks {
		if err := s.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Collect enqueues a packet for delivery. If the queue is full the packet
// is dropped (newest-dropped policy) and DroppedFull is incremented.
//
// override carries per-request context overrides — non-empty fields take
// precedence over the collector defaults. Pass a zero-value PacketContext
// to use only the collector defaults.
func (c *Collector) Collect(direction string, override PacketContext, transport TransportContext, mcp json.RawMessage) {
	ctx := PacketContext{
		SpanlyClientId:  c.opts.ClientID,
		SpanlyMonitorId: c.opts.MonitorID,
	}
	if override.SpanlyClientId != "" {
		ctx.SpanlyClientId = override.SpanlyClientId
	}
	if override.SpanlyMonitorId != "" {
		ctx.SpanlyMonitorId = override.SpanlyMonitorId
	}
	if override.ProjectId != "" {
		ctx.ProjectId = override.ProjectId
	}
	if override.EnvironmentId != "" {
		ctx.EnvironmentId = override.EnvironmentId
	}
	if override.OrganisationId != "" {
		ctx.OrganisationId = override.OrganisationId
	}

	packet := SpanlyPacket{
		Timestamp:        time.Now().UnixMilli(),
		Direction:        direction,
		Context:          ctx,
		TransportContext: transport,
		MCPPacket:        mcp,
	}

	select {
	case c.queue <- packet:
		c.collected.Add(1)
	default:
		c.droppedFull.Add(1)
	}
}

func (c *Collector) drainLoop() {
	defer close(c.drainerDone)
	for {
		select {
		case packet := <-c.queue:
			c.fanOut(c.exportCtx, packet)
		case <-c.stop:
			// Flush whatever is already queued, then exit. Close aborts
			// this via exportCtx if it outlives the grace period.
			for {
				select {
				case packet := <-c.queue:
					c.fanOut(c.exportCtx, packet)
				default:
					return
				}
			}
		}
	}
}

// fanOut exports a packet to every sink concurrently and waits for all
// to complete. A failure in one sink is isolated — sinks log/count their
// own errors via the SinkMetrics counters.
func (c *Collector) fanOut(ctx context.Context, packet SpanlyPacket) {
	if len(c.sinks) == 1 {
		_ = c.sinks[0].Export(ctx, packet)
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(c.sinks))
	for _, s := range c.sinks {
		go func(s Sink) {
			defer wg.Done()
			_ = s.Export(ctx, packet)
		}(s)
	}
	wg.Wait()
}

func (c *Collector) Metrics() CollectorMetrics {
	m := CollectorMetrics{
		Collected:   c.collected.Load(),
		DroppedFull: c.droppedFull.Load(),
		BufferDepth: int64(len(c.queue)),
	}
	for _, s := range c.sinks {
		sm := s.Metrics()
		m.Sent += sm.Sent
		m.Failed += sm.Failed
		m.AttemptsTotal += sm.AttemptsTotal
		m.AttemptsFailed += sm.AttemptsFailed
	}
	return m
}

// ParseMCPPacket extracts a JSON-RPC 2.0 object from a request/response
// body or SSE frame. Returns the raw JSON and true on match, nil/false
// otherwise. Used by the proxy on inspect-eligible paths only.
func ParseMCPPacket(body []byte) (json.RawMessage, bool) {
	raw := extractJSON(body)
	if raw == nil {
		return nil, false
	}
	var probe struct {
		JSONRPC string `json:"jsonrpc"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil || probe.JSONRPC != "2.0" {
		return nil, false
	}
	return raw, true
}

func extractJSON(body []byte) json.RawMessage {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return json.RawMessage(trimmed)
	}
	for _, line := range bytes.Split(trimmed, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data:")) {
			payload := bytes.TrimSpace(line[len("data:"):])
			if len(payload) > 0 {
				return json.RawMessage(payload)
			}
		}
	}
	return nil
}
