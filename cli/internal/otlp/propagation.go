package otlp

import (
	"context"
	"encoding/json"
	"strings"

	"go.opentelemetry.io/otel/propagation"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

// extractParentContext returns a context.Context carrying any upstream
// W3C trace context found in the packet. HTTP-mode packets get headers
// extracted via the standard TraceContext propagator; stdio-mode packets
// get them extracted from JSON-RPC `params._meta` per the official MCP
// semantic conventions. If no parent context is present, the input
// context is returned unchanged (a new root span will be created).
func extractParentContext(ctx context.Context, propagator propagation.TextMapPropagator, packet spanly.SpanlyPacket, parsed *parsedMCP) context.Context {
	switch packet.TransportContext.Type {
	case "stdio":
		return propagator.Extract(ctx, metaCarrier(parsed))
	default:
		return propagator.Extract(ctx, headerCarrier(packet.TransportContext.Headers))
	}
}

// headerCarrier adapts a plain map[string]string of HTTP headers to the
// TextMapCarrier interface. The Spanly proxy lowercases header keys when
// it captures them, so lookups normalize to lowercase to match the
// W3C TraceContext propagator's case-insensitive key expectations.
type headerCarrier map[string]string

func (h headerCarrier) Get(key string) string {
	if v, ok := h[strings.ToLower(key)]; ok {
		return v
	}
	return h[key]
}

func (h headerCarrier) Set(key, value string) { h[key] = value }

func (h headerCarrier) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

// metaCarrier reads W3C trace context headers (traceparent, tracestate,
// baggage) from a JSON-RPC message's params._meta object. Per the MCP
// semconv: "Instrumentations SHOULD propagate trace context inside MCP
// request `params._meta`."
//
// Read-only: Set is a no-op since we never inject into stdio packets at
// extraction time.
func metaCarrier(parsed *parsedMCP) propagation.TextMapCarrier {
	if parsed == nil || len(parsed.Params) == 0 {
		return emptyCarrier{}
	}
	var probe struct {
		Meta map[string]string `json:"_meta"`
	}
	if err := json.Unmarshal(parsed.Params, &probe); err != nil || len(probe.Meta) == 0 {
		return emptyCarrier{}
	}
	return mapCarrier(probe.Meta)
}

type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string {
	if v, ok := m[strings.ToLower(key)]; ok {
		return v
	}
	return m[key]
}

func (m mapCarrier) Set(key, value string) { m[key] = value }

func (m mapCarrier) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

type emptyCarrier struct{}

func (emptyCarrier) Get(string) string  { return "" }
func (emptyCarrier) Set(string, string) {}
func (emptyCarrier) Keys() []string     { return nil }
