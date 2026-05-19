// Package otlp implements an OpenTelemetry exporter Sink for the Spanly
// Collector. Each MCP JSON-RPC request/response pair is exported as a
// single OTel span; notifications are exported as fire-and-forget spans.
//
// Span shape and attribute names follow the official MCP semantic
// conventions merged into OTel on 2026-01-12 (semconv v1.41.0):
// https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/
package otlp

import (
	"encoding/json"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

// Tenant attribute keys for Spanly's PacketContext fields. These are
// not part of the official semconv (no spec for cross-tenant isolation
// in MCP yet), so they live under a Spanly-namespaced ad-hoc prefix.
const (
	attrTenantEnvironment  = "spanly.tenant.environment_id"
	attrTenantProject      = "spanly.tenant.project_id"
	attrTenantOrganisation = "spanly.tenant.organisation_id"
	attrSpanlyDirection    = "spanly.direction"
)

// parsedMCP is the subset of a JSON-RPC 2.0 message we care about for
// span shaping. Method+ID present = request; ID present + (Result|Error)
// = response; Method only (no ID) = notification.
type parsedMCP struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// kind classifies a parsed MCP message into one of the three shapes the
// correlator needs to distinguish.
type messageKind int

const (
	kindUnknown messageKind = iota
	kindRequest
	kindResponse
	kindNotification
)

func (p *parsedMCP) kind() messageKind {
	hasID := len(p.ID) > 0 && string(p.ID) != "null"
	hasMethod := p.Method != ""
	hasResult := len(p.Result) > 0 || p.Error != nil
	switch {
	case hasMethod && hasID:
		return kindRequest
	case hasResult && hasID:
		return kindResponse
	case hasMethod && !hasID:
		return kindNotification
	default:
		return kindUnknown
	}
}

// idString normalizes the JSON-RPC id (which may be a string or number)
// to its raw textual representation, with surrounding quotes stripped
// from string ids so map keys behave predictably.
func (p *parsedMCP) idString() string {
	if len(p.ID) == 0 {
		return ""
	}
	s := string(p.ID)
	// Try string id: "abc" → abc.
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var unquoted string
		if err := json.Unmarshal(p.ID, &unquoted); err == nil {
			return unquoted
		}
	}
	return s
}

// parseMCP unmarshals the envelope. Returns nil on malformed input —
// callers should treat that as "skip OTLP for this packet".
func parseMCP(raw json.RawMessage) *parsedMCP {
	if len(raw) == 0 {
		return nil
	}
	var p parsedMCP
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil
	}
	if p.JSONRPC != "2.0" {
		return nil
	}
	return &p
}

// spanName builds the OTel span name per spec:
// "{method} {target}" for low-cardinality targets (tool name, prompt name),
// just "{method}" otherwise. Resource URIs are intentionally omitted from
// the name to avoid metric cardinality explosion.
func spanName(method string, params json.RawMessage) string {
	if method == "" {
		return "mcp"
	}
	if target := lowCardinalityTarget(method, params); target != "" {
		return method + " " + target
	}
	return method
}

// lowCardinalityTarget returns the suffix to append to the span name for
// methods that have a stable, low-cardinality target identifier.
func lowCardinalityTarget(method string, params json.RawMessage) string {
	switch method {
	case "tools/call", "prompts/get":
		return paramName(params)
	}
	return ""
}

// paramName extracts the "name" string from a params object, used for
// tools/call and prompts/get. Returns "" if not present or malformed.
func paramName(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var probe struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(params, &probe); err != nil {
		return ""
	}
	return probe.Name
}

// paramURI extracts the "uri" string from a params object, used for
// resources/read.
func paramURI(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var probe struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(params, &probe); err != nil {
		return ""
	}
	return probe.URI
}

// requestAttributes builds the OTel attribute set for a request/notification
// span at start time. Result/error attributes are added later in
// responseAttributes when the response packet arrives.
func requestAttributes(packet spanly.SpanlyPacket, parsed *parsedMCP) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 8)
	if parsed.Method != "" {
		attrs = append(attrs, semconv.McpMethodNameKey.String(parsed.Method))
	}
	if parsed.kind() == kindRequest {
		if id := parsed.idString(); id != "" {
			attrs = append(attrs, semconv.JSONRPCRequestID(id))
		}
	}
	if sid := packet.Context.SpanlyMonitorId; sid != "" {
		attrs = append(attrs, semconv.McpSessionIDKey.String(sid))
	}
	if name := paramName(parsed.Params); name != "" && parsed.Method == "tools/call" {
		attrs = append(attrs, semconv.GenAIToolName(name))
	}
	if uri := paramURI(parsed.Params); uri != "" {
		attrs = append(attrs, semconv.McpResourceURIKey.String(uri))
	}
	if addr := packet.TransportContext.Path; addr != "" {
		attrs = append(attrs, semconv.ServerAddressKey.String(addr))
	}
	if packet.Context.EnvironmentId != "" {
		attrs = append(attrs, attribute.String(attrTenantEnvironment, packet.Context.EnvironmentId))
	}
	if packet.Context.ProjectId != "" {
		attrs = append(attrs, attribute.String(attrTenantProject, packet.Context.ProjectId))
	}
	if packet.Context.OrganisationId != "" {
		attrs = append(attrs, attribute.String(attrTenantOrganisation, packet.Context.OrganisationId))
	}
	if packet.Direction != "" {
		attrs = append(attrs, attribute.String(attrSpanlyDirection, packet.Direction))
	}
	return attrs
}

// responseAttributes builds the additional attributes attached when a
// response packet closes a previously-started request span. Includes the
// JSON-RPC error code/message if the response is an error.
func responseAttributes(parsed *parsedMCP) []attribute.KeyValue {
	if parsed.Error == nil {
		return nil
	}
	return []attribute.KeyValue{
		attribute.Int("rpc.jsonrpc.error_code", parsed.Error.Code),
		attribute.String("rpc.jsonrpc.error_message", parsed.Error.Message),
	}
}

// errorCodeString renders a JSON-RPC error code for use as the OTel span
// status description. Kept short — the full message is on the attribute.
func errorCodeString(code int) string { return strconv.Itoa(code) }
