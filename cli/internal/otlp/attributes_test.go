package otlp

import (
	"encoding/json"
	"testing"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

func TestParseMCPClassification(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want messageKind
	}{
		{"request", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}`, kindRequest},
		{"response result", `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`, kindResponse},
		{"response error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`, kindResponse},
		{"notification", `{"jsonrpc":"2.0","method":"notifications/cancelled"}`, kindNotification},
		{"null id is notification-shaped", `{"jsonrpc":"2.0","id":null,"method":"x"}`, kindNotification},
		{"missing jsonrpc rejected", `{"method":"tools/call","id":1}`, kindUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := parseMCP(json.RawMessage(tc.in))
			if tc.want == kindUnknown {
				if parsed != nil {
					t.Fatalf("expected parseMCP to reject %q, got %+v", tc.in, parsed)
				}
				return
			}
			if parsed == nil {
				t.Fatalf("parseMCP(%q) returned nil", tc.in)
			}
			if got := parsed.kind(); got != tc.want {
				t.Errorf("kind = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSpanName(t *testing.T) {
	cases := []struct {
		name   string
		method string
		params string
		want   string
	}{
		{"tools/call uses name", "tools/call", `{"name":"get-weather"}`, "tools/call get-weather"},
		{"prompts/get uses name", "prompts/get", `{"name":"summarize"}`, "prompts/get summarize"},
		{"resources/read drops uri (cardinality)", "resources/read", `{"uri":"file:///etc/passwd"}`, "resources/read"},
		{"unknown method falls back", "ping", ``, "ping"},
		{"empty method falls back to mcp", "", ``, "mcp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := spanName(tc.method, json.RawMessage(tc.params))
			if got != tc.want {
				t.Errorf("spanName(%q,%q) = %q, want %q", tc.method, tc.params, got, tc.want)
			}
		})
	}
}

func TestRequestAttributesPopulatesSemconv(t *testing.T) {
	parsed := parseMCP(json.RawMessage(`{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"get-weather","arguments":{}}}`))
	if parsed == nil {
		t.Fatal("parseMCP returned nil")
	}
	packet := spanly.SpanlyPacket{
		Direction: "from-client",
		Context: spanly.PacketContext{
			SpanlyMonitorId: "session-abc",
			EnvironmentId:   "tenant-1",
		},
		TransportContext: spanly.TransportContext{Type: "http", Path: "/mcp"},
	}
	attrs := requestAttributes(packet, parsed)

	got := map[string]string{}
	for _, kv := range attrs {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	wants := map[string]string{
		"mcp.method.name":              "tools/call",
		"jsonrpc.request.id":           "42",
		"mcp.session.id":               "session-abc",
		"gen_ai.tool.name":             "get-weather",
		"server.address":               "/mcp",
		"spanly.tenant.environment_id": "tenant-1",
		"spanly.direction":             "from-client",
	}
	for k, v := range wants {
		if got[k] != v {
			t.Errorf("attribute %q = %q, want %q (got map: %+v)", k, got[k], v, got)
		}
	}
}

func TestResponseAttributesAttachErrorOnError(t *testing.T) {
	parsed := parseMCP(json.RawMessage(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`))
	if parsed == nil {
		t.Fatal("parseMCP returned nil")
	}
	attrs := responseAttributes(parsed)
	if len(attrs) != 2 {
		t.Fatalf("expected 2 attributes, got %d (%+v)", len(attrs), attrs)
	}
}

func TestResponseAttributesNilOnSuccess(t *testing.T) {
	parsed := parseMCP(json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
	if parsed == nil {
		t.Fatal("parseMCP returned nil")
	}
	if attrs := responseAttributes(parsed); attrs != nil {
		t.Errorf("expected nil for success response, got %+v", attrs)
	}
}

func TestIDStringNormalizesStringAndNumber(t *testing.T) {
	cases := map[string]string{
		`"abc"`: "abc",
		`42`:    "42",
		`null`:  "null",
		``:      "",
	}
	for in, want := range cases {
		p := &parsedMCP{ID: json.RawMessage(in)}
		if got := p.idString(); got != want {
			t.Errorf("idString(%q) = %q, want %q", in, got, want)
		}
	}
}
