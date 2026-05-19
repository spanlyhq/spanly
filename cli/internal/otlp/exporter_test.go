package otlp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

func TestNewSinkDisabledWhenNoEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if _, err := NewSink(context.Background(), Options{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
}

func TestAssertHTTPProtocolRejectsGRPC(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	if err := assertHTTPProtocol(); err == nil {
		t.Fatal("expected error for protocol=grpc")
	}
}

func TestAssertHTTPProtocolAcceptsHTTPProtobuf(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	if err := assertHTTPProtocol(); err != nil {
		t.Errorf("unexpected error for protocol=http/protobuf: %v", err)
	}
}

func TestParseHeaderEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-api-key=abc, x-tenant=42")
	got := parseHeaderEnv()
	if got["x-api-key"] != "abc" || got["x-tenant"] != "42" {
		t.Errorf("parseHeaderEnv = %+v", got)
	}
}

func TestMergeHeadersOverrideWins(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	override := map[string]string{"b": "OVERRIDE", "c": "3"}
	got := mergeHeaders(base, override)
	if got["a"] != "1" || got["b"] != "OVERRIDE" || got["c"] != "3" {
		t.Errorf("merge = %+v", got)
	}
}

func TestSplitEndpointForms(t *testing.T) {
	cases := []struct {
		in     string
		host   string
		scheme string
	}{
		{"http://localhost:4318", "localhost:4318", "http"},
		{"https://otel.example:4318", "otel.example:4318", "https"},
		{"localhost:4318", "localhost:4318", ""},
	}
	for _, tc := range cases {
		host, scheme, err := splitEndpoint(tc.in)
		if err != nil {
			t.Errorf("splitEndpoint(%q) error: %v", tc.in, err)
			continue
		}
		if host != tc.host || scheme != tc.scheme {
			t.Errorf("splitEndpoint(%q) = (%q,%q), want (%q,%q)",
				tc.in, host, scheme, tc.host, tc.scheme)
		}
	}
}

// TestSinkExportsToFakeOTLPReceiver wires a real OTLP HTTP receiver and
// verifies that a request/response packet pair produces an OTLP/protobuf
// payload at /v1/traces. Span content checks are done elsewhere with the
// in-memory recorder; this test only proves the wire-level integration
// works (URL path, content-type, body size > 0).
func TestSinkExportsToFakeOTLPReceiver(t *testing.T) {
	var hits atomic.Int32
	bodySeen := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v1/traces") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); !strings.Contains(ct, "application/x-protobuf") {
			t.Errorf("Content-Type = %q, want application/x-protobuf", ct)
		}
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			t.Error("empty body")
		}
		hits.Add(1)
		select {
		case bodySeen <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	sink, err := NewSink(context.Background(), Options{Endpoint: srv.URL, Insecure: true})
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}

	reqPkt := spanly.SpanlyPacket{
		Direction:        "from-client",
		Context:          spanly.PacketContext{SpanlyMonitorId: "mid"},
		TransportContext: spanly.TransportContext{Type: "http"},
		MCPPacket:        json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}`),
	}
	respPkt := reqPkt
	respPkt.Direction = "to-client"
	respPkt.MCPPacket = json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`)

	if err := sink.Export(context.Background(), reqPkt); err != nil {
		t.Fatalf("Export request: %v", err)
	}
	if err := sink.Export(context.Background(), respPkt); err != nil {
		t.Fatalf("Export response: %v", err)
	}

	// Force the BatchSpanProcessor to flush by calling Shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sink.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	select {
	case <-bodySeen:
	case <-time.After(2 * time.Second):
		t.Fatal("OTLP receiver never saw a payload")
	}
	if got := hits.Load(); got == 0 {
		t.Errorf("expected at least one POST to receiver, got %d", got)
	}

	m := sink.Metrics()
	if m.Sent != 2 {
		t.Errorf("Sent = %d, want 2", m.Sent)
	}
}

func TestSinkExportRejectsMalformedPacket(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := NewSink(context.Background(), Options{Endpoint: srv.URL, Insecure: true})
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	defer sink.Shutdown(context.Background())

	bad := spanly.SpanlyPacket{
		Direction: "from-client",
		MCPPacket: json.RawMessage(`{"not":"jsonrpc"}`),
	}
	if err := sink.Export(context.Background(), bad); err == nil {
		t.Error("expected error for malformed packet")
	}
	if got := sink.Metrics().Failed; got != 1 {
		t.Errorf("Failed = %d, want 1", got)
	}
}

// TestSinkSatisfiesSpanlySinkInterface is a compile-time check that
// *Sink can be used wherever spanly.Sink is required.
func TestSinkSatisfiesSpanlySinkInterface(t *testing.T) {
	var _ spanly.Sink = (*Sink)(nil)
}
