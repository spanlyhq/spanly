package spanly

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newSink(t *testing.T, apiKey, override string) *SpanlySink {
	t.Helper()
	s, err := NewSpanlySink(SpanlySinkOptions{APIKey: apiKey, IngestURL: override})
	if err != nil {
		t.Fatalf("NewSpanlySink: %v", err)
	}
	return s
}

func mustCollector(t *testing.T, opts CollectorOptions, sinks ...Sink) *Collector {
	t.Helper()
	c, err := NewCollector(opts, sinks...)
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}
	return c
}

func TestNewSpanlySinkRegionDetection(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"spanly_us_abc", defaultIngestURLUS + collectPath},
		{"spanly_eu_abc", defaultIngestURLEU + collectPath},
	}
	for _, tc := range tests {
		s, err := NewSpanlySink(SpanlySinkOptions{APIKey: tc.key})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.key, err)
		}
		if s.IngestURL() != tc.want {
			t.Errorf("key %q: ingestURL = %q, want %q", tc.key, s.IngestURL(), tc.want)
		}
	}
}

func TestNewSpanlySinkInvalidKey(t *testing.T) {
	if _, err := NewSpanlySink(SpanlySinkOptions{APIKey: "wrong_prefix"}); err == nil {
		t.Fatal("expected error for invalid key prefix")
	}
}

func TestNewSpanlySinkOverrideURL(t *testing.T) {
	s, err := NewSpanlySink(SpanlySinkOptions{APIKey: "wrong_prefix", IngestURL: "http://localhost:1234/"})
	if err != nil {
		t.Fatalf("override should bypass prefix validation: %v", err)
	}
	if s.IngestURL() != "http://localhost:1234/collect" {
		t.Errorf("ingestURL = %q", s.IngestURL())
	}
}

func TestNewCollectorRequiresIdentity(t *testing.T) {
	sink := newSink(t, "spanly_us_x", "")
	if _, err := NewCollector(CollectorOptions{}, sink); err == nil {
		t.Fatal("expected error when ClientID/MonitorID are empty")
	}
}

func TestNewCollectorRequiresAtLeastOneSink(t *testing.T) {
	if _, err := NewCollector(CollectorOptions{ClientID: "c", MonitorID: "m"}); err == nil {
		t.Fatal("expected error when no sinks are provided")
	}
}

func TestCollectPostsSpanlyPacket(t *testing.T) {
	var hits int32
	received := make(chan SpanlyPacket, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer spanly_us_key" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		if r.URL.Path != "/collect" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var p SpanlyPacket
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		received <- p
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	sink := newSink(t, "spanly_us_key", srv.URL)
	c := mustCollector(t, CollectorOptions{ClientID: "cid-1", MonitorID: "mid-1"}, sink)
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	transport := TransportContext{Type: "http", HTTPMethod: "post", Path: "/mcp", Headers: map[string]string{"x-a": "1"}}
	mcp := json.RawMessage(`{"jsonrpc":"2.0","method":"ping","id":1}`)
	c.Collect("from-client", PacketContext{}, transport, mcp)

	select {
	case p := <-received:
		if p.Direction != "from-client" {
			t.Errorf("Direction = %q", p.Direction)
		}
		if p.Context.SpanlyClientId != "cid-1" || p.Context.SpanlyMonitorId != "mid-1" {
			t.Errorf("Context = %+v", p.Context)
		}
		if p.TransportContext.Type != "http" || p.TransportContext.Path != "/mcp" {
			t.Errorf("TransportContext = %+v", p.TransportContext)
		}
		if p.Timestamp <= 0 {
			t.Errorf("Timestamp = %d", p.Timestamp)
		}
		if string(p.MCPPacket) != string(mcp) {
			t.Errorf("MCPPacket = %s", p.MCPPacket)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for collect POST")
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Errorf("expected 1 hit, got %d", hits)
	}
}

func TestCollectAppliesContextOverride(t *testing.T) {
	received := make(chan SpanlyPacket, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p SpanlyPacket
		_ = json.NewDecoder(r.Body).Decode(&p)
		received <- p
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	sink := newSink(t, "spanly_us_key", srv.URL)
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "default-mid"}, sink)
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	c.Collect("from-client",
		PacketContext{
			SpanlyMonitorId: "override-mid",
			EnvironmentId:   "tenant-A",
			ProjectId:       "proj-X",
		},
		TransportContext{Type: "http", Path: "/mcp"},
		json.RawMessage(`{"jsonrpc":"2.0"}`),
	)

	select {
	case p := <-received:
		if p.Context.SpanlyMonitorId != "override-mid" {
			t.Errorf("monitor ID = %q", p.Context.SpanlyMonitorId)
		}
		if p.Context.EnvironmentId != "tenant-A" {
			t.Errorf("environmentId = %q", p.Context.EnvironmentId)
		}
		if p.Context.ProjectId != "proj-X" {
			t.Errorf("projectId = %q", p.Context.ProjectId)
		}
		if p.Context.SpanlyClientId != "cid" {
			t.Errorf("clientId should keep default = %q", p.Context.SpanlyClientId)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestCollectStampsMonotonicSequence(t *testing.T) {
	received := make(chan SpanlyPacket, 3)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p SpanlyPacket
		_ = json.NewDecoder(r.Body).Decode(&p)
		received <- p
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	sink := newSink(t, "spanly_us_key", srv.URL)
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "mid"}, sink)
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	transport := TransportContext{Type: "stdio"}
	c.Collect("from-client", PacketContext{}, transport, json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	c.CollectOversized("to-client", PacketContext{}, transport, json.RawMessage(`{"jsonrpc":"2.0","id":1}`), 42)
	c.Collect("to-client", PacketContext{}, transport, json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{}}`))

	for want := uint64(1); want <= 3; want++ {
		select {
		case p := <-received:
			if p.Sequence != want {
				t.Errorf("Sequence = %d, want %d", p.Sequence, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for packet %d", want)
		}
	}
}

func TestCollectorBufferDropsWhenFull(t *testing.T) {
	// Block ingest so packets pile up in the buffer.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()
	defer close(block)

	sink := newSink(t, "spanly_us_key", srv.URL)
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "mid", BufferSize: 4}, sink)
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	mcp := json.RawMessage(`{"jsonrpc":"2.0"}`)
	for i := 0; i < 20; i++ {
		c.Collect("from-client", PacketContext{}, TransportContext{}, mcp)
	}

	// Drainer pulls one immediately and blocks on send; queue holds up to 4 more.
	// Remaining ~15 should be dropped.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Metrics().DroppedFull > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m := c.Metrics()
	if m.DroppedFull == 0 {
		t.Fatalf("expected drops, got metrics %+v", m)
	}
	if m.Collected+m.DroppedFull != 20 {
		t.Errorf("collected+dropped = %d, want 20 (metrics: %+v)", m.Collected+m.DroppedFull, m)
	}
}

func TestCloseFlushesQueuedPackets(t *testing.T) {
	// Slow ingest so packets are still queued when Close is called.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		hits.Add(1)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	sink := newSink(t, "spanly_us_key", srv.URL)
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "mid"}, sink)
	c.Start()

	for i := 0; i < 5; i++ {
		c.Collect("from-client", PacketContext{}, TransportContext{}, json.RawMessage(`{"jsonrpc":"2.0"}`))
	}
	if err := c.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 5 {
		t.Errorf("ingest received %d packets, want 5 (Close must flush the queue)", got)
	}
}

func TestCollectorRetriesOn5xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"upstream"}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()

	sink, err := NewSpanlySink(SpanlySinkOptions{
		APIKey:         "spanly_us_key",
		IngestURL:      srv.URL,
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "mid"}, sink)
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	c.Collect("from-client", PacketContext{}, TransportContext{}, json.RawMessage(`{"jsonrpc":"2.0"}`))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Metrics().Sent == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("packet never sent; metrics = %+v", c.Metrics())
}

// stubSink records every packet it sees and optionally errors out on Export.
// Used to verify multi-sink fan-out and isolation semantics.
type stubSink struct {
	name    string
	failErr error

	mu       sync.Mutex
	packets  []SpanlyPacket
	exported atomic.Int64
	failed   atomic.Int64
}

func (s *stubSink) Name() string { return s.name }

func (s *stubSink) Export(_ context.Context, p SpanlyPacket) error {
	s.mu.Lock()
	s.packets = append(s.packets, p)
	s.mu.Unlock()
	if s.failErr != nil {
		s.failed.Add(1)
		return s.failErr
	}
	s.exported.Add(1)
	return nil
}

func (s *stubSink) Shutdown(_ context.Context) error { return nil }

func (s *stubSink) Metrics() SinkMetrics {
	return SinkMetrics{
		Sent:           s.exported.Load(),
		Failed:         s.failed.Load(),
		AttemptsTotal:  s.exported.Load() + s.failed.Load(),
		AttemptsFailed: s.failed.Load(),
	}
}

func (s *stubSink) snapshot() []SpanlyPacket {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SpanlyPacket, len(s.packets))
	copy(out, s.packets)
	return out
}

func TestCollectorFansOutToAllSinks(t *testing.T) {
	a := &stubSink{name: "a"}
	b := &stubSink{name: "b", failErr: errors.New("nope")}
	c := mustCollector(t, CollectorOptions{ClientID: "cid", MonitorID: "mid"}, a, b)

	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })

	for i := 0; i < 3; i++ {
		c.Collect("from-client", PacketContext{}, TransportContext{}, json.RawMessage(`{"jsonrpc":"2.0"}`))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.snapshot()) == 3 && len(b.snapshot()) == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(a.snapshot()); got != 3 {
		t.Errorf("sink a saw %d packets, want 3", got)
	}
	if got := len(b.snapshot()); got != 3 {
		t.Errorf("sink b saw %d packets, want 3", got)
	}

	m := c.Metrics()
	if m.Sent != 3 {
		t.Errorf("aggregated Sent = %d, want 3 (only sink a succeeds)", m.Sent)
	}
	if m.Failed != 3 {
		t.Errorf("aggregated Failed = %d, want 3 (sink b errors every time)", m.Failed)
	}
}

func TestParseMCPPacket(t *testing.T) {
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"plain json rpc", `{"jsonrpc":"2.0","method":"x"}`, true},
		{"wrapped with whitespace", "  \n {\"jsonrpc\":\"2.0\"} \n", true},
		{"sse frame", "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1}\n\n", true},
		{"sse frame crlf", "event: message\r\ndata: {\"jsonrpc\":\"2.0\"}\r\n\r\n", true},
		{"not json rpc", `{"foo":"bar"}`, false},
		{"malformed", `{not json`, false},
		{"empty", ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := ParseMCPPacket([]byte(tc.in))
			if ok != tc.ok {
				t.Errorf("ParseMCPPacket(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			}
		})
	}
}

func TestPacketJSONFieldNames(t *testing.T) {
	p := SpanlyPacket{
		Timestamp: 1,
		Direction: "from-client",
		Context: PacketContext{
			SpanlyClientId:  "c",
			SpanlyMonitorId: "m",
			EnvironmentId:   "env",
			ProjectId:       "proj",
		},
		TransportContext: TransportContext{
			Type: "http", HTTPMethod: "post", Path: "/x", Headers: map[string]string{"a": "b"},
		},
		MCPPacket: json.RawMessage(`{"jsonrpc":"2.0"}`),
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, field := range []string{
		`"timestamp":`, `"direction":`, `"context":`, `"spanlyClientId":`,
		`"spanlyMonitorId":`, `"transportContext":`, `"httpMethod":`, `"mcpPacket":`,
		`"environmentId":`, `"projectId":`, `"sequence":`,
	} {
		if !strings.Contains(s, field) {
			t.Errorf("serialized packet missing field %s: %s", field, s)
		}
	}
}
