package spanly

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeIngest struct {
	mu      sync.Mutex
	packets []SpanlyPacket
	srv     *httptest.Server
	done    chan struct{}
}

func newFakeIngest(t *testing.T, expected int) *fakeIngest {
	t.Helper()
	f := &fakeIngest{done: make(chan struct{})}
	count := 0
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p SpanlyPacket
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("ingest decode: %v", err)
		}
		f.mu.Lock()
		f.packets = append(f.packets, p)
		count++
		if count == expected {
			close(f.done)
		}
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	return f
}

func (f *fakeIngest) waitFor(t *testing.T, timeout time.Duration) []SpanlyPacket {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for ingest packets: got %d", len(f.packets))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]SpanlyPacket, len(f.packets))
	copy(out, f.packets)
	return out
}

func newProxyFor(t *testing.T, upstream, ingest string, opts ...func(*ProxyOptions)) (*Proxy, *Collector) {
	t.Helper()
	u, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewSpanlySink(SpanlySinkOptions{
		APIKey: "spanly_us_testkey", IngestURL: ingest,
		InitialBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCollector(CollectorOptions{ClientID: "cid", MonitorID: "mid"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	c.Start(context.Background())
	po := ProxyOptions{Upstream: u, Collector: c}
	for _, o := range opts {
		o(&po)
	}
	p, err := NewProxy(po)
	if err != nil {
		t.Fatal(err)
	}
	return p, c
}

func TestProxyForwardsRequestAndCollectsBoth(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			t.Errorf("upstream got path %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("upstream got method %q", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":42,"result":{"ok":true}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	reqBody := `{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"x"}}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(respBody), `"id":42`) {
		t.Errorf("response body not forwarded: %s", respBody)
	}

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}
	var from, to *SpanlyPacket
	for i := range packets {
		switch packets[i].Direction {
		case "from-client":
			from = &packets[i]
		case "to-client":
			to = &packets[i]
		}
	}
	if from == nil || to == nil {
		t.Fatalf("expected both directions, got %+v", packets)
	}
	if from.TransportContext.HTTPMethod != "post" {
		t.Errorf("from httpMethod = %q", from.TransportContext.HTTPMethod)
	}
	if from.TransportContext.Path != "/mcp" {
		t.Errorf("from path = %q", from.TransportContext.Path)
	}
}

func TestProxyStreamsSSEAndEmitsPerFrame(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		frames := []string{
			`event: message` + "\n" + `data: {"jsonrpc":"2.0","id":1,"result":{"step":"a"}}` + "\n\n",
			`event: message` + "\n" + `data: {"jsonrpc":"2.0","id":2,"result":{"step":"b"}}` + "\n\n",
		}
		for _, f := range frames {
			_, _ = fmt.Fprint(w, f)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 3)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call"}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"step":"a"`) || !strings.Contains(string(body), `"step":"b"`) {
		t.Errorf("SSE body not streamed verbatim: %q", body)
	}

	packets := ingest.waitFor(t, 2*time.Second)
	var fromCount, toCount int
	for _, p := range packets {
		switch p.Direction {
		case "from-client":
			fromCount++
		case "to-client":
			toCount++
		}
	}
	if fromCount != 1 {
		t.Errorf("expected 1 from-client packet, got %d", fromCount)
	}
	if toCount != 2 {
		t.Errorf("expected 2 to-client packets (one per SSE frame), got %d", toCount)
	}
}

func TestProxyMonitorIDOverrideFromHeader(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(MonitorIDHeader, "header-mid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 2 {
		t.Fatalf("got %d packets", len(packets))
	}
	for _, p := range packets {
		if p.Context.SpanlyMonitorId != "header-mid" {
			t.Errorf("monitor ID = %q, want header-mid", p.Context.SpanlyMonitorId)
		}
	}
}

func TestProxyContextHeaderMapping(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.ContextHeaders = []HeaderMapping{
			{Header: "X-Tenant", Field: "environmentId"},
			{Header: "X-Project", Field: "projectId"},
		}
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant", "tenant-A")
	req.Header.Set("X-Project", "proj-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	packets := ingest.waitFor(t, 2*time.Second)
	for _, p := range packets {
		if p.Context.EnvironmentId != "tenant-A" {
			t.Errorf("environmentId = %q", p.Context.EnvironmentId)
		}
		if p.Context.ProjectId != "proj-1" {
			t.Errorf("projectId = %q", p.Context.ProjectId)
		}
	}
}

func TestProxyInspectPrefixSkipsNonMatching(t *testing.T) {
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	var ingestHits int32
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&ingestHits, 1)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer ingest.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.URL, func(o *ProxyOptions) {
		o.InspectPrefix = []string{"/mcp"}
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	// Hit a non-matching path with a JSON-RPC body — should pass through with no telemetry.
	resp, err := http.Post(proxyServer.URL+"/health", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(&ingestHits); got != 0 {
		t.Errorf("ingest hits for non-matching path = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&upstreamHits); got != 1 {
		t.Errorf("upstream hits = %d, want 1 (passthrough should still forward)", got)
	}
	pm := proxy.Metrics()
	if pm.PassthroughRequests != 1 {
		t.Errorf("passthrough counter = %d", pm.PassthroughRequests)
	}
	if pm.InspectRequests != 0 {
		t.Errorf("inspect counter = %d", pm.InspectRequests)
	}
}

func TestProxyInspectPrefixMatchingPathInspected(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InspectPrefix = []string{"/mcp"}
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	resp, err := http.Post(proxyServer.URL+"/mcp/v1", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	pkts := ingest.waitFor(t, 2*time.Second)
	if len(pkts) != 2 {
		t.Errorf("expected 2 packets, got %d", len(pkts))
	}
	pm := proxy.Metrics()
	if pm.InspectRequests != 1 {
		t.Errorf("inspect counter = %d", pm.InspectRequests)
	}
}

func TestProxyIgnoresNonJSONRPCBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"not":"jsonrpc"}`))
	}))
	defer upstream.Close()

	var hits int
	var mu sync.Mutex
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer ingest.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(`hello`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Errorf("expected 0 collect calls for non-JSON-RPC traffic, got %d", hits)
	}
}

func TestNewProxyValidatesContextField(t *testing.T) {
	u, _ := url.Parse("http://localhost")
	sink, _ := NewSpanlySink(SpanlySinkOptions{APIKey: "spanly_us_x", IngestURL: "http://x"})
	c, _ := NewCollector(CollectorOptions{ClientID: "c", MonitorID: "m"}, sink)
	if _, err := NewProxy(ProxyOptions{
		Upstream:       u,
		Collector:      c,
		ContextHeaders: []HeaderMapping{{Header: "X", Field: "bogus"}},
	}); err == nil {
		t.Error("expected error for bogus context field")
	}
}

func TestNormalizeHTTPMethod(t *testing.T) {
	cases := map[string]string{
		"POST":    "post",
		"GET":     "get",
		"DELETE":  "delete",
		"PUT":     "get",
		"OPTIONS": "get",
	}
	for in, want := range cases {
		if got := normalizeHTTPMethod(in); got != want {
			t.Errorf("normalizeHTTPMethod(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFlattenHeaders(t *testing.T) {
	h := http.Header{}
	h.Add("X-Foo", "a")
	h.Add("X-Foo", "b")
	h.Set("Content-Type", "application/json")
	got := flattenHeaders(h)
	if got["x-foo"] != "a, b" {
		t.Errorf("multi value = %q", got["x-foo"])
	}
	if got["content-type"] != "application/json" {
		t.Errorf("single value = %q", got["content-type"])
	}
}

func TestIsSSE(t *testing.T) {
	cases := map[string]bool{
		"text/event-stream":                true,
		"text/event-stream; charset=utf-8": true,
		" TEXT/EVENT-STREAM ":              true,
		"application/json":                 false,
		"":                                 false,
	}
	for in, want := range cases {
		if got := isSSE(in); got != want {
			t.Errorf("isSSE(%q) = %v, want %v", in, got, want)
		}
	}
}
