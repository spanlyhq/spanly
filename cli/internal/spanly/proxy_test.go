package spanly

import (
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
	c.Start()
	t.Cleanup(func() { _ = c.Close(time.Second) })
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
	if to.TransportContext.StatusCode != http.StatusOK {
		t.Errorf("to statusCode = %d, want 200", to.TransportContext.StatusCode)
	}
	if from.TransportContext.StatusCode != 0 {
		t.Errorf("from statusCode = %d, want 0 (request leg has no status)", from.TransportContext.StatusCode)
	}
}

func TestProxyCapturesStatusAndRateLimitHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "30")
		w.Header().Set("RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"rate limited"}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	packets := ingest.waitFor(t, 2*time.Second)
	var to *SpanlyPacket
	for i := range packets {
		if packets[i].Direction == "to-client" {
			to = &packets[i]
		}
	}
	if to == nil {
		t.Fatalf("expected a to-client packet, got %+v", packets)
	}
	if to.TransportContext.StatusCode != http.StatusTooManyRequests {
		t.Errorf("statusCode = %d, want 429", to.TransportContext.StatusCode)
	}
	if got := to.TransportContext.Headers["retry-after"]; got != "30" {
		t.Errorf("retry-after header = %q, want 30", got)
	}
	if got := to.TransportContext.Headers["ratelimit-remaining"]; got != "0" {
		t.Errorf("ratelimit-remaining header = %q, want 0", got)
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

func TestProxyRedactsSensitiveHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("upstream Authorization = %q, want original value forwarded", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "session=abc123")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.RedactHeaders = []string{"X-Custom-Token"}
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Cookie", "session=abc123")
	req.Header.Set("X-Api-Key", "key-123")
	req.Header.Set("X-Custom-Token", "custom-secret")
	req.Header.Set("Mcp-Session-Id", "session-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("Set-Cookie"); got != "session=abc123" {
		t.Errorf("client Set-Cookie = %q, want original value forwarded", got)
	}

	packets := ingest.waitFor(t, 2*time.Second)
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
	for _, name := range []string{"authorization", "cookie", "x-api-key", "x-custom-token"} {
		if got := from.TransportContext.Headers[name]; got != "[REDACTED]" {
			t.Errorf("from-client %s = %q, want [REDACTED]", name, got)
		}
	}
	if got := from.TransportContext.Headers["mcp-session-id"]; got != "session-1" {
		t.Errorf("from-client mcp-session-id = %q, want preserved", got)
	}
	if got := to.TransportContext.Headers["set-cookie"]; got != "[REDACTED]" {
		t.Errorf("to-client set-cookie = %q, want [REDACTED]", got)
	}
	if got := to.TransportContext.Headers["content-type"]; got != "application/json" {
		t.Errorf("to-client content-type = %q, want preserved", got)
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

func TestProxyForwardsOversizedRequestAndEmitsMetadata(t *testing.T) {
	var upstreamGot atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		upstreamGot.Store(n)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 1)
	defer ingest.srv.Close()

	p, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	front := httptest.NewServer(p)
	defer front.Close()

	// A valid tools/call request padded past the inspection cap: the full body
	// must reach the upstream, and a metadata-only event (method + tool name +
	// true size, no buffered body) must be emitted.
	body := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"bigtool","arguments":{"pad":"` +
		strings.Repeat("a", MaxInspectBytes) + `"}}}`

	resp, err := http.Post(front.URL+"/mcp", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}

	if got := upstreamGot.Load(); got != int64(len(body)) {
		t.Errorf("upstream received %d bytes, want %d", got, len(body))
	}

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 1 {
		t.Fatalf("expected 1 metadata packet, got %d", len(packets))
	}
	got := packets[0]
	if got.Direction != "from-client" {
		t.Errorf("direction = %q, want from-client", got.Direction)
	}
	if got.Oversized == nil {
		t.Fatal("expected Oversized to be set")
	}
	if got.Oversized.OriginalSize != int64(len(body)) {
		t.Errorf("OriginalSize = %d, want %d", got.Oversized.OriginalSize, len(body))
	}
	if len(got.MCPPacket) >= MaxInspectBytes {
		t.Errorf("stub packet is %d bytes, expected a small metadata stub", len(got.MCPPacket))
	}
	var stub struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if err := json.Unmarshal(got.MCPPacket, &stub); err != nil {
		t.Fatalf("stub not valid JSON: %v", err)
	}
	if stub.Method != "tools/call" {
		t.Errorf("stub method = %q, want tools/call", stub.Method)
	}
	if stub.Params.Name != "bigtool" {
		t.Errorf("stub tool name = %q, want bigtool", stub.Params.Name)
	}
}

func TestProxyForwardsOversizedResponseAndEmitsMetadata(t *testing.T) {
	big := `{"jsonrpc":"2.0","id":9,"result":{"content":[{"type":"text","text":"` +
		strings.Repeat("b", MaxInspectBytes) + `"}]}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, big)
	}))
	defer upstream.Close()

	// Two events: the (small) request and the oversized response.
	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	p, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	front := httptest.NewServer(p)
	defer front.Close()

	reqBody := `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"x"}}`
	resp, err := http.Post(front.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(big) {
		t.Errorf("client received %d bytes, want the full %d-byte response", len(got), len(big))
	}

	packets := ingest.waitFor(t, 2*time.Second)
	var oversized *SpanlyPacket
	for i := range packets {
		if packets[i].Oversized != nil {
			oversized = &packets[i]
		}
	}
	if oversized == nil {
		t.Fatal("expected an oversized metadata packet for the response")
	}
	if oversized.Direction != "to-client" {
		t.Errorf("direction = %q, want to-client", oversized.Direction)
	}
	if oversized.Oversized.OriginalSize != int64(len(big)) {
		t.Errorf("OriginalSize = %d, want %d", oversized.Oversized.OriginalSize, len(big))
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
	got := flattenHeaders(h, nil)
	if got["x-foo"] != "a, b" {
		t.Errorf("multi value = %q", got["x-foo"])
	}
	if got["content-type"] != "application/json" {
		t.Errorf("single value = %q", got["content-type"])
	}
}

func TestFlattenHeadersRedacts(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer secret")
	h.Add("Set-Cookie", "a=1")
	h.Add("Set-Cookie", "b=2")
	h.Set("Content-Type", "application/json")
	redact := map[string]struct{}{"authorization": {}, "set-cookie": {}}
	got := flattenHeaders(h, redact)
	if got["authorization"] != "[REDACTED]" {
		t.Errorf("authorization = %q", got["authorization"])
	}
	if got["set-cookie"] != "[REDACTED]" {
		t.Errorf("set-cookie = %q", got["set-cookie"])
	}
	if got["content-type"] != "application/json" {
		t.Errorf("content-type = %q", got["content-type"])
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

func TestProxyInjectsSyntheticSessionIDOnInitialize(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	sessionID := resp.Header.Get(SessionIDHeader)
	if !strings.HasPrefix(sessionID, SyntheticSessionIDPrefix) {
		t.Fatalf("Mcp-Session-Id = %q, want synthetic ID with prefix %q", sessionID, SyntheticSessionIDPrefix)
	}

	packets := ingest.waitFor(t, 2*time.Second)
	for i := range packets {
		if packets[i].Direction != "to-client" {
			continue
		}
		if got := packets[i].TransportContext.Headers["mcp-session-id"]; got != sessionID {
			t.Errorf("to-client mcp-session-id = %q, want %q", got, sessionID)
		}
	}
}

func TestProxyDoesNotInjectWhenUpstreamSetsSessionID(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(SessionIDHeader, "upstream-session")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get(SessionIDHeader); got != "upstream-session" {
		t.Errorf("Mcp-Session-Id = %q, want upstream value preserved", got)
	}
	ingest.waitFor(t, 2*time.Second)
}

func TestProxyDoesNotInjectOnNonInitialize(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	reqBody := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{}}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get(SessionIDHeader); got != "" {
		t.Errorf("Mcp-Session-Id = %q, want empty", got)
	}
	ingest.waitFor(t, 2*time.Second)
}

func TestProxyDoesNotInjectWhenDisabled(t *testing.T) {
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

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	resp, err := http.Post(proxyServer.URL+"/mcp", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get(SessionIDHeader); got != "" {
		t.Errorf("Mcp-Session-Id = %q, want empty when injection disabled", got)
	}
	ingest.waitFor(t, 2*time.Second)
}

func TestProxyStripsSyntheticSessionIDFromUpstreamRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(SessionIDHeader); got != "" {
			t.Errorf("upstream saw Mcp-Session-Id = %q, want stripped", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SessionIDHeader, NewSyntheticSessionID())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	packets := ingest.waitFor(t, 2*time.Second)
	for i := range packets {
		if packets[i].Direction != "from-client" {
			continue
		}
		if got := packets[i].TransportContext.Headers["mcp-session-id"]; !strings.HasPrefix(got, SyntheticSessionIDPrefix) {
			t.Errorf("from-client mcp-session-id = %q, want synthetic ID kept in telemetry", got)
		}
	}
}

func TestProxyForwardsRealSessionIDToUpstream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(SessionIDHeader); got != "real-session" {
			t.Errorf("upstream Mcp-Session-Id = %q, want forwarded", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 2)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(SessionIDHeader, "real-session")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	ingest.waitFor(t, 2*time.Second)
}

func TestContainsInitializeRequest(t *testing.T) {
	cases := map[string]bool{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`: true,
		`[{"method":"tools/call"},{"method":"initialize"}]`:          true,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`:             false,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`:     false,
		`not json`: false,
		``:         false,
	}
	for in, want := range cases {
		if got := containsInitializeRequest([]byte(in)); got != want {
			t.Errorf("containsInitializeRequest(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestProxyRecordsSessionTerminationOnDelete(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("upstream got method %q, want DELETE", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 1)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL)
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, err := http.NewRequest(http.MethodDelete, proxyServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(SessionIDHeader, "session-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 1 {
		t.Fatalf("got %d packets, want 1", len(packets))
	}
	p := packets[0]
	if p.Direction != "from-client" {
		t.Errorf("direction = %q, want from-client", p.Direction)
	}
	var mcp struct {
		Method string `json:"method"`
		Params struct {
			SessionID string `json:"sessionId"`
		} `json:"params"`
	}
	if err := json.Unmarshal(p.MCPPacket, &mcp); err != nil {
		t.Fatalf("unmarshal mcpPacket: %v", err)
	}
	if mcp.Method != SessionTerminatedMethod {
		t.Errorf("method = %q, want %q", mcp.Method, SessionTerminatedMethod)
	}
	if mcp.Params.SessionID != "session-1" {
		t.Errorf("params.sessionId = %q, want session-1", mcp.Params.SessionID)
	}
	if p.TransportContext.HTTPMethod != "delete" {
		t.Errorf("transport httpMethod = %q, want delete", p.TransportContext.HTTPMethod)
	}
	if got := p.TransportContext.Headers["mcp-session-id"]; got != "session-1" {
		t.Errorf("transport mcp-session-id = %q, want session-1", got)
	}
}

func TestProxyDoesNotRecordTerminationOnFailedDelete(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
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

	req, err := http.NewRequest(http.MethodDelete, proxyServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(SessionIDHeader, "unknown")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Errorf("expected 0 collect calls for a rejected DELETE, got %d", hits)
	}
}

func TestProxyDoesNotRecordTerminationWithoutSessionID(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
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

	req, err := http.NewRequest(http.MethodDelete, proxyServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Errorf("expected 0 collect calls for a DELETE without session ID, got %d", hits)
	}
}

func TestProxyInterceptsSyntheticSessionDelete(t *testing.T) {
	var upstreamHits int
	var mu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		upstreamHits++
		mu.Unlock()
		// A sessionless upstream would reject this DELETE; assert we never
		// reach it, so its status here is irrelevant.
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 1)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	sessionID := NewSyntheticSessionID()
	req, err := http.NewRequest(http.MethodDelete, proxyServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(SessionIDHeader, sessionID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (Spanly answers the synthetic DELETE)", resp.StatusCode)
	}

	mu.Lock()
	hits := upstreamHits
	mu.Unlock()
	if hits != 0 {
		t.Errorf("upstream called %d times, want 0 (synthetic DELETE must not be forwarded)", hits)
	}

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 1 {
		t.Fatalf("got %d packets, want 1", len(packets))
	}
	p := packets[0]
	if p.Direction != "from-client" {
		t.Errorf("direction = %q, want from-client", p.Direction)
	}
	var mcp struct {
		Method string `json:"method"`
		Params struct {
			SessionID string `json:"sessionId"`
		} `json:"params"`
	}
	if err := json.Unmarshal(p.MCPPacket, &mcp); err != nil {
		t.Fatalf("unmarshal mcpPacket: %v", err)
	}
	if mcp.Method != SessionTerminatedMethod {
		t.Errorf("method = %q, want %q", mcp.Method, SessionTerminatedMethod)
	}
	if mcp.Params.SessionID != sessionID {
		t.Errorf("params.sessionId = %q, want %q", mcp.Params.SessionID, sessionID)
	}
}

func TestProxyForwardsRealSessionDeleteWhenInjecting(t *testing.T) {
	var upstreamHits int
	var mu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("upstream got method %q, want DELETE", r.Method)
		}
		mu.Lock()
		upstreamHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	ingest := newFakeIngest(t, 1)
	defer ingest.srv.Close()

	proxy, _ := newProxyFor(t, upstream.URL, ingest.srv.URL, func(o *ProxyOptions) {
		o.InjectSessionID = true
	})
	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	req, err := http.NewRequest(http.MethodDelete, proxyServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(SessionIDHeader, "server-issued-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	mu.Lock()
	hits := upstreamHits
	mu.Unlock()
	if hits != 1 {
		t.Errorf("upstream called %d times, want 1 (real DELETE must still be forwarded)", hits)
	}

	packets := ingest.waitFor(t, 2*time.Second)
	if len(packets) != 1 {
		t.Fatalf("got %d packets, want 1", len(packets))
	}
	var mcp struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(packets[0].MCPPacket, &mcp); err != nil {
		t.Fatalf("unmarshal mcpPacket: %v", err)
	}
	if mcp.Method != SessionTerminatedMethod {
		t.Errorf("method = %q, want %q", mcp.Method, SessionTerminatedMethod)
	}
}
