package spanly

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func startAdmin(t *testing.T, upstream *url.URL) (*AdminServer, *Collector, *Proxy, *SpanlySink, string, context.CancelFunc) {
	t.Helper()
	addr := freePort(t)
	sink, err := NewSpanlySink(SpanlySinkOptions{APIKey: "spanly_us_test", IngestURL: "http://localhost:0"})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCollector(CollectorOptions{ClientID: "cid", MonitorID: "mid"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProxy(ProxyOptions{Upstream: upstream, Collector: c})
	if err != nil {
		t.Fatal(err)
	}
	admin := NewAdminServer(addr, upstream, c, p)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	admin.Start(ctx, errCh)

	// give listener a moment to come up
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return admin, c, p, sink, addr, cancel
}

func TestAdminHealthz(t *testing.T) {
	u, _ := url.Parse("http://localhost:1")
	_, _, _, _, addr, cancel := startAdmin(t, u)
	defer cancel()

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "ok") {
		t.Errorf("body = %q", body)
	}
}

func TestAdminReadyz(t *testing.T) {
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamLn.Close()

	u, _ := url.Parse("http://" + upstreamLn.Addr().String())
	_, _, _, _, addr, cancel := startAdmin(t, u)
	defer cancel()

	// ready path: upstream listening
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("ready status = %d", resp.StatusCode)
	}

	// unready path: upstream gone
	upstreamLn.Close()
	time.Sleep(1100 * time.Millisecond) // beat the 1s readiness cache
	resp2, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unready status = %d", resp2.StatusCode)
	}
}

func TestAdminMetrics(t *testing.T) {
	u, _ := url.Parse("http://localhost:1")
	_, c, p, sink, addr, cancel := startAdmin(t, u)
	defer cancel()

	// generate some counter activity
	c.collected.Add(5)
	sink.sent.Add(3)
	c.droppedFull.Add(1)
	p.inspectReqs.Add(7)
	p.passthroughReqs.Add(11)

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	for _, want := range []string{
		"spanly_uptime_seconds",
		"spanly_packets_collected_total 5",
		"spanly_packets_sent_total 3",
		`spanly_packets_dropped_total{reason="buffer_full"} 1`,
		`spanly_proxy_requests_total{path_class="inspect"} 7`,
		`spanly_proxy_requests_total{path_class="passthrough"} 11`,
		"spanly_buffer_depth",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("metrics output missing %q\n%s", want, s)
		}
	}
}
