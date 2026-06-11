package spanly

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// AdminServer exposes /healthz, /readyz, and /metrics on a separate
// listener for operational observability of the spanly process itself.
//
// /healthz   200 if the admin listener is up.
// /readyz    200 if the upstream accepts a TCP dial (cached for ~1s).
// /metrics   Prometheus text format. Combines Collector + Proxy counters.
type AdminServer struct {
	addr      string
	upstream  *url.URL
	collector *Collector
	proxy     *Proxy
	started   time.Time

	readyMu      sync.Mutex
	readyResult  bool
	readyChecked time.Time
}

func NewAdminServer(addr string, upstream *url.URL, collector *Collector, proxy *Proxy) *AdminServer {
	return &AdminServer{
		addr:      addr,
		upstream:  upstream,
		collector: collector,
		proxy:     proxy,
		started:   time.Now(),
	}
}

// Start launches the admin HTTP listener in a goroutine. errCh receives
// a listener failure; nothing is sent on graceful shutdown.
func (a *AdminServer) Start(ctx context.Context, errCh chan<- error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)
	mux.HandleFunc("/metrics", a.handleMetrics)

	srv := &http.Server{
		Addr:              a.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
}

func (a *AdminServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (a *AdminServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if a.checkReady() {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ready\n"))
		return
	}
	http.Error(w, "upstream unreachable\n", http.StatusServiceUnavailable)
}

// checkReady returns true if the upstream accepts a TCP connection.
// Result is cached for 1s to avoid spamming upstream from frequent probes.
func (a *AdminServer) checkReady() bool {
	a.readyMu.Lock()
	defer a.readyMu.Unlock()
	if time.Since(a.readyChecked) < time.Second {
		return a.readyResult
	}
	host := a.upstream.Host
	if !strings.Contains(host, ":") {
		switch a.upstream.Scheme {
		case "https":
			host += ":443"
		default:
			host += ":80"
		}
	}
	conn, err := net.DialTimeout("tcp", host, 1*time.Second)
	a.readyResult = (err == nil)
	a.readyChecked = time.Now()
	if conn != nil {
		_ = conn.Close()
	}
	return a.readyResult
}

func (a *AdminServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	cm := a.collector.Metrics()
	pm := a.proxy.Metrics()
	uptime := time.Since(a.started).Seconds()

	fmt.Fprintf(w, "# HELP spanly_uptime_seconds Time since the spanly process started.\n")
	fmt.Fprintf(w, "# TYPE spanly_uptime_seconds gauge\n")
	fmt.Fprintf(w, "spanly_uptime_seconds %f\n", uptime)

	fmt.Fprintf(w, "# HELP spanly_packets_collected_total Packets accepted into the buffer.\n")
	fmt.Fprintf(w, "# TYPE spanly_packets_collected_total counter\n")
	fmt.Fprintf(w, "spanly_packets_collected_total %d\n", cm.Collected)

	fmt.Fprintf(w, "# HELP spanly_packets_dropped_total Packets dropped before delivery.\n")
	fmt.Fprintf(w, "# TYPE spanly_packets_dropped_total counter\n")
	fmt.Fprintf(w, "spanly_packets_dropped_total{reason=\"buffer_full\"} %d\n", cm.DroppedFull)
	fmt.Fprintf(w, "spanly_packets_dropped_total{reason=\"retries_exhausted\"} %d\n", cm.Failed)

	fmt.Fprintf(w, "# HELP spanly_packets_sent_total Packets successfully POSTed to ingest.\n")
	fmt.Fprintf(w, "# TYPE spanly_packets_sent_total counter\n")
	fmt.Fprintf(w, "spanly_packets_sent_total %d\n", cm.Sent)

	fmt.Fprintf(w, "# HELP spanly_collect_attempts_total Attempts to POST to ingest (including retries).\n")
	fmt.Fprintf(w, "# TYPE spanly_collect_attempts_total counter\n")
	fmt.Fprintf(w, "spanly_collect_attempts_total{outcome=\"ok\"} %d\n", cm.AttemptsTotal-cm.AttemptsFailed)
	fmt.Fprintf(w, "spanly_collect_attempts_total{outcome=\"error\"} %d\n", cm.AttemptsFailed)

	fmt.Fprintf(w, "# HELP spanly_buffer_depth Current number of packets in the in-memory buffer.\n")
	fmt.Fprintf(w, "# TYPE spanly_buffer_depth gauge\n")
	fmt.Fprintf(w, "spanly_buffer_depth %d\n", cm.BufferDepth)

	fmt.Fprintf(w, "# HELP spanly_proxy_requests_total HTTP requests handled by the proxy, by inspection class.\n")
	fmt.Fprintf(w, "# TYPE spanly_proxy_requests_total counter\n")
	fmt.Fprintf(w, "spanly_proxy_requests_total{path_class=\"inspect\"} %d\n", pm.InspectRequests)
	fmt.Fprintf(w, "spanly_proxy_requests_total{path_class=\"passthrough\"} %d\n", pm.PassthroughRequests)
}

// ProxyMetrics is the operational counter snapshot exposed by Proxy.
type ProxyMetrics struct {
	InspectRequests     int64
	PassthroughRequests int64
}

func (p *Proxy) Metrics() ProxyMetrics {
	return ProxyMetrics{
		InspectRequests:     p.inspectReqs.Load(),
		PassthroughRequests: p.passthroughReqs.Load(),
	}
}
