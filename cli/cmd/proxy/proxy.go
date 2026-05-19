// Package proxy implements the `spanly proxy` subcommand: a standalone
// HTTP/SSE reverse proxy that terminates on a bind address, forwards every
// request to a single upstream, and ships JSON-RPC frames to Spanly.
//
// Used when wrapping the child process is not possible — third-party
// services, K8s declarative sidecar containers, or network-level
// interception. For the common case ("I start my own server"), prefer
// `spanly run`.
package proxy

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/spanlyhq/spanly/cli/internal/otlp"
	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

const defaultInspectPrefix = "/mcp,/sse"

// Main runs the `spanly proxy` subcommand.
func Main(args []string, version string) error {
	cfg, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage(os.Stdout)
			return nil
		}
		printUsage(os.Stderr)
		return err
	}

	apiKey := os.Getenv("SPANLY_API_KEY")
	if apiKey == "" {
		return errors.New("SPANLY_API_KEY environment variable is required")
	}

	spanlySink, err := spanly.NewSpanlySink(spanly.SpanlySinkOptions{
		APIKey:         apiKey,
		IngestURL:      os.Getenv("SPANLY_INGEST_URL"),
		MaxAttempts:    cfg.maxAttempts,
		InitialBackoff: cfg.initialBackoff,
		MaxBackoff:     cfg.maxBackoff,
	})
	if err != nil {
		return err
	}

	sinks := []spanly.Sink{spanlySink}
	otlpSink, err := otlp.NewSink(context.Background(), otlp.Options{
		Endpoint: cfg.otlpEndpoint,
		Headers:  cfg.otlpHeaders,
	})
	switch {
	case err == nil:
		sinks = append(sinks, otlpSink)
		log.Printf("otlp exporter enabled (endpoint %s)", otlpSink.Endpoint())
	case errors.Is(err, otlp.ErrDisabled):
		// OTLP is opt-in; nothing configured, nothing to do.
	default:
		return fmt.Errorf("otlp: %w", err)
	}

	collector, err := spanly.NewCollector(spanly.CollectorOptions{
		ClientID:   uuid.NewString(),
		MonitorID:  uuid.NewString(),
		BufferSize: cfg.bufferSize,
	}, sinks...)
	if err != nil {
		return err
	}

	proxy, err := spanly.NewProxy(spanly.ProxyOptions{
		Upstream:       cfg.upstream,
		Collector:      collector,
		InspectPrefix:  cfg.inspectPrefix,
		ContextHeaders: cfg.contextHeaders,
	})
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.bindAddr,
		Handler:           proxy,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("proxying %s -> %s (version %s, ingest %s, inspect=%v)",
		cfg.bindAddr, cfg.upstream.String(), version, spanlySink.IngestURL(), cfg.inspectPrefix)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	collector.Start(ctx)

	adminErrCh := make(chan error, 1)
	if cfg.adminAddr != "" {
		admin := spanly.NewAdminServer(cfg.adminAddr, cfg.upstream, collector, proxy)
		admin.Start(ctx, adminErrCh)
		log.Printf("admin server listening on %s", cfg.adminAddr)
	}

	proxyErrCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			proxyErrCh <- err
			return
		}
		proxyErrCh <- nil
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		collector.Drain(cfg.shutdownGrace)
		_ = collector.Shutdown(shutdownCtx)
		return nil
	case err := <-proxyErrCh:
		return err
	case err := <-adminErrCh:
		return fmt.Errorf("admin server: %w", err)
	}
}

type config struct {
	upstream       *url.URL
	bindAddr       string
	inspectPrefix  []string
	contextHeaders []spanly.HeaderMapping
	bufferSize     int
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	shutdownGrace  time.Duration
	adminAddr      string
	otlpEndpoint   string
	otlpHeaders    map[string]string
}

func parseArgs(args []string) (*config, error) {
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	inspectPrefix := fs.String("inspect-prefix", defaultInspectPrefix,
		`Comma-separated path prefixes to inspect for JSON-RPC. Other paths pass through with no telemetry. Empty string = inspect all paths.`)
	bufferSize := fs.Int("buffer-size", 10000,
		"Maximum packets buffered when ingest is unreachable.")
	maxAttempts := fs.Int("retry-max-attempts", 3,
		"Maximum POST attempts per packet before dropping.")
	initialBackoff := fs.Duration("retry-backoff", 1*time.Second,
		"Initial retry backoff (exponential).")
	maxBackoff := fs.Duration("retry-max-backoff", 30*time.Second,
		"Cap on retry backoff.")
	shutdownGrace := fs.Duration("shutdown-grace", 10*time.Second,
		"Time to flush in-flight telemetry on graceful shutdown.")
	adminAddr := fs.String("admin-addr", "",
		"Listen address for /healthz, /readyz, /metrics (e.g. ':9090'). Empty = disabled.")
	otlpEndpoint := fs.String("otlp-endpoint", "",
		"OTLP/HTTP endpoint to co-export spans to (e.g. 'http://otel-collector:4318'). Overrides OTEL_EXPORTER_OTLP_ENDPOINT. Empty = OTLP disabled.")

	var contextHeaders stringSliceFlag
	fs.Var(&contextHeaders, "context-header",
		`Map an HTTP header to a PacketContext field, e.g. 'X-Tenant=environmentId'. Repeatable. Allowed fields: environmentId, projectId, organisationId.`)

	var otlpHeaderFlags stringSliceFlag
	fs.Var(&otlpHeaderFlags, "otlp-header",
		`Add a header to OTLP exporter requests, e.g. 'x-api-key=abc'. Repeatable. Additive on top of OTEL_EXPORTER_OTLP_HEADERS.`)

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	positional := fs.Args()
	if len(positional) != 2 {
		return nil, errors.New("usage: spanly proxy [flags] <upstream> <bind>")
	}

	upstream, err := parseUpstream(positional[0])
	if err != nil {
		return nil, fmt.Errorf("invalid upstream: %w", err)
	}
	bindAddr, err := normalizeBind(positional[1])
	if err != nil {
		return nil, err
	}

	prefixes := parseInspectPrefix(*inspectPrefix)

	mappings, err := parseContextHeaders(contextHeaders)
	if err != nil {
		return nil, err
	}

	otlpHeaders, err := parseOTLPHeaders(otlpHeaderFlags)
	if err != nil {
		return nil, err
	}

	return &config{
		upstream:       upstream,
		bindAddr:       bindAddr,
		inspectPrefix:  prefixes,
		contextHeaders: mappings,
		bufferSize:     *bufferSize,
		maxAttempts:    *maxAttempts,
		initialBackoff: *initialBackoff,
		maxBackoff:     *maxBackoff,
		shutdownGrace:  *shutdownGrace,
		adminAddr:      *adminAddr,
		otlpEndpoint:   *otlpEndpoint,
		otlpHeaders:    otlpHeaders,
	}, nil
}

// parseOTLPHeaders splits "k=v" entries into a map. Empty input → nil.
func parseOTLPHeaders(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for _, entry := range raw {
		eq := strings.IndexByte(entry, '=')
		if eq <= 0 || eq == len(entry)-1 {
			return nil, fmt.Errorf("--otlp-header %q must be of form KEY=VALUE", entry)
		}
		out[strings.TrimSpace(entry[:eq])] = strings.TrimSpace(entry[eq+1:])
	}
	return out, nil
}

// parseInspectPrefix splits a comma-separated prefix list, trimming
// whitespace and dropping empty entries. Returns nil for an empty input,
// which signals "inspect all paths" downstream.
func parseInspectPrefix(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseContextHeaders(raw []string) ([]spanly.HeaderMapping, error) {
	var out []spanly.HeaderMapping
	for _, entry := range raw {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("--context-header %q must be of form HEADER=field", entry)
		}
		m := spanly.HeaderMapping{Header: parts[0], Field: parts[1]}
		if err := spanly.ValidateContextField(m.Field); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `spanly proxy — standalone HTTP/SSE proxy for MCP servers

Usage:
  spanly proxy [flags] <upstream> <bind>

Arguments:
  upstream  host:port (or URL) of the MCP server you want to monitor
  bind      listen address (host:port). Required.

Flags:
  --inspect-prefix string         Comma-separated path prefixes to inspect for
                                  JSON-RPC. Other paths pass through with no
                                  telemetry. Default: "/mcp,/sse".
                                  Empty string = inspect all paths.
  --context-header HEADER=field   Map an HTTP header to a packet context field.
                                  Repeatable. Fields: environmentId, projectId,
                                  organisationId.
  --buffer-size int               Max packets buffered when ingest is
                                  unreachable. Default: 10000.
  --retry-max-attempts int        Max POST attempts per packet. Default: 3.
  --retry-backoff duration        Initial retry backoff (exponential). Default: 1s.
  --retry-max-backoff duration    Cap on retry backoff. Default: 30s.
  --shutdown-grace duration       Time to flush telemetry on shutdown. Default: 10s.
  --admin-addr string             Admin listener (e.g. ':9090'). Default: disabled.
  --otlp-endpoint string          OTLP/HTTP endpoint to co-export spans to.
                                  Overrides OTEL_EXPORTER_OTLP_ENDPOINT. Empty = disabled.
  --otlp-header KEY=VALUE         Add a header to OTLP requests. Repeatable.
                                  Additive on top of OTEL_EXPORTER_OTLP_HEADERS.

Environment:
  SPANLY_API_KEY                 Required. Region detected from prefix.
  SPANLY_INGEST_URL              Optional override of the ingest URL.
  OTEL_EXPORTER_OTLP_ENDPOINT    Enable OTLP co-export to this endpoint.
  OTEL_EXPORTER_OTLP_HEADERS     Comma-separated 'k=v' OTLP request headers.
  OTEL_SERVICE_NAME              Override service.name on emitted spans (default 'spanly-cli').

Per-request headers (recognised on inbound traffic):
  X-Spanly-Monitor-Id   Override the spanlyMonitorId for this request.

Example:
  spanly proxy --inspect-prefix=/mcp,/sse \
               --context-header=X-Tenant=environmentId \
               --admin-addr=:9090 \
               localhost:3000 :3001`)
}

func parseUpstream(s string) (*url.URL, error) {
	if s == "" {
		return nil, errors.New("upstream must not be empty")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, errors.New("upstream must include a host")
	}
	return u, nil
}

func normalizeBind(s string) (string, error) {
	if s == "" {
		return "", errors.New("bind address is required")
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", err
		}
		s = u.Host
	}
	if _, _, err := net.SplitHostPort(s); err != nil {
		return "", fmt.Errorf("invalid bind %q: %w", s, err)
	}
	return s, nil
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(v string) error { *s = append(*s, v); return nil }
