// Package run implements the `spanly run` subcommand: a process supervisor
// that wraps a child MCP server, intercepts its transport (stdio or HTTP),
// and forwards telemetry to Spanly with zero code changes to the child.
//
// Two modes:
//
//   spanly run -- node server.js               # stdio (default)
//   spanly run --port 3000 -- node server.js   # http (--port set)
//
// In stdio mode the wrapper inherits its parent's stdin/stdout and pipes
// them through interception readers that parse line-delimited JSON-RPC
// frames. In HTTP mode the wrapper picks an unused port for the child,
// sets PORT in its environment, waits for it to listen, then runs the
// regular HTTP/SSE proxy on the user's --port.
package run

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

const defaultInspectPrefix = "/mcp,/sse"

type config struct {
	args     []string // command + args, post `--`
	httpPort int      // 0 = stdio mode

	childPort         int
	childPortEnv      string
	childStartupTimeout time.Duration

	inspectPrefix  []string
	contextHeaders []spanly.HeaderMapping
	bufferSize     int
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	shutdownGrace  time.Duration
	adminAddr      string
}

// Main runs the `spanly run` subcommand.
func Main(args []string, version string) (err error) {
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

	collector, err := spanly.NewCollector(spanly.CollectorOptions{
		ClientID:   uuid.NewString(),
		MonitorID:  uuid.NewString(),
		BufferSize: cfg.bufferSize,
	}, spanlySink)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	collector.Start(ctx)

	logf := func(format string, a ...any) { log.Printf(format, a...) }
	logf("spanly run: command=%v mode=%s version=%s ingest=%s",
		cfg.args, modeName(cfg.httpPort), version, spanlySink.IngestURL())

	var exitCode int
	if cfg.httpPort == 0 {
		exitCode, err = runStdio(ctx, cfg, collector)
	} else {
		exitCode, err = runHTTP(ctx, cfg, collector, version)
	}

	collector.Drain(cfg.shutdownGrace)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
	_ = collector.Shutdown(shutdownCtx)
	cancel()

	if err != nil {
		return err
	}
	if exitCode != 0 {
		// Non-zero child exit propagated to wrapper exit code via os.Exit
		// Implemented in main via the err path is awkward; instead we use
		// the sentinel error type below so main can map it to exit code.
		return &exitCodeError{code: exitCode}
	}
	return nil
}

// exitCodeError lets the wrapper propagate the child's exit code through
// the standard error-return path in main.
type exitCodeError struct{ code int }

func (e *exitCodeError) Error() string { return fmt.Sprintf("child exited with code %d", e.code) }

// ExitCode extracts the underlying exit code if err is an exitCodeError,
// or returns -1 otherwise. Used by main to translate to os.Exit.
func ExitCode(err error) int {
	var ec *exitCodeError
	if errors.As(err, &ec) {
		return ec.code
	}
	return -1
}

func modeName(httpPort int) string {
	if httpPort == 0 {
		return "stdio"
	}
	return "http"
}

func parseArgs(args []string) (*config, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	httpPort := fs.Int("port", 0,
		"HTTP listen port for the wrapper. If set, runs in HTTP mode (wrapper takes this port; child gets a random one). Default 0 = stdio mode.")
	childPort := fs.Int("child-port", 0,
		"Port the child should bind to in HTTP mode. Default 0 = pick a random unused port.")
	childPortEnv := fs.String("child-port-env", "PORT",
		"Name of the env var used to pass the child port to the child process.")
	childStartupTimeout := fs.Duration("child-startup-timeout", 30*time.Second,
		"Max time to wait for the child to start listening before erroring out.")

	inspectPrefix := fs.String("inspect-prefix", defaultInspectPrefix,
		`Comma-separated path prefixes to inspect for JSON-RPC (HTTP mode). Empty = inspect all.`)
	bufferSize := fs.Int("buffer-size", 10000, "Max packets buffered when ingest is unreachable.")
	maxAttempts := fs.Int("retry-max-attempts", 3, "Max POST attempts per packet.")
	initialBackoff := fs.Duration("retry-backoff", 1*time.Second, "Initial retry backoff.")
	maxBackoff := fs.Duration("retry-max-backoff", 30*time.Second, "Cap on retry backoff.")
	shutdownGrace := fs.Duration("shutdown-grace", 10*time.Second,
		"Time to flush in-flight telemetry on graceful shutdown.")
	adminAddr := fs.String("admin-addr", "",
		"Admin listener for /healthz, /readyz, /metrics. Empty = disabled.")

	var contextHeaders stringSliceFlag
	fs.Var(&contextHeaders, "context-header",
		`Map an HTTP header to a PacketContext field, e.g. 'X-Tenant=environmentId'. HTTP mode only. Repeatable.`)

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return nil, errors.New("missing child command (use '--' to separate flags from the command)")
	}

	prefixes := parseInspectPrefix(*inspectPrefix)
	mappings, err := parseContextHeaders(contextHeaders)
	if err != nil {
		return nil, err
	}

	return &config{
		args:                rest,
		httpPort:            *httpPort,
		childPort:           *childPort,
		childPortEnv:        *childPortEnv,
		childStartupTimeout: *childStartupTimeout,
		inspectPrefix:       prefixes,
		contextHeaders:      mappings,
		bufferSize:          *bufferSize,
		maxAttempts:         *maxAttempts,
		initialBackoff:      *initialBackoff,
		maxBackoff:          *maxBackoff,
		shutdownGrace:       *shutdownGrace,
		adminAddr:           *adminAddr,
	}, nil
}

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
	fmt.Fprintln(w, `spanly run — wrap an MCP server and ship telemetry to Spanly

Usage:
  spanly run [flags] -- <command> [args...]

Modes:
  stdio (default)   Inherit parent stdin/stdout, intercept line-delimited
                    JSON-RPC frames in both directions.
  http (--port)     Spawn child on a random port, bind wrapper on --port,
                    proxy + inspect HTTP/SSE traffic.

Flags:
  --port int                   HTTP listen port. If set, runs in HTTP mode.
                               Default: 0 = stdio.
  --child-port int             Port the child should bind in HTTP mode.
                               Default: random.
  --child-port-env string      Env var passed to child with the chosen port.
                               Default: PORT.
  --child-startup-timeout dur  Max wait for child to listen. Default: 30s.

  --inspect-prefix string      Comma-separated path prefixes to inspect.
                               (HTTP mode only.) Default: "/mcp,/sse".
  --context-header HEADER=fld  Map header to context field. Repeatable.
                               (HTTP mode only.)

  --buffer-size int            Default: 10000.
  --retry-max-attempts int     Default: 3.
  --retry-backoff duration     Default: 1s.
  --retry-max-backoff duration Default: 30s.
  --shutdown-grace duration    Default: 10s.
  --admin-addr string          /healthz /readyz /metrics. Default: disabled.

Environment:
  SPANLY_API_KEY                 Required. Region detected from prefix.
  SPANLY_INGEST_URL              Optional override.

Examples:
  spanly run -- node server.js
  spanly run --port 3000 -- python -m my_mcp
  spanly run --port 3000 --admin-addr=:9090 -- ./my-mcp-server`)
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(v string) error { *s = append(*s, v); return nil }
