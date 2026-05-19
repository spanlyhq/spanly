package otlp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

const (
	tracerName        = "github.com/spanlyhq/spanly/cli"
	defaultServiceName = "spanly-cli"
)

// Options configures the OTLP Sink. CLI flags take precedence over
// the standard OTEL_* env vars; env vars fill in missing values. When
// neither Endpoint nor OTEL_EXPORTER_OTLP_ENDPOINT is set, NewSink
// returns ErrDisabled — callers should treat that as "OTLP is opt-in,
// don't register the sink."
type Options struct {
	// Endpoint overrides OTEL_EXPORTER_OTLP_ENDPOINT (and the
	// signal-scoped OTEL_EXPORTER_OTLP_TRACES_ENDPOINT). Required for
	// the sink to be enabled. Either a full URL ("https://otel:4318")
	// or host:port form.
	Endpoint string

	// Headers add to (and override conflicting keys in) the headers
	// configured via OTEL_EXPORTER_OTLP_HEADERS.
	Headers map[string]string

	// Insecure forces HTTP (vs HTTPS) regardless of the endpoint scheme.
	// Default false; honored only when the endpoint is bare host:port.
	Insecure bool
}

// ErrDisabled is returned by NewSink when no endpoint is configured.
// Callers should treat this as a soft signal ("OTLP not requested")
// rather than an error to surface to the user.
var ErrDisabled = errors.New("otlp: no endpoint configured")

// Sink is the OTel implementation of spanly.Sink. Each packet is parsed
// as JSON-RPC and routed to the correlator, which emits OTel spans via
// a TracerProvider with a BatchSpanProcessor. Failures in the OTel
// pipeline are logged via the SDK's own error handler and counted
// against this sink's Failed metric.
type Sink struct {
	provider   *sdktrace.TracerProvider
	exporter   *otlptrace.Exporter
	tracer     trace.Tracer
	correlator *correlator
	propagator propagation.TextMapPropagator

	endpoint string

	sent   atomic.Int64
	failed atomic.Int64
}

// NewSink builds the TracerProvider, exporter, and correlator. The
// returned Sink is ready to be registered with the Spanly Collector.
// The caller is responsible for invoking Shutdown to flush spans on
// process exit (the Collector's Shutdown does this when invoked).
func NewSink(ctx context.Context, opts Options) (*Sink, error) {
	endpoint := firstNonEmpty(
		opts.Endpoint,
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	)
	if endpoint == "" {
		return nil, ErrDisabled
	}

	if err := assertHTTPProtocol(); err != nil {
		return nil, err
	}

	headers := mergeHeaders(parseHeaderEnv(), opts.Headers)

	httpOpts, err := buildHTTPOptions(endpoint, headers, opts.Insecure)
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracehttp.New(ctx, httpOpts...)
	if err != nil {
		return nil, fmt.Errorf("otlp: build exporter: %w", err)
	}

	res, err := buildResource(ctx)
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	tracer := provider.Tracer(tracerName)
	return &Sink{
		provider:   provider,
		exporter:   exporter,
		tracer:     tracer,
		correlator: newCorrelator(tracer),
		propagator: propagator,
		endpoint:   endpoint,
	}, nil
}

// Endpoint returns the resolved OTLP endpoint, useful for startup logs.
func (s *Sink) Endpoint() string { return s.endpoint }

func (s *Sink) Name() string { return "otlp" }

func (s *Sink) Metrics() spanly.SinkMetrics {
	sent := s.sent.Load()
	failed := s.failed.Load()
	return spanly.SinkMetrics{
		Sent:           sent,
		Failed:         failed,
		AttemptsTotal:  sent + failed,
		AttemptsFailed: failed,
	}
}

// Export hands the packet to the correlator. Errors here are limited to
// "couldn't parse the JSON-RPC envelope"; the OTel BatchSpanProcessor
// owns transport-level retry and failure handling for the actual export.
func (s *Sink) Export(ctx context.Context, packet spanly.SpanlyPacket) error {
	parsed := parseMCP(packet.MCPPacket)
	if parsed == nil {
		s.failed.Add(1)
		return errors.New("otlp: malformed JSON-RPC packet")
	}
	parentCtx := extractParentContext(ctx, s.propagator, packet, parsed)
	s.correlator.handle(parentCtx, packet, parsed)
	s.sent.Add(1)
	return nil
}

// Shutdown flushes pending spans (force-ends any uncorrelated requests),
// drains the BatchSpanProcessor, and closes the exporter connection.
func (s *Sink) Shutdown(ctx context.Context) error {
	s.correlator.shutdown()
	return s.provider.Shutdown(ctx)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// assertHTTPProtocol rejects non-HTTP/protobuf protocols at startup.
// We intentionally don't link in gRPC; users who want gRPC can run an
// OTel Collector hop and point us at its HTTP receiver.
func assertHTTPProtocol() error {
	for _, env := range []string{
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
	} {
		v := strings.TrimSpace(os.Getenv(env))
		if v == "" || v == "http/protobuf" {
			continue
		}
		return fmt.Errorf("otlp: %s=%q not supported (only http/protobuf is built in)", env, v)
	}
	return nil
}

// parseHeaderEnv parses OTEL_EXPORTER_OTLP_HEADERS (and the
// signal-scoped OTEL_EXPORTER_OTLP_TRACES_HEADERS) per the spec:
// comma-separated "k=v" pairs.
func parseHeaderEnv() map[string]string {
	out := map[string]string{}
	for _, env := range []string{
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS",
	} {
		raw := os.Getenv(env)
		if raw == "" {
			continue
		}
		for _, pair := range strings.Split(raw, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			eq := strings.IndexByte(pair, '=')
			if eq <= 0 {
				continue
			}
			out[strings.TrimSpace(pair[:eq])] = strings.TrimSpace(pair[eq+1:])
		}
	}
	return out
}

func mergeHeaders(base, override map[string]string) map[string]string {
	if len(override) == 0 {
		return base
	}
	out := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

// buildHTTPOptions translates an endpoint string + headers into the
// exporter's option list. Accepts both "https://host:port" and bare
// "host:port" forms.
func buildHTTPOptions(endpoint string, headers map[string]string, insecure bool) ([]otlptracehttp.Option, error) {
	host, scheme, err := splitEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(host),
	}
	if insecure || scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if len(headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(headers))
	}
	return opts, nil
}

// splitEndpoint parses an endpoint URL or host:port and returns the
// "host:port" portion plus scheme ("http"/"https"/""). The OTLP HTTP
// exporter takes endpoint as bare "host:port" with insecure controlled
// separately.
func splitEndpoint(endpoint string) (host, scheme string, err error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", "", errors.New("empty endpoint")
	}
	if strings.HasPrefix(endpoint, "http://") {
		return strings.TrimPrefix(endpoint, "http://"), "http", nil
	}
	if strings.HasPrefix(endpoint, "https://") {
		return strings.TrimPrefix(endpoint, "https://"), "https", nil
	}
	return endpoint, "", nil
}

// buildResource constructs the OTel Resource. Service name defaults to
// spanly-cli unless OTEL_SERVICE_NAME is set; OTEL_RESOURCE_ATTRIBUTES
// is honored automatically by the SDK's resource.Default().
func buildResource(ctx context.Context) (*resource.Resource, error) {
	service := os.Getenv("OTEL_SERVICE_NAME")
	if service == "" {
		service = defaultServiceName
	}
	attrs := []attribute.KeyValue{
		semconv.ServiceName(service),
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithAttributes(attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp: build resource: %w", err)
	}
	return res, nil
}
