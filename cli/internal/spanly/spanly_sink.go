package spanly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultIngestURLUS = "https://ingest-us.spanly.dev"
	defaultIngestURLEU = "https://ingest-eu.spanly.dev"
	collectPath        = "/collect"
	collectTimeout     = 5 * time.Second

	defaultMaxAttempts    = 3
	defaultInitialBackoff = 1 * time.Second
	defaultMaxBackoff     = 30 * time.Second
)

var (
	errEmptyIdentity = errors.New("ClientID and MonitorID are required")
	errNoSinks       = errors.New("at least one Sink is required")
)

func joinSinkErrors(errs []error) error {
	return errors.Join(errs...)
}

// SpanlySinkOptions configures the Spanly HTTP sink.
//
// APIKey is required and drives region detection (us / eu) when
// IngestURL is empty. MaxAttempts/InitialBackoff/MaxBackoff govern the
// per-packet retry policy on transient (5xx, network) failures; 4xx
// responses are non-retryable and logged.
type SpanlySinkOptions struct {
	APIKey    string
	IngestURL string

	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// SpanlySink ships packets to Spanly's ingest endpoint via HTTP POST,
// with bounded exponential-backoff retry on transient failures.
type SpanlySink struct {
	apiKey    string
	ingestURL string
	client    *http.Client

	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration

	sent           atomic.Int64
	failed         atomic.Int64
	attemptsTotal  atomic.Int64
	attemptsFailed atomic.Int64
}

// NewSpanlySink validates options and returns a ready-to-use Spanly sink.
func NewSpanlySink(opts SpanlySinkOptions) (*SpanlySink, error) {
	if opts.APIKey == "" {
		return nil, errors.New("APIKey is required")
	}
	base := opts.IngestURL
	if base == "" {
		switch {
		case strings.HasPrefix(opts.APIKey, "spanly_us_"):
			base = defaultIngestURLUS
		case strings.HasPrefix(opts.APIKey, "spanly_eu_"):
			base = defaultIngestURLEU
		default:
			return nil, fmt.Errorf("invalid API key format: must start with spanly_us_ or spanly_eu_")
		}
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = defaultInitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultMaxBackoff
	}
	return &SpanlySink{
		apiKey:         opts.APIKey,
		ingestURL:      strings.TrimRight(base, "/") + collectPath,
		client:         &http.Client{Timeout: collectTimeout},
		maxAttempts:    opts.MaxAttempts,
		initialBackoff: opts.InitialBackoff,
		maxBackoff:     opts.MaxBackoff,
	}, nil
}

// IngestURL returns the resolved ingest endpoint, useful for startup logs.
func (s *SpanlySink) IngestURL() string { return s.ingestURL }

func (s *SpanlySink) Name() string { return "spanly" }

func (s *SpanlySink) Metrics() SinkMetrics {
	return SinkMetrics{
		Sent:           s.sent.Load(),
		Failed:         s.failed.Load(),
		AttemptsTotal:  s.attemptsTotal.Load(),
		AttemptsFailed: s.attemptsFailed.Load(),
	}
}

func (s *SpanlySink) Shutdown(_ context.Context) error { return nil }

// Export POSTs the packet to the ingest endpoint with retry. Returns nil
// on terminal outcome (success or non-retryable rejection); a non-nil
// error means retries were exhausted on a transient failure.
func (s *SpanlySink) Export(ctx context.Context, packet SpanlyPacket) error {
	backoff := s.initialBackoff
	var lastErr error
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		s.attemptsTotal.Add(1)
		err := s.send(ctx, packet)
		if err == nil {
			s.sent.Add(1)
			return nil
		}
		s.attemptsFailed.Add(1)
		lastErr = err
		if attempt >= s.maxAttempts {
			break
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			s.failed.Add(1)
			return ctx.Err()
		}
		backoff *= 2
		if backoff > s.maxBackoff {
			backoff = s.maxBackoff
		}
	}
	s.failed.Add(1)
	return lastErr
}

// send returns nil for success or non-retryable failure (e.g. 4xx),
// and an error for retryable failures (network errors, 5xx).
func (s *SpanlySink) send(ctx context.Context, packet SpanlyPacket) error {
	body, err := json.Marshal(packet)
	if err != nil {
		log.Printf("spanly: failed to marshal packet: %v", err)
		return nil
	}
	sendCtx, cancel := context.WithTimeout(ctx, collectTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, s.ingestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 500:
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ingest %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	case resp.StatusCode >= 400:
		msg, _ := io.ReadAll(resp.Body)
		log.Printf("spanly: collect rejected %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
		return nil
	default:
		return nil
	}
}
