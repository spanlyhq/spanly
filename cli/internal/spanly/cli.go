package spanly

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DefaultInspectPrefix is the default comma-separated list of path
// prefixes inspected for JSON-RPC in HTTP mode.
const DefaultInspectPrefix = "/mcp,/sse"

// PipelineOptions configures NewPipelineFromEnv.
type PipelineOptions struct {
	BufferSize     int
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// NewPipelineFromEnv builds the telemetry pipeline shared by both
// subcommands: a Spanly sink configured from SPANLY_API_KEY and
// SPANLY_INGEST_URL, feeding a Collector with a fresh client/monitor
// identity. The caller still owns the collector's Start/Close lifecycle.
func NewPipelineFromEnv(opts PipelineOptions) (*Collector, *SpanlySink, error) {
	apiKey := os.Getenv("SPANLY_API_KEY")
	if apiKey == "" {
		return nil, nil, errors.New("SPANLY_API_KEY environment variable is required")
	}
	sink, err := NewSpanlySink(SpanlySinkOptions{
		APIKey:         apiKey,
		IngestURL:      os.Getenv("SPANLY_INGEST_URL"),
		MaxAttempts:    opts.MaxAttempts,
		InitialBackoff: opts.InitialBackoff,
		MaxBackoff:     opts.MaxBackoff,
	})
	if err != nil {
		return nil, nil, err
	}
	collector, err := NewCollector(CollectorOptions{
		ClientID:   uuid.NewString(),
		MonitorID:  uuid.NewString(),
		BufferSize: opts.BufferSize,
	}, sink)
	if err != nil {
		return nil, nil, err
	}
	return collector, sink, nil
}

// ParseInspectPrefix splits a comma-separated prefix list, trimming
// whitespace and dropping empty entries. Returns nil for an empty input,
// which signals "inspect all paths" downstream.
func ParseInspectPrefix(s string) []string {
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

// ParseContextHeaders parses repeated HEADER=field flag values into
// HeaderMapping entries, validating each target field.
func ParseContextHeaders(raw []string) ([]HeaderMapping, error) {
	var out []HeaderMapping
	for _, entry := range raw {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("--context-header %q must be of form HEADER=field", entry)
		}
		m := HeaderMapping{Header: parts[0], Field: parts[1]}
		if err := ValidateContextField(m.Field); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// StringSliceFlag is a flag.Value that collects every occurrence of a
// repeatable string flag.
type StringSliceFlag []string

func (s *StringSliceFlag) String() string     { return strings.Join(*s, ",") }
func (s *StringSliceFlag) Set(v string) error { *s = append(*s, v); return nil }
