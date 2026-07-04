package run

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

type captureSink struct {
	mu      sync.Mutex
	packets []spanly.SpanlyPacket
}

func (s *captureSink) Export(_ context.Context, p spanly.SpanlyPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.packets = append(s.packets, p)
	return nil
}
func (s *captureSink) Shutdown(context.Context) error { return nil }
func (s *captureSink) Name() string                   { return "capture" }
func (s *captureSink) Metrics() spanly.SinkMetrics    { return spanly.SinkMetrics{} }

func (s *captureSink) snapshot() []spanly.SpanlyPacket {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]spanly.SpanlyPacket, len(s.packets))
	copy(out, s.packets)
	return out
}

// An oversized stdio line must still be forwarded whole (a bufio.Scanner would
// abandon the rest of the stream) and must produce a metadata-only event.
func TestForwardJSONRPCOversizedLine(t *testing.T) {
	sink := &captureSink{}
	c, err := spanly.NewCollector(spanly.CollectorOptions{ClientID: "c", MonitorID: "m"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	c.Start()

	small := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
	oversized := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"big","arguments":{"pad":"` +
		strings.Repeat("a", spanly.MaxInspectBytes) + `"}}}`
	after := `{"jsonrpc":"2.0","id":3,"method":"ping"}`
	input := small + "\n" + oversized + "\n" + after + "\n"

	var out bytes.Buffer
	forwardJSONRPC(strings.NewReader(input), &out, "from-client", c, spanly.TransportContext{Type: "stdio"})

	if err := c.Close(2 * time.Second); err != nil {
		t.Fatalf("collector close: %v", err)
	}

	if out.String() != input {
		t.Errorf("forwarded stream differs from input (len got %d, want %d)", out.Len(), len(input))
	}

	packets := sink.snapshot()
	if len(packets) != 3 {
		t.Fatalf("expected 3 collected packets, got %d", len(packets))
	}
	var oversizedCount int
	for _, p := range packets {
		if p.Oversized != nil {
			oversizedCount++
			if p.Oversized.OriginalSize != int64(len(oversized)) {
				t.Errorf("OriginalSize = %d, want %d", p.Oversized.OriginalSize, len(oversized))
			}
		}
	}
	if oversizedCount != 1 {
		t.Errorf("expected exactly 1 oversized packet, got %d", oversizedCount)
	}
}
