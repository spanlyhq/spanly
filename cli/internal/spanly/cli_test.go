package spanly

import (
	"reflect"
	"testing"
)

func TestParseInspectPrefix(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"/mcp", []string{"/mcp"}},
		{"/mcp,/sse", []string{"/mcp", "/sse"}},
		{" /mcp , /sse ", []string{"/mcp", "/sse"}},
		{",,/mcp,", []string{"/mcp"}},
	}
	for _, tc := range cases {
		got := ParseInspectPrefix(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseInspectPrefix(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseContextHeaders(t *testing.T) {
	good, err := ParseContextHeaders([]string{
		"X-Tenant=environmentId",
		"X-Org=organisationId",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []HeaderMapping{
		{Header: "X-Tenant", Field: "environmentId"},
		{Header: "X-Org", Field: "organisationId"},
	}
	if !reflect.DeepEqual(good, want) {
		t.Errorf("got %+v, want %+v", good, want)
	}

	if _, err := ParseContextHeaders([]string{"no-equals"}); err == nil {
		t.Error("expected error for malformed entry")
	}
	if _, err := ParseContextHeaders([]string{"X=invalidField"}); err == nil {
		t.Error("expected error for unknown context field")
	}
	if _, err := ParseContextHeaders([]string{"=environmentId"}); err == nil {
		t.Error("expected error for empty header name")
	}
}

func TestNewPipelineFromEnv(t *testing.T) {
	t.Setenv("SPANLY_API_KEY", "")
	if _, _, err := NewPipelineFromEnv(PipelineOptions{}); err == nil {
		t.Error("expected error when SPANLY_API_KEY is unset")
	}

	t.Setenv("SPANLY_API_KEY", "spanly_us_key")
	t.Setenv("SPANLY_INGEST_URL", "http://localhost:1")
	collector, sink, err := NewPipelineFromEnv(PipelineOptions{BufferSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	if sink.IngestURL() != "http://localhost:1/collect" {
		t.Errorf("IngestURL = %q", sink.IngestURL())
	}
	if got := len(collector.Sinks()); got != 1 {
		t.Errorf("sinks = %d, want 1", got)
	}
}
