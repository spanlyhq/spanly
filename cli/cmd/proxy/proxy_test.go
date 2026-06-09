package proxy

import (
	"reflect"
	"testing"
	"time"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

func TestParseUpstream(t *testing.T) {
	cases := []struct {
		in      string
		wantURL string
		wantErr bool
	}{
		{"localhost:3000", "http://localhost:3000", false},
		{"http://localhost:3000/mcp", "http://localhost:3000/mcp", false},
		{"https://mcp.example.com", "https://mcp.example.com", false},
		{"", "", true},
	}
	for _, tc := range cases {
		u, err := parseUpstream(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseUpstream(%q) err = %v, wantErr = %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && u.String() != tc.wantURL {
			t.Errorf("parseUpstream(%q) = %q, want %q", tc.in, u.String(), tc.wantURL)
		}
	}
}

func TestNormalizeBindRequired(t *testing.T) {
	if _, err := normalizeBind(""); err == nil {
		t.Fatal("expected error for empty bind")
	}
}

func TestNormalizeBindValid(t *testing.T) {
	got, err := normalizeBind("0.0.0.0:9999")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.0.0.0:9999" {
		t.Errorf("got %q", got)
	}
}

func TestNormalizeBindInvalid(t *testing.T) {
	if _, err := normalizeBind("not-a-host-port"); err == nil {
		t.Fatal("expected error for invalid bind")
	}
}

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
		got := parseInspectPrefix(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseInspectPrefix(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseContextHeaders(t *testing.T) {
	good, err := parseContextHeaders([]string{
		"X-Tenant=environmentId",
		"X-Org=organisationId",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []spanly.HeaderMapping{
		{Header: "X-Tenant", Field: "environmentId"},
		{Header: "X-Org", Field: "organisationId"},
	}
	if !reflect.DeepEqual(good, want) {
		t.Errorf("got %+v, want %+v", good, want)
	}

	if _, err := parseContextHeaders([]string{"no-equals"}); err == nil {
		t.Error("expected error for malformed entry")
	}
	if _, err := parseContextHeaders([]string{"X=invalidField"}); err == nil {
		t.Error("expected error for unknown context field")
	}
	if _, err := parseContextHeaders([]string{"=environmentId"}); err == nil {
		t.Error("expected error for empty header name")
	}
}

func TestParseArgsRequiresBothPositionals(t *testing.T) {
	if _, err := parseArgs([]string{"localhost:3000"}); err == nil {
		t.Error("expected error when bind missing")
	}
	if _, err := parseArgs([]string{}); err == nil {
		t.Error("expected error when both args missing")
	}
}

func TestParseArgsDefaults(t *testing.T) {
	cfg, err := parseArgs([]string{"localhost:3000", "localhost:3001"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.bindAddr != "localhost:3001" {
		t.Errorf("bindAddr = %q", cfg.bindAddr)
	}
	if cfg.bufferSize != 10000 {
		t.Errorf("bufferSize = %d", cfg.bufferSize)
	}
	if cfg.maxAttempts != 3 {
		t.Errorf("maxAttempts = %d", cfg.maxAttempts)
	}
	if cfg.shutdownGrace != 10*time.Second {
		t.Errorf("shutdownGrace = %v", cfg.shutdownGrace)
	}
	if !reflect.DeepEqual(cfg.inspectPrefix, []string{"/mcp", "/sse"}) {
		t.Errorf("inspectPrefix = %v", cfg.inspectPrefix)
	}
}

func TestParseArgsCustom(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--inspect-prefix=/mcp",
		"--context-header=X-Tenant=environmentId",
		"--redact-header=X-Custom-Token",
		"--redact-header=X-Other-Secret",
		"--buffer-size=42",
		"--admin-addr=:9090",
		"localhost:3000",
		":3001",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.inspectPrefix, []string{"/mcp"}) {
		t.Errorf("inspectPrefix = %v", cfg.inspectPrefix)
	}
	if cfg.bufferSize != 42 {
		t.Errorf("bufferSize = %d", cfg.bufferSize)
	}
	if cfg.adminAddr != ":9090" {
		t.Errorf("adminAddr = %q", cfg.adminAddr)
	}
	if len(cfg.contextHeaders) != 1 || cfg.contextHeaders[0].Field != "environmentId" {
		t.Errorf("contextHeaders = %+v", cfg.contextHeaders)
	}
	if !reflect.DeepEqual(cfg.redactHeaders, []string{"X-Custom-Token", "X-Other-Secret"}) {
		t.Errorf("redactHeaders = %v", cfg.redactHeaders)
	}
}
