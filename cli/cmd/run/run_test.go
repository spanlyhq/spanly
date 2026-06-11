package run

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParseArgsRequiresCommand(t *testing.T) {
	if _, err := parseArgs([]string{}); err == nil {
		t.Fatal("expected error when no command provided")
	}
	if _, err := parseArgs([]string{"--port=3000"}); err == nil {
		t.Fatal("expected error when only flags provided")
	}
}

func TestParseArgsStdioDefault(t *testing.T) {
	cfg, err := parseArgs([]string{"--", "node", "server.js"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpPort != 0 {
		t.Errorf("httpPort = %d, want 0 (stdio)", cfg.httpPort)
	}
	if !reflect.DeepEqual(cfg.args, []string{"node", "server.js"}) {
		t.Errorf("args = %v", cfg.args)
	}
}

func TestParseArgsHTTPMode(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--port=3000",
		"--child-port=3500",
		"--child-port-env=HTTP_PORT",
		"--child-startup-timeout=5s",
		"--inspect-prefix=/mcp",
		"--context-header=X-Tenant=environmentId",
		"--admin-addr=:9090",
		"--",
		"python", "-m", "my_mcp",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpPort != 3000 {
		t.Errorf("httpPort = %d", cfg.httpPort)
	}
	if cfg.childPort != 3500 {
		t.Errorf("childPort = %d", cfg.childPort)
	}
	if cfg.childPortEnv != "HTTP_PORT" {
		t.Errorf("childPortEnv = %q", cfg.childPortEnv)
	}
	if cfg.childStartupTimeout != 5*time.Second {
		t.Errorf("childStartupTimeout = %v", cfg.childStartupTimeout)
	}
	if !reflect.DeepEqual(cfg.inspectPrefix, []string{"/mcp"}) {
		t.Errorf("inspectPrefix = %v", cfg.inspectPrefix)
	}
	if len(cfg.contextHeaders) != 1 || cfg.contextHeaders[0].Field != "environmentId" {
		t.Errorf("contextHeaders = %+v", cfg.contextHeaders)
	}
	if cfg.adminAddr != ":9090" {
		t.Errorf("adminAddr = %q", cfg.adminAddr)
	}
	if !reflect.DeepEqual(cfg.args, []string{"python", "-m", "my_mcp"}) {
		t.Errorf("args = %v", cfg.args)
	}
}

func TestParseArgsRejectsBadContextHeader(t *testing.T) {
	if _, err := parseArgs([]string{"--context-header=bogus", "--", "x"}); err == nil {
		t.Error("expected error for malformed context-header")
	}
	if _, err := parseArgs([]string{"--context-header=X=invalidField", "--", "x"}); err == nil {
		t.Error("expected error for unsupported context field")
	}
}

func TestExitCodeError(t *testing.T) {
	err := &exitCodeError{code: 42}
	if got := ExitCode(err); got != 42 {
		t.Errorf("ExitCode = %d, want 42", got)
	}
	if got := ExitCode(errors.New("regular")); got != -1 {
		t.Errorf("ExitCode for non-exitCode error = %d, want -1", got)
	}
	if got := ExitCode(nil); got != -1 {
		t.Errorf("ExitCode(nil) = %d, want -1", got)
	}
}

func TestModeName(t *testing.T) {
	if modeName(0) != "stdio" {
		t.Errorf("modeName(0) = %q", modeName(0))
	}
	if modeName(3000) != "http" {
		t.Errorf("modeName(3000) = %q", modeName(3000))
	}
}
