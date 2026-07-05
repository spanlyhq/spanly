package run_test

// Integration tests that build the spanly binary and exec it against a
// trivial child (`cat` for stdio; a tiny inline Go HTTP server for http
// mode). They verify the end-to-end happy paths: stdin/stdout piping,
// HTTP proxying, and telemetry shipping.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	spanlyBinary string
	helperBinary string
	buildOnce    sync.Once
	buildErr     error
)

// build compiles both the spanly CLI and the http test helper into a
// shared tempdir, once per test run.
func build(t *testing.T) (string, string) {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "spanly-int-")
		if err != nil {
			buildErr = err
			return
		}
		spanlyBinary = filepath.Join(dir, "spanly")
		helperBinary = filepath.Join(dir, "fake-mcp")

		// Build spanly itself ("../.." → apps/cli, the main module).
		if err := runGoBuild(spanlyBinary, "../.."); err != nil {
			buildErr = err
			return
		}

		// Materialise the helper as its own tiny module and build it.
		helperSrcDir, err := os.MkdirTemp(dir, "helper-")
		if err != nil {
			buildErr = err
			return
		}
		files := map[string]string{
			"main.go": httpHelperSource,
			"go.mod":  "module spanlyfakemcp\n\ngo 1.22\n",
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(helperSrcDir, name), []byte(content), 0o644); err != nil {
				buildErr = err
				return
			}
		}
		if err := runGoBuildIn(helperBinary, ".", helperSrcDir); err != nil {
			buildErr = err
			return
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return spanlyBinary, helperBinary
}

func runGoBuild(out, pkg string) error {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runGoBuildIn(out, pkg, cwd string) error {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = cwd
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func startFakeIngest(t *testing.T) (*httptest.Server, <-chan []byte) {
	t.Helper()
	ch := make(chan []byte, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case ch <- body:
		default:
		}
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("nothing listening on %s after %s", addr, timeout)
}

func pickPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitOrKill(cmd *exec.Cmd, timeout time.Duration) {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(timeout):
		// Wrapper's child is in its own pgid; signal the wrapper, which
		// forwards SIGTERM to the child as designed.
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}
}

func TestRunStdioEchoesAndCollects(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stdio test depends on `cat`")
	}
	bin, _ := build(t)
	ingest, ingestCh := startFakeIngest(t)

	cmd := exec.Command(bin, "run", "--shutdown-grace=2s", "--", "cat")
	cmd.Env = append(os.Environ(),
		"SPANLY_API_KEY=spanly_us_test",
		"SPANLY_INGEST_URL="+ingest.URL,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { waitOrKill(cmd, 3*time.Second) })

	if _, err := fmt.Fprintln(stdin, `{"jsonrpc":"2.0","id":42,"method":"ping"}`); err != nil {
		t.Fatal(err)
	}

	out := bufio.NewScanner(stdout)
	out.Buffer(make([]byte, 64*1024), 1024*1024)
	if !out.Scan() {
		t.Fatalf("no stdout from wrapper: %v", out.Err())
	}
	if !strings.Contains(out.Text(), `"id":42`) {
		t.Errorf("stdout did not echo: %q", out.Text())
	}

	got := 0
	timeout := time.After(3 * time.Second)
	for got < 2 {
		select {
		case <-ingestCh:
			got++
		case <-timeout:
			t.Fatalf("only %d packets collected, want 2", got)
		}
	}

	// Trigger graceful child exit by closing stdin → cat sees EOF → exits.
	// The wrapper then records the pipe close as a synthetic termination
	// notification (GAP-2) before shutting the collector down.
	_ = stdin.Close()

	termTimeout := time.After(3 * time.Second)
	for {
		select {
		case body := <-ingestCh:
			if strings.Contains(string(body), `"spanly/session/terminated"`) {
				return
			}
		case <-termTimeout:
			t.Fatal("no session-terminated packet after stdio close")
		}
	}
}

func TestRunHTTPProxiesAndCollects(t *testing.T) {
	bin, helper := build(t)
	ingest, ingestCh := startFakeIngest(t)

	wrapperPort := pickPort(t)

	cmd := exec.Command(bin, "run",
		"--port", strconv.Itoa(wrapperPort),
		"--shutdown-grace=2s",
		"--child-startup-timeout=10s",
		"--",
		helper,
	)
	cmd.Env = append(os.Environ(),
		"SPANLY_API_KEY=spanly_us_test",
		"SPANLY_INGEST_URL="+ingest.URL,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// SIGTERM lets the wrapper signal-forward to its child and drain
	// telemetry — required to avoid orphan processes after the test exits.
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		waitOrKill(cmd, 5*time.Second)
	})

	if err := waitForTCP(fmt.Sprintf("127.0.0.1:%d", wrapperPort), 15*time.Second); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(
		fmt.Sprintf("http://127.0.0.1:%d/mcp", wrapperPort),
		"application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"tools/call"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `"id":7`) {
		t.Errorf("response body did not include id=7: %s", body)
	}

	got := 0
	timeout := time.After(5 * time.Second)
	for got < 2 {
		select {
		case <-ingestCh:
			got++
		case <-timeout:
			t.Fatalf("only %d packets, want 2", got)
		}
	}
}

const httpHelperSource = `package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		fmt.Fprintln(os.Stderr, "PORT env var not set")
		os.Exit(1)
	}
	http.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, ` + "`{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}`" + `)
	})
	if err := http.ListenAndServe("127.0.0.1:"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
}
`
