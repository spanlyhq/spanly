package run

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

// runHTTP boots the child on a random/configured port, waits for it to
// listen, and starts the existing HTTP/SSE proxy on the wrapper's --port.
func runHTTP(ctx context.Context, cfg *config, collector *spanly.Collector, version string) (int, error) {
	childPort, err := pickChildPort(cfg.childPort)
	if err != nil {
		return 1, fmt.Errorf("pick child port: %w", err)
	}

	cmd := exec.Command(cfg.args[0], cfg.args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = newProcAttrs()
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%d", cfg.childPortEnv, childPort))

	if err := cmd.Start(); err != nil {
		return 1, fmt.Errorf("start child: %w", err)
	}

	// Concurrent: wait for the child + wait for it to listen. Whichever
	// happens first determines what we do next.
	childExited := make(chan error, 1)
	go func() { childExited <- cmd.Wait() }()

	if err := waitForListen(ctx, childPort, cfg.childStartupTimeout, childExited); err != nil {
		// Try to clean up the child if it's still running.
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		<-childExited
		return 1, err
	}

	upstream := &url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", childPort)}
	proxy, err := spanly.NewProxy(spanly.ProxyOptions{
		Upstream:        upstream,
		Collector:       collector,
		InspectPrefix:   cfg.inspectPrefix,
		ContextHeaders:  cfg.contextHeaders,
		RedactHeaders:   cfg.redactHeaders,
		InjectSessionID: cfg.injectSessionID,
	})
	if err != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		<-childExited
		return 1, err
	}

	bindAddr := fmt.Sprintf(":%d", cfg.httpPort)
	server := &http.Server{
		Addr:              bindAddr,
		Handler:           proxy,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("wrapper listening on %s -> child localhost:%d (version %s, inspect=%v)",
		bindAddr, childPort, version, cfg.inspectPrefix)

	adminErrCh := make(chan error, 1)
	if cfg.adminAddr != "" {
		admin := spanly.NewAdminServer(cfg.adminAddr, upstream, collector, proxy)
		admin.Start(ctx, adminErrCh)
		log.Printf("admin server listening on %s", cfg.adminAddr)
	}

	serverErrCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverErrCh <- err
	}()

	// Block until either: the wrapper is signalled, the child exits, or
	// one of the wrapper's listeners errors out.
	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining...")
		// Tell the child to shut down gracefully.
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutCtx)

		select {
		case err := <-childExited:
			return childExitCode(err), nil
		case <-time.After(cfg.shutdownGrace):
			if cmd.Process != nil {
				log.Printf("child did not exit within %s, sending SIGKILL", cfg.shutdownGrace)
				_ = cmd.Process.Kill()
			}
			err := <-childExited
			return childExitCode(err), nil
		}

	case err := <-childExited:
		log.Printf("child exited; shutting down wrapper")
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutCtx)
		return childExitCode(err), nil

	case err := <-serverErrCh:
		// Wrapper's listener failed — kill child, propagate.
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		<-childExited
		return 1, fmt.Errorf("proxy listener: %w", err)

	case err := <-adminErrCh:
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		<-childExited
		return 1, fmt.Errorf("admin server: %w", err)
	}
}

// pickChildPort returns the configured port if non-zero, otherwise asks
// the kernel for a free port (binds, reads address, closes — the port
// may race but is overwhelmingly likely to remain free for the brief
// window between close and the child binding).
func pickChildPort(want int) (int, error) {
	if want > 0 {
		return want, nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("unexpected listener address type")
	}
	return addr.Port, nil
}

// waitForListen polls the child port via TCP dial until it accepts a
// connection, the timeout elapses, or the child exits early.
func waitForListen(ctx context.Context, port int, timeout time.Duration, childExited <-chan error) error {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	target := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for {
		conn, err := net.DialTimeout("tcp", target, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-tick.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("child did not begin listening on port %d within %s", port, timeout)
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-childExited:
			return fmt.Errorf("child exited before listening: %v", err)
		}
	}
}

func childExitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return 1
}
