package run

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"slices"
	"syscall"
	"time"

	"github.com/spanlyhq/spanly/cli/internal/spanly"
)

// runStdio executes the child with stdin/stdout pipes and forwards
// JSON-RPC frames in both directions while teeing them to the collector.
func runStdio(ctx context.Context, cfg *config, collector *spanly.Collector) (int, error) {
	cmd := exec.Command(cfg.args[0], cfg.args[1:]...)
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = newProcAttrs()

	childStdin, err := cmd.StdinPipe()
	if err != nil {
		return 1, fmt.Errorf("stdin pipe: %w", err)
	}
	childStdout, err := cmd.StdoutPipe()
	if err != nil {
		return 1, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return 1, fmt.Errorf("start child: %w", err)
	}

	transport := spanly.TransportContext{Type: "stdio"}

	// parent stdin -> child stdin. Not waited on: a read on os.Stdin
	// cannot be interrupted, and once the child exits there is nothing
	// left to forward — waiting would hang the wrapper until the MCP
	// client closes its end.
	go func() {
		defer childStdin.Close()
		forwardJSONRPC(os.Stdin, childStdin, "from-client", collector, transport)
	}()

	// child stdout -> parent stdout
	stdoutDone := make(chan struct{})
	go func() {
		defer close(stdoutDone)
		forwardJSONRPC(childStdout, os.Stdout, "to-client", collector, transport)
	}()

	// signal forwarding: when ctx is cancelled (SIGINT/TERM/HUP received),
	// pass the signal along and give the child --shutdown-grace to exit.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if cmd.Process != nil {
				_ = cmd.Process.Signal(syscall.SIGTERM)
			}
			select {
			case <-done:
			case <-time.After(cfg.shutdownGrace):
				if cmd.Process != nil {
					log.Printf("child did not exit within %s, sending SIGKILL", cfg.shutdownGrace)
					_ = cmd.Process.Kill()
				}
			}
		case <-done:
		}
	}()

	exitErr := cmd.Wait()
	close(done)
	<-stdoutDone

	if exitErr == nil {
		return 0, nil
	}
	if ee, ok := exitErr.(*exec.ExitError); ok {
		return ee.ExitCode(), nil
	}
	return 1, exitErr
}

// forwardJSONRPC reads line-delimited frames from in, copies each line to
// out unchanged, and ships any line that parses as a JSON-RPC 2.0 packet
// to the collector with the given direction and transport context.
//
// The bufio.Scanner buffer is reused across iterations, so we clone the
// JSON-RPC payload before enqueueing — the collector retains the bytes
// asynchronously.
func forwardJSONRPC(in io.Reader, out io.Writer, direction string, collector *spanly.Collector, transport spanly.TransportContext) {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), spanly.MaxInspectBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if packet, ok := spanly.ParseMCPPacket(line); ok {
			collector.Collect(direction, spanly.PacketContext{}, transport, slices.Clone(packet))
		}
		if _, err := out.Write(line); err != nil {
			log.Printf("forward write: %v", err)
			return
		}
		if _, err := out.Write([]byte{'\n'}); err != nil {
			log.Printf("forward write: %v", err)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("forward scan: %v", err)
	}
}
