package run

import (
	"bufio"
	"bytes"
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

	// The pipe is closed in both directions: nothing else will be captured
	// for this session. Record the end explicitly — without it a clean stdio
	// shutdown is indistinguishable from an abandoned session (GAP-2). The
	// collector is still open here; the caller's Close() flushes the queue.
	collector.Collect("from-client", spanly.PacketContext{}, transport, spanly.StdioSessionTerminatedPacket())

	if exitErr == nil {
		return 0, nil
	}
	if ee, ok := exitErr.(*exec.ExitError); ok {
		return ee.ExitCode(), nil
	}
	return 1, exitErr
}

// forwardJSONRPC reads line-delimited frames from in, copies each line to out
// unchanged, and ships any line that parses as a JSON-RPC 2.0 packet to the
// collector with the given direction and transport context.
//
// A line over MaxInspectBytes is still forwarded whole (a bufio.Scanner would
// instead fail with ErrTooLong and abandon the rest of the stream), and a
// metadata-only event is emitted for it (see GAP-3 in
// docs/LIVE_SCAN_CAPTURE_GUARANTEES.md) so the oversized frame stays visible to
// aggregations and size-based checks. The line buffer is reused across
// iterations, so the JSON-RPC payload is cloned before enqueueing — the
// collector retains the bytes asynchronously.
func forwardJSONRPC(in io.Reader, out io.Writer, direction string, collector *spanly.Collector, transport spanly.TransportContext) {
	reader := bufio.NewReaderSize(in, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := out.Write(line); werr != nil {
				log.Printf("forward write: %v", werr)
				return
			}
			frame := bytes.TrimSuffix(line, []byte{'\n'})
			if len(frame) > spanly.MaxInspectBytes {
				if stub := spanly.BuildOversizedStub(frame); stub != nil {
					collector.CollectOversized(direction, spanly.PacketContext{}, transport, stub, int64(len(frame)))
				}
			} else if packet, ok := spanly.ParseMCPPacket(frame); ok {
				collector.Collect(direction, spanly.PacketContext{}, transport, slices.Clone(packet))
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("forward read: %v", err)
			}
			return
		}
	}
}
