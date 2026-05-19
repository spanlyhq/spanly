// Command spanly is the Spanly observability CLI for MCP servers.
//
// Two subcommands:
//
//	spanly run -- <cmd>      wrap a child process; ship telemetry (stdio + HTTP)
//	spanly proxy <upstream>  standalone HTTP/SSE proxy for an existing server
//
// Use `run` whenever you start your own MCP server. Use `proxy` only when
// wrapping is impossible (third-party services, K8s declarative sidecar
// pattern, network-level interception).
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spanlyhq/spanly/cli/cmd/proxy"
	"github.com/spanlyhq/spanly/cli/cmd/run"
)

var version = "dev"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("spanly: ")

	err := dispatch(os.Args[1:])
	if err == nil {
		return
	}
	if code := run.ExitCode(err); code >= 0 {
		// Child of `spanly run` exited non-zero. Don't print our own
		// error — the child already wrote whatever it wanted to stderr.
		os.Exit(code)
	}
	fmt.Fprintf(os.Stderr, "spanly: %v\n", err)
	os.Exit(1)
}

func dispatch(args []string) error {
	if len(args) == 0 {
		printUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "run":
		return run.Main(args[1:], version)
	case "proxy":
		return proxy.Main(args[1:], version)
	case "version", "-v", "--version":
		fmt.Println(version)
		return nil
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `spanly — observability for MCP servers

Usage:
  spanly run [flags] -- <cmd> [args...]   wrap a child MCP server; ship telemetry
  spanly proxy <upstream> [bind]          standalone HTTP/SSE proxy
  spanly version                          print version

Run 'spanly <subcommand> --help' for subcommand-specific help.`)
}
