# spanly

Python SDK for [Spanly](https://spanly.com) — observability for MCP
(Model Context Protocol) servers and AI agents.

Wrap your MCP server in one line and Spanly captures every tool call,
prompt, resource access, and JSON-RPC packet, with full payloads and
timings.

## When to use the SDK vs the CLI

For most users the **CLI** (`@spanly/spanly`) is the easiest onramp — it
wraps your MCP server with zero code changes and works in any language:

```bash
npx -y @spanly/spanly run -- python -m my_mcp
```

Reach for **this SDK** when you need any of:

- **Per-request hooks** (`on_collect`, `on_error`) — mutate or filter
  packets, attach custom context (multi-tenant tagging, user IDs), drop
  traffic by predicate.
- **In-process embedding** — no extra binary, no extra container, no
  process supervision.
- **Test integration** — direct control over the monitor lifecycle from
  `pytest`.

If none of those apply, prefer the CLI.

## Install

```bash
pip install spanly
# or: uv add spanly / poetry add spanly
```

Requires Python 3.10+.

## Usage

```python
import os
from spanly import SpanlyClient

spanly = SpanlyClient(api_key=os.environ["SPANLY_API_KEY"])
spanly.monitor(server)

# server is an MCPServer / FastMCP / low-level Server from `mcp`
server.run()
```

That's it — all MCP traffic on `server` is now reported to your Spanly
project.

`SpanlyClient()` also reads `SPANLY_API_KEY` directly from the environment,
so the shorter form works too:

```python
from spanly import SpanlyClient
SpanlyClient().monitor(server)
```

Get an API key by signing up at [spanly.com](https://spanly.com) and
creating a project. Region (`us` / `eu`) is encoded in the key prefix
and auto-detected.

### Per-request hooks

```python
from spanly import SpanlyClient, MonitorOptions

def on_collect(direction, context, packet):
    context.environment_id = current_tenant_id()
    if looks_sensitive(packet):
        return None  # drop
    return packet

SpanlyClient().monitor(server, MonitorOptions(
    on_collect=on_collect,
    on_error=lambda e: log.error("spanly: %s", e),
))
```

### Trace context propagation

The SDK preserves the W3C `traceparent` value verbatim on every captured
packet — the HTTP header on HTTP transports, the
`params._meta.traceparent` field on stdio. Pick your APM provider in the
Spanly dashboard (Settings, Integrations) and every request with trace
context links straight to the matching trace in Datadog, Sentry or
New Relic.

Capture is automatic — no SDK configuration, no extra dependency, no
spans emitted into your APM.

## Links

- [spanly.com](https://spanly.com)
- [Documentation](https://spanly.com/docs/python-sdk/)
- [TypeScript SDK (`@spanly/sdk`)](https://www.npmjs.com/package/@spanly/sdk)
- [Spanly CLI (`@spanly/spanly`)](https://github.com/spanlyhq/spanly/tree/main/cli)
