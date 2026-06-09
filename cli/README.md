# spanly — observability CLI for MCP servers

A small Go binary that captures traffic from your MCP server and ships it
to [Spanly](https://spanly.com). Two modes:

- **`spanly run -- <cmd>`** — wraps a child MCP server you start.
  Works for both **stdio** and **HTTP** transports. Zero code change,
  zero MCP-client config change. _This is the default path._
- **`spanly proxy <upstream> <bind>`** — standalone HTTP/SSE reverse
  proxy. Use when you can't wrap the child (third-party services,
  K8s declarative sidecar containers, network-level interception).

For language-specific in-process control (per-request `onCollect` hooks,
multi-tenant tagging beyond `--context-header`), see
[`@spanly/sdk`](../../packages/public/spanly) and
[`spanly-python`](../../packages/public/spanly-python).

## 30-second demo

```bash
export SPANLY_API_KEY="spanly_us_..."
npx -y @spanly/spanly run -- node ./server.js
```

That's it. Run your MCP server normally — telemetry shows up in the
Spanly dashboard.

For HTTP MCP servers, set `--port`:

```bash
spanly run --port 3000 -- node ./server.js
```

The wrapper takes port 3000; the child gets a random port and reads it
from `PORT`. **Your MCP client URL doesn't change.**

## Install

### Quick install (macOS, Linux)

```bash
curl -fsSL https://spanly.com/install.sh | sh
```

Detects your OS and architecture, verifies the checksum, and installs the
binary onto your `PATH`. Override with `SPANLY_VERSION` or
`SPANLY_INSTALL_DIR`.

### npm (recommended for MCP client configs)

```bash
npx -y @spanly/spanly run -- <your-mcp-command>
```

Runs without any pre-install — ideal for embedding directly in Claude
Desktop, Cursor, or Windsurf server configs.

### Homebrew

```bash
brew install spanlyhq/tap/spanly
```

### Direct download

Grab the latest binary from the [Releases page](https://github.com/spanlyhq/spanly/releases?q=cli-v).

### From source

```bash
go install github.com/spanlyhq/spanly/cli@latest
```

### Docker

```bash
docker run -e SPANLY_API_KEY=spanly_us_... \
  spanly/spanly:latest proxy host.docker.internal:3000 :3001
```

## `spanly run` reference

```
spanly run [flags] -- <command> [args...]
```

| Flag | Default | Description |
|---|---|---|
| `--port` | `0` | If set, runs in HTTP mode (wrapper takes this port; child gets a random one). Default `0` = stdio. |
| `--child-port` | `0` | Port the child binds in HTTP mode. `0` = pick random unused port. |
| `--child-port-env` | `PORT` | Env var passed to child with the chosen port. |
| `--child-startup-timeout` | `30s` | Max wait for child to listen. |
| `--inspect-prefix` | `/mcp,/sse` | Comma-separated path prefixes to inspect (HTTP mode). Empty = inspect all. |
| `--context-header` | _none_ | `HEADER=field` mapping. Repeatable. Fields: `environmentId`, `projectId`, `organisationId`. |
| `--redact-header` | _none_ | Additional header to redact from captured telemetry (HTTP mode). Repeatable. `Authorization`, `Cookie`, `Set-Cookie`, `Proxy-Authorization` and `X-Api-Key` are always redacted. |
| `--buffer-size` | `10000` | Max packets buffered when ingest is unreachable. |
| `--retry-max-attempts` | `3` | Max POST attempts per packet. |
| `--retry-backoff` | `1s` | Initial retry backoff (exponential). |
| `--retry-max-backoff` | `30s` | Cap on retry backoff. |
| `--shutdown-grace` | `10s` | Time to flush in-flight telemetry on shutdown. |
| `--admin-addr` | _disabled_ | `/healthz`, `/readyz`, `/metrics` listener (e.g. `:9090`). |

Examples:

```bash
spanly run -- node server.js
spanly run --port 3000 -- python -m my_mcp
spanly run --port 3000 --admin-addr=:9090 -- ./my-mcp-server
spanly run --port 3000 --context-header=X-Tenant=environmentId -- ./srv
```

## `spanly proxy` reference

```
spanly proxy [flags] <upstream> <bind>
```

Same flags as `spanly run` (excluding `--port`, `--child-*`). `<upstream>`
is the MCP server you want to monitor; `<bind>` is the address your MCP
client connects to instead.

Example:

```bash
export SPANLY_API_KEY="spanly_us_..."
spanly proxy localhost:3000 localhost:3001
```

## Per-request headers

Inbound headers recognized by both `run` and `proxy`:

| Header | Effect |
|---|---|
| `X-Spanly-Monitor-Id` | Override `spanlyMonitorId` for this request. |
| Any header named in `--context-header` | Maps to the corresponding context field. |

## Admin endpoints

When `--admin-addr` is set:

- `GET /healthz` — 200 if the listener is up.
- `GET /readyz` — 200 if the upstream is reachable (1s cache).
- `GET /metrics` — Prometheus text format. Counters: packets collected/sent/dropped/failed, retry attempts, buffer depth, request counts by inspection class.

## OpenTelemetry

The CLI does not export OTel spans — it ships telemetry to Spanly only.
The inbound `traceparent` header (when present) is preserved verbatim
on each captured packet, and the Spanly dashboard parses it at view
time to render a cross-link to the matching span in your APM (Datadog,
Sentry, Honeycomb, …). No configuration required.

## What gets captured

For every JSON-RPC packet on inspected paths:

- The raw JSON-RPC 2.0 body.
- HTTP method, path, and headers (including `mcp-session-id`) — HTTP mode only.
  Credential-bearing headers (`Authorization`, `Cookie`, `Set-Cookie`,
  `Proxy-Authorization`, `X-Api-Key`) are replaced with `[REDACTED]` before
  the packet leaves your machine; add more with `--redact-header`. The
  proxied request and response keep their original headers.
- A timestamp and the per-process `spanlyClientId`/`spanlyMonitorId`.

SSE (`text/event-stream`) responses are streamed to the client verbatim; each
`data:` frame is parsed and emitted as a separate telemetry packet.

Telemetry delivery is bounded-buffered + retried. Network failures don't
block the proxied request.

## Putting Spanly behind nginx / Caddy / Envoy (SSE notes)

If you front Spanly with a reverse proxy, SSE responses can stall in the
front proxy's response buffer. Configure pass-through:

**nginx:**
```nginx
location /mcp {
    proxy_pass http://spanly:3001;
    proxy_buffering off;
    proxy_cache off;
    proxy_set_header X-Accel-Buffering no;
    proxy_read_timeout 1h;
}
```

**Caddy:**
```caddy
reverse_proxy spanly:3001 {
    flush_interval -1
}
```

**Envoy:**
- Disable response buffering on the relevant route.
- Set `auto_host_rewrite: true` if Spanly is selected by name.

## Production deploy

| Tool | Where |
|---|---|
| Helm chart | [`charts/spanly`](../../charts/spanly) — standalone Pod + Service in front of an internal MCP. |
| Kustomize component | [`kustomize/spanly-sidecar`](../../kustomize/spanly-sidecar) — co-locate spanly as a sidecar in your existing Pod. |
| Docker image | `spanly/spanly:<tag>`, `ghcr.io/spanly/spanly:<tag>` |

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `SPANLY_API_KEY` | yes | Region detected from prefix (`spanly_us_*` / `spanly_eu_*`). |
| `SPANLY_INGEST_URL` | no | Override ingest base URL (self-hosted / local dev). |

## Development

```bash
go build -o dist/spanly .
go test ./...
go vet ./...
```

## Migrating from the old `spanly <upstream> [bind]` syntax

See [MIGRATING.md](./MIGRATING.md). TL;DR: every command needs a
subcommand now (`spanly proxy ...` or `spanly run ...`).

## What's not (yet) supported

- **Windows** — best-effort, not regression-tested. Use WSL.
- **WebSockets** — only HTTP, SSE, and stdio.
- **TLS termination** on the bind side — front spanly with your own
  reverse proxy.
