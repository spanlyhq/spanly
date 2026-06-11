# @spanly/sdk

TypeScript SDK for [Spanly](https://spanly.com), observability for MCP
(Model Context Protocol) servers and AI agents.

Wrap your MCP server in one line and Spanly captures every tool call,
prompt, resource access, and JSON-RPC packet, with full requests and timings.

## When to use the SDK vs the CLI

For most users the **CLI** (`@spanly/spanly`) is the easiest onramp. It
wraps your MCP server with zero code changes and works in any language:

```bash
npx -y @spanly/spanly run -- node ./server.js
```

Reach for **this SDK** when you need any of:

- **Per-request hooks** (`onCollect`, `onError`): mutate or filter
  packets, attach custom context, drop traffic by predicate.
- **Per-request multi-tenant tagging** beyond what `--context-header`
  covers (e.g. extract tenant from auth tokens, JWT claims, request
  body, application-level state).
- **In-process embedding**: no extra binary, no extra container, no
  process supervision.
- **Test integration**: direct control over the monitor lifecycle from
  Jest / Vitest setups.

If none of those apply, prefer the CLI.

## Install

```bash
npm install @spanly/sdk
# or: pnpm add @spanly/sdk / yarn add @spanly/sdk / bun add @spanly/sdk
```

## Usage

```ts
import { SpanlyClient } from '@spanly/sdk';

const spanly = new SpanlyClient({
  apiKey: process.env.SPANLY_API_KEY,
});

spanly.monitor(mcpServer);
```

That's it. All MCP traffic on `mcpServer` is now reported to your
Spanly project.

Get an API key by signing up at [spanly.com](https://spanly.com) and
creating a project.

### Per-request hooks

```ts
spanly.monitor(mcpServer, {
  onCollect: (direction, context, mcpPacket) => {
    // attach tenant from the active request scope
    context.environmentId = currentTenantId();
    // drop sensitive tools entirely
    if (looksSensitive(mcpPacket)) return null;
    return mcpPacket;
  },
  onError: (err) => log.error('spanly:', err),
});
```

### Session grouping for stateless servers

If your Streamable HTTP transport runs sessionless
(`sessionIdGenerator: undefined`), the SDK assigns a synthetic
`Mcp-Session-Id` (prefixed `spanly-`) on initialize responses so Spanly
can still group requests into sessions. Servers that assign their own
session IDs are untouched. Opt out with:

```ts
spanly.monitor(mcpServer, { injectSessionId: false });
```

### Trace context propagation

The SDK preserves the W3C `traceparent` value verbatim on every captured
packet: the HTTP header on HTTP transports, the
`params._meta.traceparent` field on stdio. Pick your APM provider in the
Spanly dashboard (Settings, Integrations) and every request with trace
context links straight to the matching trace in Datadog, Sentry or
New Relic.

Capture is automatic. No SDK configuration, no extra dependency, no
spans emitted into your APM.

## Links

- [spanly.com](https://spanly.com)
- [Documentation](https://spanly.com/docs/typescript-sdk/)
- [Spanly CLI (`@spanly/spanly`)](https://github.com/spanlyhq/spanly/tree/main/cli): `run` + `proxy` modes for any language.
- [Python SDK (`spanly`)](https://pypi.org/project/spanly/)
