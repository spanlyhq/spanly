# Spanly Helm chart

Standalone deployment of `spanly proxy` as its own Pod + Service in front
of an existing internal MCP server.

## Prerequisites

- Kubernetes 1.22+
- Helm 3.8+
- A Spanly API key stored as a Secret in the target namespace.

```bash
kubectl create secret generic spanly --from-literal=api-key=spanly_us_xxx
```

## Install

From a clone of [github.com/spanlyhq/spanly](https://github.com/spanlyhq/spanly):

```bash
git clone https://github.com/spanlyhq/spanly.git
helm install spanly ./spanly/charts/spanly \
  --set proxy.upstream=http://my-mcp-svc.default.svc:3000
```

Then point your MCP clients at `http://spanly.<ns>.svc:3001`.

## Common configuration

| Value                    | Default     | Notes                                                                     |
| ------------------------ | ----------- | ------------------------------------------------------------------------- |
| `proxy.upstream`         | _required_  | The MCP service to front.                                                 |
| `proxy.inspectPrefix`    | `/mcp,/sse` | Comma-separated path prefixes to inspect.                                 |
| `proxy.contextHeaders[]` | `[]`        | `HEADER=field` mappings (`environmentId`, `projectId`, `organisationId`). |
| `apiKey.secretName`      | `spanly`    | Secret holding the API key.                                               |
| `apiKey.secretKey`       | `api-key`   | Key within that secret.                                                   |
| `service.port`           | `3001`      | Port your MCP clients connect to.                                         |
| `admin.enabled`          | `true`      | Expose `/healthz`, `/readyz`, `/metrics`.                                 |
| `admin.port`             | `9090`      |                                                                           |
| `serviceMonitor.enabled` | `false`     | Prometheus Operator scrape config.                                        |
| `replicaCount`           | `1`         |                                                                           |
| `resources`              | small       | Override per workload.                                                    |

## Examples

### Multi-tenant header-based attribution

```bash
helm install spanly ./charts/spanly \
  --set proxy.upstream=http://mcp:3000 \
  --set "proxy.contextHeaders[0]=X-Tenant=environmentId"
```

### Custom ingest URL (local development)

```bash
helm install spanly ./charts/spanly \
  --set proxy.upstream=http://mcp:3000 \
  --set ingestURL=http://my-ingest.internal:3002
```

### With Prometheus Operator

```bash
helm install spanly ./charts/spanly \
  --set proxy.upstream=http://mcp:3000 \
  --set serviceMonitor.enabled=true
```

## Sidecar pattern (advanced)

To run spanly as a sidecar container in the same Pod as your app instead
of a separate Deployment, see `kustomize/spanly-sidecar/` for a
strategic-merge patch that injects the spanly container into an existing
Deployment.

## Uninstall

```bash
helm uninstall spanly
```
