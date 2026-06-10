# Spanly sidecar: Kustomize component

Inject the `spanly` proxy as a sidecar container into an existing
Deployment using Kustomize.

## When to use this vs the Helm chart

| Pattern | When |
|---|---|
| **Helm chart (`charts/spanly/`)** | Deploy spanly as its own Pod + Service in front of an existing internal MCP service. |
| **This Kustomize component** | Deploy spanly as a sidecar container co-located in the same Pod as the MCP server. |

## Usage

1. **Create the API key secret** in your namespace:

   ```bash
   kubectl create secret generic spanly --from-literal=api-key=spanly_us_xxx
   ```

2. **Label your Deployment** so the patch knows where to apply:

   ```yaml
   # my-deployment.yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: my-mcp-server
     labels:
       spanly-sidecar: "true"   # <-- this label
   spec: ...
   ```

3. **Reference the component** from your kustomization:

   ```yaml
   # kustomization.yaml
   apiVersion: kustomize.config.k8s.io/v1beta1
   kind: Kustomization
   resources:
     - my-deployment.yaml
   components:
     - ../spanly-sidecar          # path to this directory
   ```

4. **Override defaults** if needed via additional patches in your
   kustomization (e.g. change the upstream port from `localhost:3000`,
   or the bind port from `:3001`).

## Defaults baked into `patch.yaml`

| Setting | Default | How to override |
|---|---|---|
| Image | `spanly/spanly:latest` | `images:` field in your kustomization |
| Upstream | `localhost:3000` | Patch `args` |
| Bind | `:3001` | Patch `args` |
| Inspect prefix | `/mcp,/sse` | Patch `args` |
| Secret name | `spanly` | Patch `env` |
| Admin port | `9090` | Patch `args` and `ports` |

## What this does

After applying:

- Your Pod now runs your app **and** a spanly container.
- Clients connecting to **port 3001** of the Pod get the proxied + observed
  experience; the upstream MCP server (port 3000 in your container) is no
  longer directly exposed by your Service.
- The admin port (9090) exposes `/healthz`, `/readyz`, and `/metrics`.

Update your Service to target `3001` instead of your app port:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-mcp-server
spec:
  selector: { app: my-mcp-server }
  ports:
    - port: 3001
      targetPort: 3001
```

## Verifying

```bash
kubectl logs -l spanly-sidecar=true -c spanly
kubectl port-forward deploy/my-mcp-server 9090:9090
curl localhost:9090/metrics
```
