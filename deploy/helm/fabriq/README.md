# fabriq Helm chart

Deploys the **fabriq worker** — the data fabric's background plane (outbox
relay, graph/search projection consumers, reconciler, CRDT document
materializer) — plus an **advisory-locked schema-migration hook**.
Datastores are **external**: you point the chart at managed
Postgres+Timescale, Redis, and optionally FalkorDB + Elasticsearch. The
chart bundles no databases.

> The worker is `cmd/fabriq serve` — one binary that also provides the
> operator commands (`migrate`, `rebuild`, `reconcile`, `inspect`). The
> chart's migration Job runs `fabriq migrate up` from the same image.
> `api-example` is a demo HTTP service and is intentionally **not** part of
> this chart.

## Prerequisites

- A container image. Build it from the repo root (hermetic; vendors the
  local grove `replace` directives):

  ```bash
  make docker-build IMAGE=ghcr.io/xraph/fabriq TAG=0.1.0
  make docker-push  IMAGE=ghcr.io/xraph/fabriq TAG=0.1.0
  ```

- External datastores reachable from the cluster: Postgres (required),
  Redis (required for projections + live subscriptions), and optionally
  FalkorDB and/or Elasticsearch.

## Install

Production — reference a pre-created Secret holding the `FABRIQ_*`
connection keys:

```bash
kubectl create secret generic fabriq-conn \
  --from-literal=FABRIQ_POSTGRES_DSN='postgres://app:***@pg:5432/fabriq?sslmode=require' \
  --from-literal=FABRIQ_REDIS_ADDR='redis:6379' \
  --from-literal=FABRIQ_FALKORDB_ADDR='falkordb:6379' \
  --from-literal=FABRIQ_ELASTICSEARCH_ADDRS='https://es:9200'

helm install fabriq deploy/helm/fabriq \
  --set image.repository=ghcr.io/xraph/fabriq --set image.tag=0.1.0 \
  --set secret.existingSecret=fabriq-conn
```

Dev/CI — let the chart create the Secret from values:

```bash
helm install fabriq deploy/helm/fabriq \
  --set secret.postgresDSN='postgres://app:app@pg:5432/fabriq?sslmode=disable' \
  --set secret.redisAddr='redis:6379'
```

`helm upgrade` re-runs the migration hook (advisory-locked, so it's a
no-op when the schema is current) before the worker rolls.

## Why no Kubernetes leader election / RBAC

The worker's singletons — relay, reconciler, document materializer — elect
through **Postgres advisory locks** (keys 1001/1002/1003) on dedicated
connections, so leadership tracks database connectivity rather than a
Kubernetes Lease. Scale `worker.replicaCount` freely: exactly one replica
holds each singleton; projection consumers share Redis consumer groups and
all stay active. The ServiceAccount needs no RBAC (token automount is
off).

## Operations

- **Health/metrics** on `:8081` (`service.port`): `/_/livez`, `/_/readyz`,
  `/_/health`, and `/metrics` (Prometheus). Scrape annotations are on by
  default; set `metrics.serviceMonitor.enabled=true` for a
  prometheus-operator `ServiceMonitor`.
- **Disruption**: a `PodDisruptionBudget` keeps `minAvailable: 1` so the
  relay leader fails over cleanly during node drains;
  `terminationGracePeriodSeconds: 30` covers the worker's SIGTERM drain.
- **Scaling**: `autoscaling.enabled=true` gives a CPU HPA. For lag-driven
  scaling of projection consumers, prefer **KEDA** on Redis stream lag or
  the `fabriq_projection_lag_events` metric — projection throughput is
  I/O-bound on the engines, not CPU.
- **Rebuild / reconcile** are operator commands in the same image:
  `kubectl run --rm -it --image=<img> --restart=Never op -- fabriq rebuild --tenant T --projection graph`
  (or run them as Jobs).

## Key values

| Key | Default | Notes |
|-----|---------|-------|
| `image.repository` / `image.tag` | `ghcr.io/xraph/fabriq` / chart appVersion | |
| `secret.existingSecret` | `""` | Use a pre-created Secret (recommended). |
| `secret.postgresDSN` | `""` | Required when not using `existingSecret`. |
| `secret.redisAddr` | `""` | Required for projections/subscriptions. |
| `secret.falkordbAddr` | `""` | Set to enable the graph projection. |
| `secret.elasticsearchAddrs` | `""` | Set to enable the search projection. |
| `config.reconcileInterval` | `""` (5m) | `"0"` disables the scheduled reconciler. |
| `worker.replicaCount` | `2` | Safe to raise — advisory-lock leadership. |
| `migrate.enabled` | `true` | pre-install/pre-upgrade migration hook. |
| `pdb.enabled` / `pdb.minAvailable` | `true` / `1` | |
| `autoscaling.enabled` | `false` | CPU HPA. |
| `metrics.serviceMonitor.enabled` | `false` | prometheus-operator. |
| `networkPolicy.enabled` | `false` | Egress to your datastores. |

See [`values.yaml`](values.yaml) for the full set.
