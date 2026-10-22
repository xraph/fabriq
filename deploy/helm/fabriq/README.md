# fabriq Helm chart

This chart deploys the fabriq worker: the outbox relay, the graph and search
projection consumers, the reconciler and the CRDT document materializer. It
also renders a schema-migration hook that runs under an advisory lock. The
datastores are external. You point the chart at managed Postgres with
Timescale, Redis, and optionally FalkorDB and Elasticsearch. It bundles no
databases.

The worker is `fabriq serve`, one binary that also provides the operator
commands (`migrate`, `rebuild`, `reconcile`, `inspect`). The chart's migration
Job runs `fabriq migrate up` from the same image. `api-example` is a demo HTTP
service and is not part of this chart.

## Prerequisites

You need a container image. Releases publish `ghcr.io/xraph/fabriq` for
`linux/amd64` and `linux/arm64`, so most of the time you only pick a tag. To
build your own from the repo root (the build is hermetic and vendors the local
grove `replace` directives):

```bash
make docker-build IMAGE=ghcr.io/xraph/fabriq TAG=1.9.0
make docker-push  IMAGE=ghcr.io/xraph/fabriq TAG=1.9.0
```

You also need external datastores the cluster can reach: Postgres (required),
Redis (required for projections and live subscriptions), and optionally
FalkorDB and Elasticsearch.

## Install

For production, create a Secret that holds the full Postgres DSN and let the
chart read it. Redis, FalkorDB and Elasticsearch addresses are plain
`host:port` values, so they go in as values:

```bash
kubectl create secret generic fabriq-pg \
  --from-literal=FABRIQ_POSTGRES_DSN='postgres://fabriq_app:***@pg:5432/fabriq?sslmode=require'

helm install fabriq deploy/helm/fabriq \
  --set image.repository=ghcr.io/xraph/fabriq --set image.tag=1.9.0 \
  --set postgres.existingSecret=fabriq-pg \
  --set postgres.dsnKey=FABRIQ_POSTGRES_DSN \
  --set redis.addr=redis:6379 \
  --set projections.graph=true --set falkordb.addr=falkordb:6379 \
  --set projections.search=true --set elasticsearch.addrs=https://es:9200
```

The `projections.*` switches are optional. Turning one on makes the chart fail
the install when that engine's address is missing, which catches the
"I meant to wire search" mistake before it reaches the cluster. The worker
starts a projection whenever its address is present, switch or not.

For dev and CI, let the chart create the Secret from a literal DSN:

```bash
helm install fabriq deploy/helm/fabriq \
  --set postgres.dsn='postgres://app:app@pg:5432/fabriq?sslmode=disable' \
  --set redis.addr=redis:6379
```

You can also compose the DSN from parts with `postgres.host`, `postgres.user`,
`postgres.database` and friends, and pull the password from a Secret with
`postgres.passwordSecret`. Pick one of the three styles. They are not meant to
be mixed.

`helm upgrade` re-runs the migration hook before the worker rolls. The hook is
advisory-locked, so it does nothing when the schema is already current.

## What the chart does not do

The chart deploys the worker. It does not mount the admin API, and it has no
value that injects extra Secret-backed environment variables. That matters if
you want the hosted server described in the Deployment page of the docs
site, because `FABRIQ_ADMIN_API=true` needs a `FABRIQ_ADMIN_KEY`, and the engine
passwords have variables of their own.

The plain settings work today. Put them in `config.extraEnv`, which lands in
the worker's ConfigMap:

```yaml
config:
  extraEnv:
    FABRIQ_DOMAIN_PACK: none
    FABRIQ_ADMIN_API: "true"
```

Don't put the admin key or any password there. A ConfigMap is readable by
anyone who can read the namespace. Until the chart grows a Secret-backed env
hook, run the hosted server from the image directly (a Deployment you write
yourself, or `helm template` output you patch) with the key and the engine
credentials coming from a Secret you own.

Two more things to know. The migration Job and the worker share one Postgres
DSN, so the Job connects as whatever role the worker uses. A hosted server
wants a privileged role for `migrate up` and the `fabriq_app` role for `serve`.
Set `migrate.enabled=false`, run `migrate preflight`, `migrate up` and
`migrate app-role` as your own Jobs with the privileged DSN, and give the chart
the `fabriq_app` DSN. Also, `config.extraEnv` reaches the worker only. The
migration Job doesn't read the ConfigMap.

## Why there is no Kubernetes leader election or RBAC

The worker's singletons (relay, reconciler, document materializer) elect
through Postgres advisory locks (keys 1001, 1002 and 1003) on dedicated
connections, so leadership follows database connectivity and not a Kubernetes
Lease. Scale `worker.replicaCount` freely: exactly one replica holds each
singleton, and the projection consumers share Redis consumer groups and all
stay active. The ServiceAccount needs no RBAC, and token automount is off.

## Operations

- Health and metrics live on `:8081` (`service.port`): `/_/livez`, `/_/readyz`
  and two Prometheus endpoints. The chart wires the probes to `/_/livez` and
  `/_/readyz`. Readiness returns 503 until the facade is open and Postgres
  answers, so a pod that can't reach its database stays out of rotation.
  `/_/health` is forge's own report, and it says ready during a 60 second
  grace window, so don't probe it.
- `/metrics` serves fabriq's own `fabriq_*` series. `/_/metrics` is forge's:
  HTTP server metrics, plus grove's `db_client_operation_duration_seconds` for
  every database fabriq dials. The scrape annotations are on by default and
  cover `/metrics` only, because the annotation convention names one path. Set
  `metrics.serviceMonitor.enabled=true` for a prometheus-operator
  `ServiceMonitor`, which scrapes both while `metrics.forgeEndpoint` is `true`.
- A `PodDisruptionBudget` keeps `minAvailable: 1`, so the relay leader fails
  over cleanly during node drains. `terminationGracePeriodSeconds: 30` covers
  the worker's SIGTERM drain.
- `autoscaling.enabled=true` gives you a CPU HPA. For lag-driven scaling of
  the projection consumers, use KEDA on Redis stream lag or on the
  `fabriq_projection_lag_events` metric. Projection throughput is bound by the
  engines, not by CPU.
- `rebuild` and `reconcile` are operator commands in the same image:
  `kubectl run --rm -it --image=<img> --restart=Never op -- fabriq rebuild --tenant T --projection graph`
  (or run them as Jobs).

## Key values

| Key | Default | Notes |
|-----|---------|-------|
| `image.repository`, `image.tag` | `ghcr.io/xraph/fabriq`, chart appVersion | An empty tag means the chart's `appVersion`. |
| `postgres.existingSecret` | `""` | A Secret you manage. Recommended. |
| `postgres.dsnKey` | `FABRIQ_POSTGRES_DSN` | The key in that Secret that holds the full DSN. |
| `postgres.dsn` | `""` | A literal DSN for dev and CI. The chart keeps it in its own Secret. |
| `postgres.host`, `postgres.port`, `postgres.database`, `postgres.user` | `""`, `5432`, `""`, `""` | Compose the DSN from parts. |
| `postgres.sslmode`, `postgres.params` | `require`, `""` | `params` is appended after `sslmode`. |
| `postgres.password` | `""` | Inline password for dev and CI. |
| `postgres.passwordSecret`, `postgres.passwordKey` | `""`, `password` | Pull the password from an existing Secret. Keep it URL-safe. |
| `redis.existingSecret`, `redis.addrKey`, `redis.addr` | `""`, `FABRIQ_REDIS_ADDR`, `""` | Required for projections and subscriptions. |
| `falkordb.existingSecret`, `falkordb.addrKey`, `falkordb.addr` | `""`, `FABRIQ_FALKORDB_ADDR`, `""` | Set an address to enable the graph projection. |
| `elasticsearch.existingSecret`, `elasticsearch.addrsKey`, `elasticsearch.addrs` | `""`, `FABRIQ_ELASTICSEARCH_ADDRS`, `""` | Comma-separated. Set it to enable the search projection. |
| `projections.graph`, `projections.search` | `false` | Fail the install if the engine's address is missing. |
| `config.httpAddr` | `:8081` | Bind address for probes and metrics. |
| `config.reconcileInterval` | `""` (5m) | `"0"` disables the scheduled reconciler. |
| `config.extraEnv` | `{}` | Plain, non-secret env for the worker. |
| `worker.replicaCount` | `2` | Safe to raise, because leadership is advisory-lock based. |
| `migrate.enabled` | `true` | The pre-install and pre-upgrade migration hook. |
| `pdb.enabled`, `pdb.minAvailable` | `true`, `1` | |
| `autoscaling.enabled` | `false` | CPU HPA. |
| `metrics.serviceMonitor.enabled` | `false` | prometheus-operator. |
| `metrics.forgeEndpoint` | `true` | The ServiceMonitor also scrapes `/_/metrics`. |
| `networkPolicy.enabled` | `false` | Ingress to the http port and DNS egress. You supply datastore peers in `networkPolicy.egress`. |

See [`values.yaml`](values.yaml) for the full set.
