# Operations Runbooks

Metrics referenced here are defined in `internal/metrics` and exposed by
the binaries at `/_/metrics` (forge).

## Outbox backlog grows (`fabriq_outbox_backlog`)

Cause: the relay isn't draining — worker down, no leader, or Redis
unreachable.

1. `kubectl get pods` / check fabriq-worker replicas are running
   (`/_/readyz`).
2. Exactly one relay leads (advisory lock 1001). Check worker logs for
   election churn; a flapping Postgres connection abdicates leadership
   (session watchdog) by design.
3. Check Redis availability from the worker. The relay is at-least-once:
   once Redis returns, the backlog drains in ULID order; downstream is
   version-gated idempotent, so duplicates are harmless.
4. Commands never fail because the relay is down (the outbox IS the
   buffer); this is a latency incident, not a data-loss incident.

## Projection lag (`fabriq_projection_lag_events`)

1. Identify the lagging group: `XINFO GROUPS fabriq:events`.
2. Consumers scale by replica count (consumer groups, no election) — add
   replicas if apply throughput is the limit.
3. A poisoned event (handler error loop) stays pending and is XAUTOCLAIMed
   between consumers: look for the same stream id cycling in logs.
   Because appliers are pure and version-gated, the usual cause is an
   engine outage, not the event itself.
4. If a projection fell off the stream's MAXLEN horizon: rebuild from
   Postgres (below) — that is always safe and always possible.

## Rebuild (blue-green)

`fabriq rebuild --tenant T --projection graph|search [--drop-old]`

Marks `fabriq_projection_state` status=building (the live engine starts
dual-applying immediately), replays the Postgres snapshot into
`tenant_T_v{N+1}` (graph) or the `_v{N+1}` indexes (search), then flips:
model_version++, target pointer moves, ES aliases swap atomically in one
`_aliases` call. Readers follow the pointer/alias (graph reads re-resolve
within the 2s resolver TTL). After the soak window run
`fabriq rebuild finalize --tenant T --old <target>` to drop the old
target — or pass `--drop-old` to skip the soak. Reindex source is ALWAYS
Postgres, never the old projection; the e2e suite proves the rebuilt
graph is identical to the event-built one.

## Tenant hook trips (`fabriq_tenant_hook_trips_total`)

**Any non-zero value is a fabriq bug — page the owning team.** It means a
query reached an engine without tenant scoping: pool-path access to a
tenant table, or raw SQL touching the readings table without a tenant
predicate. RLS contained the blast radius (stamped transactions only see
their tenant; unstamped see nothing), but the call site must be found and
fixed. The error carries the table and operation.

## Conflation depth grows (`fabriq_conflation_depth`)

Subscribers aren't draining. Hub delivery is non-blocking (full buffers
drop; clients refetch + resume via Last-Event-ID), so this self-heals at
the cost of client refetches — investigate slow SSE consumers or raise
`WithSubscribeBuffer`.

## SSE behind proxies

The bridge sets `X-Accel-Buffering: no`, flushes after every event and
heartbeats every 15s. If clients see batched events, a proxy is buffering:
check nginx (`proxy_buffering off` honored via the header), ALB idle
timeout > heartbeat interval.

## Reconciler

`fabriq reconcile --tenant T [--repair] [--falkordb ...] [--elasticsearch ...]`

Compares per-aggregate versions between Postgres and each projection:

- **missing/stale** (projection behind the row): repairs by upserting the
  aggregate's latest event into the outbox with `published_at = NULL` —
  the relay republishes, version-gated consumers converge.
- **zombie** (projected but no row): emits a synthetic
  `<entity>.deleted` one version past what the projection holds.
- A projection AHEAD of the truth scan is not drift (events land between
  the two reads).

Reconciliation never writes an engine directly. In `fabriq-worker` it
runs leader-elected (lock 1002) every `FABRIQ_RECONCILE_INTERVAL`
(default 5m, `0` disables), across every tenant seen in the outbox.
Caveat: the ES projection scan is capped at 10k docs per entity per
tenant; beyond that, scroll support is needed before trusting zombie
detection there.
