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

## Rebuild (blue-green, phase 4)

`fabriq rebuild --tenant T --projection graph|search`

Builds `tenant_T_v{N+1}` (or the versioned index) from a Postgres
snapshot watermark, catches up live events, flips the pointer in
`fabriq_projection_state` (ES: atomic alias swap), soaks, then drops the
old target. Reads keep working throughout — they follow the
pointer/alias. Reindex source is ALWAYS Postgres, never the old
projection.

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

## Reconciler (phase 6)

`fabriq reconcile --tenant T [--repair]` compares counts + max versions
between Postgres and each projection and (with `--repair`) re-emits
synthetic events for drifted aggregates through the normal pipeline. The
scheduled job runs leader-elected (lock 1002); alert on its drift gauge
rather than trusting silence.
