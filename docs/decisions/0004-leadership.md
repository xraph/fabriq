# ADR 0004 — Advisory-lock leadership on grove dedicated connections

**Status:** accepted · 2026-06-12

Singleton runners (outbox relay, reconciler) need exactly-one-active
semantics across fabriq-worker replicas. Session-level `pg_advisory_lock`
is the spec'd mechanism, and it is safe here because grove's pgdriver
implements `driver.ConnAcquirer`: the elector takes a **dedicated pooled
connection**, acquires `pg_try_advisory_lock` on it, and every later
operation on that session stays on that connection (the same mechanism
grove's own migration lock uses).

Failure handling: the lock dies with the session, so the elector runs a
liveness watchdog (`SELECT 1` every heartbeat on the dedicated conn) and
abdicates — cancelling the leader's context — the moment the session is
gone. A replica then wins the next campaign. Split-brain is excluded by
Postgres lock semantics; the watchdog only bounds how long a dead leader
keeps *running* after losing the lock.

The elector lives in `adapters/postgres` (not the spec's
`internal/leader`) because depguard fences grove driver imports to
`adapters/`; the package boundary moved, the contract did not. Lock keys:
1001 relay, 1002 reconciler — stable, never reused.
