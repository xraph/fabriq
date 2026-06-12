# ADR 0005 — Relay wake-up: grove Listener for LISTEN/NOTIFY

**Status:** accepted · 2026-06-12 · revised 2026-06-12 (direct-pgx workaround
replaced by grove's fixed Listener)

The outbox relay polls with `FOR UPDATE SKIP LOCKED` and is woken by
`pg_notify('fabriq_outbox', id)` issued inside the command transaction
(delivery on commit, so the wake-up can never outrun the data).

## History

Receiving asynchronous notifications requires a connection parked in
`WaitForNotification`. grove's `pgdriver.Listener` originally had a
connection-ordering race (`Start` parked the conn in `WaitForNotification`;
`Listen`'s subsequent `Exec` on the same non-concurrency-safe pgx conn
failed), so `adapters/postgres/listen.go` opened ONE direct `pgx.Conn` —
fabriq's sole exception to the *"no direct pgx"* rule, documented here.

## Current state

grove fixed the race: `Listener.Listen`/`Unlisten` queue LISTEN/UNLISTEN
commands, and the listen goroutine — which exclusively owns the dedicated
pool connection — executes them between `WaitForNotification` calls (woken
via context cancel, which pgconn treats as a non-fatal deadline interrupt).

`listen.go` now uses the supported flow,
`db.Listen(ctx, channel, handler)`, and fabriq has no direct pgx usage.
Two behaviors stay in fabriq's wrapper:

- **Reconnect/backoff.** grove's Listener exits its loop on fatal
  connection errors and does not auto-reconnect (a stopped Listener cannot
  be restarted), so `notifyLoop` rebuilds a fresh Listener with 1s backoff
  forever.
- **Liveness probe.** The Listener exposes no death signal; `listenOnce`
  re-issues `Listen` every 15s — idempotent per session, fails fast with
  "listener stopped" once the goroutine has exited — to detect a dead
  listener and trigger the rebuild.

The Listener parks one connection from the adapter's pool (default size
16) for the relay's lifetime; the previous workaround held an extra
out-of-pool connection instead.

The wake-up remains an optimization only: the interval poll is the
correctness mechanism, so a broken listener degrades latency, never
delivery.
