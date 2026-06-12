# ADR 0005 — Relay wake-up: direct pgx for LISTEN/NOTIFY (the one grove exception)

**Status:** accepted · 2026-06-12

The outbox relay polls with `FOR UPDATE SKIP LOCKED` and is woken by
`pg_notify('fabriq_outbox', id)` issued inside the command transaction
(delivery on commit, so the wake-up can never outrun the data).

Receiving asynchronous notifications requires a connection parked in
`WaitForNotification` — something grove cannot express today:

- `driver.DedicatedConn` exposes Exec/Query/QueryRow only;
- grove's `pgdriver.Listener` has a connection-ordering race
  (`Start` parks the conn in `WaitForNotification`; `Listen`'s subsequent
  `Exec` on the same non-concurrency-safe pgx conn fails), has no tests
  and no in-repo usage. An upstream fix has been proposed.

Per the project rule — *"do not use pgx directly except where grove's
driver genuinely cannot express something, document any such case"* —
`adapters/postgres/listen.go` opens ONE direct `pgx.Conn` for
LISTEN + WaitForNotification, with reconnect/backoff. It is an
optimization only: the interval poll remains the correctness mechanism,
so a broken listener degrades latency, never delivery. When grove's
Listener is fixed, the swap is contained to that one file.
