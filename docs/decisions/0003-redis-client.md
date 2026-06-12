# ADR 0003 — go-redis directly in adapters/redis (not grove's kv driver)

**Status:** accepted · 2026-06-12

Grove ships a kv abstraction with a Redis driver, including a thin stream
surface (XAdd/XRead/XReadGroup/XAck). Fabriq's fan-out plane needs more
than it exposes:

- `MAXLEN ~` approximate trimming on XADD (channel cap and event-stream
  cap are part of the catch-up contract);
- `XAUTOCLAIM` for crashed-consumer recovery in projection groups;
- pipelined multi-stream publishes (event stream + N change channels per
  envelope);
- `XRANGE (exclusive` reads for Last-Event-ID resume.

The spec explicitly allows `go-redis/v9`, and depguard fences it to
`adapters/` — no other package can import it. The cache and presence
surfaces use the same client for one connection pool. If grove's kv
stream API grows these capabilities, swapping is contained to
`adapters/redis`.
