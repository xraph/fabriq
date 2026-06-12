# CRDT Document Plane — Design (Phase 7)

Status: **implemented** (2026-06-12): the Postgres document store (adapters/postgres/document.go) folds the append-only update log through grove's MergeField engine; Sync exchanges seq-based vectors (snapshot + tail after compaction); quiet-window materialization emits ONE ordinary <entity>.updated event (version++) through the outbox with post-merge validation flagging; compaction is storage-only. Doc ids are "<entity>/<ulid>" — the registry binds the relational shape. Remaining: the live WS/SSE sync endpoint riding Hub.PublishRaw (the seam exists; clients can poll Sync today), and grove crdt-js client wiring.

## What this plane is for

`KindDocument` entities are collaborative documents — page-builder
documents, annotations — where concurrent editing is the norm and
last-write-wins rows would destroy work. They are **not** written through
the command plane (`Exec` rejects them); they converge through CRDT merges
and only *materialize* into ordinary rows.

## Storage

- `fabriq_crdt_updates(doc_id, seq, tenant_id, update_data, at)` —
  append-only update log, per-document monotonic `seq`. RLS applies.
- `fabriq_crdt_snapshots(doc_id, tenant_id, snapshot, last_seq, at)` —
  compacted state up to `last_seq`. RLS applies.
- The merge engine comes from grove's `crdt` / `crdt-js` packages
  (HLC-stamped field-level merge, OR-sets, RGA lists). **Referenced, never
  reimplemented here.**

## Sync transport

Bidirectional sync rides the subscription hub's connection layer with
**no conflation and no update coalescing** — `Hub.PublishRaw` is the seam
built in phase 1. CRDT frames must arrive complete and in order; the
conflating delta path and the document sync path share connections, never
semantics. Client → server: `ApplyUpdate`; server → client: `Sync(state
vector) -> missing updates` (snapshot + tail after compaction).

## Materialization (the bridge back into the fabric)

After `CRDTSpec.QuietWindow` of silence on a document:

1. Merge the log (grove crdt engine).
2. **Post-merge validation** — CRDTs converge but don't guarantee business
   validity. Violations flag the document for resolution; nothing
   materializes.
3. Write the merged state into the entity's relational row and emit
   **exactly ONE ordinary domain event** (`<entity>.updated`, `version+1`)
   through the transactional outbox.

Downstream (graph, search, audit, subscriptions) therefore sees CRDT
documents as perfectly normal entities. `Materialized.Version` only
advances at materialization, not per update.

## Compaction

Every `CRDTSpec.SnapshotEvery` updates, `Compact` folds the log into the
snapshot and trims `seq <= last_seq` in one transaction (the fabriq-worker
compactor job). Compaction changes storage shape, never merge results.

## Awareness / presence

Cursors and who's-online ride Redis pub/sub (`adapters/redis/pubsub.go`,
already implemented): ephemeral, never persisted, no delivery guarantees.

## Test plan (sketched in document_test.go)

Append/seq ordering, state-vector sync, quiet-window → one event,
post-merge validation flags, compaction transparency, raw-path ordering.
