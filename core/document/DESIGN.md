# CRDT Document Plane — Design (Phase 7)

Status: **implemented, full CRDT surface** (2026-07-02): the Postgres
document store (adapters/postgres/document.go) folds the append-only
update log through grove's canonical `crdt.ApplyChange` — every type
merges losslessly (LWW, PN-counter, OR-set, RGA list, nested document,
and the character-level **text** CRDT with formatting). Sync exchanges
seq-based vectors (snapshot + tail after compaction); quiet-window
materialization emits ONE ordinary <entity>.updated event (version++)
through the outbox with post-merge validation flagging (projections:
counter totals, set/list element arrays, text strings —
document.ProjectValues). The worker's leader-elected document-plane loop
drives BOTH MaterializeQuiet and CompactDue (SnapshotEvery budget,
physical row counts); Compact GCs tombstones behind a 1h horizon (text
skeletonizes — addresses preserved). Live sync is first-class at the
gateway: POST docs/update|sync + SSE docs/subscribe (RAW frames via
syncingDocStore fan-out → Hub.SubscribeRaw), with ephemeral presence
(docpresence channel, capped stream, never persisted). fabriqtest's
FakeDocumentStore implements the same contract (core/document contract
suite). grove crdt-js mirrors the full type surface incl. text
(golden-fixture cross-engine parity). Doc ids are "<entity>/<ulid>" —
the registry binds the relational shape. Remaining: WS multiplexing of
doc frames on the live WS controller (SSE + HTTP cover sync today);
secondary-scope support (tracked separately).

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
