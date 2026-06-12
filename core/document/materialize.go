package document

// PHASE 7 SCAFFOLD — materialization.
//
// Contract (normative; see DESIGN.md):
//
//   - After a document's quiet window (CRDTSpec.QuietWindow) with no new
//     updates, the materializer merges the update log (grove's crdt
//     engine — referenced, never reimplemented), runs POST-MERGE
//     VALIDATION (CRDTs converge but do not guarantee business validity;
//     violations flag the document for resolution instead of
//     materializing), writes the snapshot into the entity's relational
//     row, and emits ONE ordinary domain event (<entity>.updated,
//     version++) through the transactional outbox.
//   - Projections, search and audit therefore see CRDT documents as
//     perfectly normal entities; nothing downstream knows the row was
//     CRDT-merged.
//   - The materializer is a fabriq-worker job (leader-elected per shard).
//
// TODO(phase 7): Materializer{Store, command.Store, validator hook} +
// quiet-window scheduling driven by crdt_updates arrival times.
