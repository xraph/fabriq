package document

// PHASE 7 SCAFFOLD — compaction.
//
// Contract (normative; see DESIGN.md):
//
//   - Every CRDTSpec.SnapshotEvery updates (or on a size budget), Compact
//     folds fabriq_crdt_updates(doc) into fabriq_crdt_snapshots(doc) at
//     last_seq and deletes log rows with seq <= last_seq, inside one
//     transaction.
//   - Sync() then serves (snapshot + tail) instead of the full log, which
//     bounds reconnect cost for long-lived documents.
//   - Compaction is the fabriq-worker compactor job; it never changes
//     merge results, only their storage shape.
//
// TODO(phase 7): Compactor over the postgres document store.
