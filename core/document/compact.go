package document

// Compaction contract (normative; see DESIGN.md). Implemented by the
// Postgres document store (adapters/postgres DocStore.Compact/CompactDue):
//
//   - Compact folds fabriq_crdt_updates(doc) into fabriq_crdt_snapshots(doc)
//     at last_seq and deletes log rows with seq <= last_seq, inside one
//     transaction. With ArchiveHistory the trimmed range is first sealed
//     into an immutable blob segment so history survives outside the DB.
//   - Sync() then serves (snapshot + tail) instead of the full log, which
//     bounds reconnect cost for long-lived documents.
//   - Compaction is scheduled by the fabriq worker: the forgeext
//     document-plane loop runs CompactDue every tick, compacting each
//     unflagged doc whose un-compacted update count has reached its
//     entity's CRDTSpec.SnapshotEvery budget (<= 0 disables). It never
//     changes merge results, only their storage shape.
