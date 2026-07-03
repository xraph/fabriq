package document

// Materialization — the bridge from the CRDT log back into the fabric —
// is IMPLEMENTED in the postgres adapter (DocStore.MaterializeQuiet in
// adapters/postgres/document.go) and driven by the forgeext worker's
// leader-elected document-plane loop. Contract:
//
//   - After CRDTSpec.QuietWindow of silence on a document with updates
//     beyond the last materialization, merge the log (grove engine, the
//     shared FoldChange), project values (ProjectValues), run post-merge
//     validation (registry.CoerceRow + the optional ValidateFunc), and —
//     only on success — write the entity row plus exactly ONE ordinary
//     <entity>.updated event (version+1) through the transactional
//     outbox, with the materialization watermark in the same tx (a crash
//     can never re-emit).
//   - Validation failures flag the document for resolution (flag_reason
//     recorded); flagged documents are skipped by both sweeps until an
//     operator intervenes.
//
// Behavior is pinned by TestDocMaterialize_* (adapters/postgres) and the
// worker integration suite; see DESIGN.md for the full plane contract.
