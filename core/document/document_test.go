package document_test

// PHASE 7 SCAFFOLD — the document plane's behavior tests land with the
// implementation. They are sketched here so the contract is executable
// review material:
//
// TODO(phase 7): TestApplyUpdate_AppendsToLog — ApplyUpdate writes one
// crdt_updates row with the next per-doc seq; concurrent appenders never
// reuse a seq.
//
// TODO(phase 7): TestSync_ReturnsMissingUpdates — Sync(stateVector)
// returns exactly the updates the client lacks (grove crdt diff), as
// (snapshot + tail) after compaction.
//
// TODO(phase 7): TestQuietWindowMaterializes_OneDomainEvent — after
// QuietWindow of silence, exactly ONE <entity>.updated event lands in the
// outbox with version+1, and the relational row equals the merged state.
//
// TODO(phase 7): TestMaterialize_PostMergeValidationFlags — a merged
// state violating business rules does NOT materialize; the document is
// flagged for resolution and no event is emitted.
//
// TODO(phase 7): TestCompact_FoldsLogIntoSnapshot — Compact moves seq <=
// last_seq into the snapshot and Sync output is unchanged before/after.
//
// TODO(phase 7): TestSyncBypassesConflation — document sync frames ride
// Hub.PublishRaw; a burst of updates is delivered complete and in order
// (contrast: delta channels conflate).
