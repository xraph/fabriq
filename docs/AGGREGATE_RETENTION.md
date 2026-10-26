# Aggregate retention

Use `Fabriq.EraseAggregate` when an aggregate has reached its retention limit. You must configure `WithErasureAuthorizer` and run migration `202610260036` first. The default executor has no erasure authority.

Your authorizer resolves the caller and current deletion grant from trusted context. It holds that grant through the synchronous callback and supplies the audit actor and authority revision. It must authorize the exact tenant, entity, aggregate ID and scope, including the empty scope. An ordinary command grant does not grant retention access.

Pass an `ErasureRequest` with the current active row version and a bounded reason code such as `expired`. Use version zero only when you expect the active row to be absent. A correction that commits while erasure waits changes the version, so the old request fails with a conflict. Reload the current state and obtain fresh authority before retrying with a different request.

The Postgres transaction removes four named local payload targets:

- The registered aggregate row.
- Every retained command outbox payload and trace context for that identity.
- Its local embedding and metadata, if the vector table is installed.
- Its local geometry and metadata, if the spatial table is installed.

The receipt records those targets in `LocalTargets`. `LocalPayloadsErased` applies only to that list. Historical outbox envelope identifiers, versions and publication metadata remain for audit, with an empty payload and an erasure timestamp. The tombstone retains the request, audit identity and receipt. It contains no aggregate payload.

Erasure checks stored scopes before deleting anything. A hidden row or historical payload in another scope causes a generic conflict and rolls back the transaction. The implementation temporarily uses tenant-wide visibility inside this transaction to detect those conflicts; deletion predicates still require the exact authorized scope. Empty historical delete events contain no payload and can be retired with the aggregate's other events.

Commands, vector writes and spatial writes share an aggregate identity lock with erasure. Once the transaction commits, attempts to recreate or update that tenant/entity/ID fail with `ErrAggregateErased`, even if the caller chooses a different scope. Retrying the same authorized erasure returns its original receipt. Updating or deleting the tombstone through normal application SQL is rejected. The migration refuses rollback because removing the tombstones would permit identity reuse.

The relay excludes erased outbox rows. An erasure transaction waits for a relay transaction already publishing a selected row to finish, so an in-flight publication can precede the erasure receipt. Copies already delivered to streams, graph, search, caches, live subscribers or backups remain `unconfirmed` in `Downstream`. You need separate destination-specific cleanup and evidence for those copies. Custom hook tables, blob storage and document/CRDT payloads are outside the named local targets; this API accepts `KindAggregate` only.

This is logical removal from the active database. It does not attest physical disk overwriting, WAL expiry, replica convergence or backup deletion. Keep those retention policies separate. A successful receipt must never be presented as confirmation that every copy across the fabric has been erased.

The installed adapter regression runs against a disposable Postgres instance with the non-superuser app role:

```sh
GOWORK=off go test -race -tags=integration ./adapters/postgres \
  -run '^TestAggregateRetentionPostgres$' -count=1 -timeout=5m
```

It exercises local payload deletion, restart receipts, identity fences, exact scope denial, rollback, concurrent version changes and relay coordination. Cortex can use this optional capability after Fabriq is released and its dependency is pinned. An unreleased checkout does not change the behavior of an installed Cortex adapter.
