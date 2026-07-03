# ADR 0010 — fs_node pure adjacency: drop the materialized path

**Status:** accepted · 2026-07-03. Amends the tree write model in ADR 0008.

## Context

`fs_nodes` carried both `parent_id` (adjacency) and a materialized `path`
column, kept transactionally consistent on move/rename by an in-tx command
hook that bulk-rewrote descendant path prefixes. The bulk `UPDATE` was
invisible to the event stream: descendants changed on disk with no per-row
events and no version bumps, so any event-driven consumer of those rows
silently missed moves. It also made move cost O(subtree) on the write path,
for a table whose whole purpose is cheap reparenting.

## Decision

`parent_id` is the only persisted tree truth. Paths are derived at read time:

- single node: `NodePath` — an O(depth) parent-chain walk;
- path lookup: `GetNodeByPath` — segment descent on the
  `(tenant_id, parent_id, name)` unique index;
- subtree: one recursive CTE through `RelationalQuerier.Query`
  (the tenant-guarded raw-SQL escape hatch), ordered by derived path.
- backends whose raw-SQL escape hatch is unavailable
  (`fabriqerr.ErrRawSQLUnsupported`, e.g. the in-memory fakes) fall back to a
  portable adjacency walk with the same semantics; subtree ordering is
  byte-wise (`COLLATE "C"`) on both paths.

Moves/renames are a single `OpUpdate` on the moved node: one command, one
event, zero descendant writes. Migration 0029 drops the `path` column and its
index.

## Alternatives considered

- **Keep materialized path + hook (status quo).** O(subtree) writes that
  bypass events — the worst of both.
- **Closure table.** Fast ancestor/descendant reads, but a subtree move
  rewrites O(subtree × depth) closure rows — reintroduces the bulk-write
  problem.
- **Per-row move commands.** One event per descendant, O(subtree) commands —
  correct event semantics but pathological for large folders (10k nodes =
  10k commands + 10k events for a rename).

## Consequences

- `fs_node.updated` ("moved") events fire only for the moved node. A consumer
  that cares about descendants must treat a parent move as a subtree-scoped
  change (re-derive paths on read, or reconcile by prefix).
- Reads that need paths pay O(depth) point lookups or one CTE; the explorer
  UI reads (`ListChildren`, `WatchChildren`) never needed paths and are
  unchanged.
- `domain.FsNode` no longer has a `Path` field; `FsRef.Path` remains
  (computed, not persisted).
- The CTE truncates silently at the `fsMaxDepth` (512) backstop while the
  portable walk errors — divergence documented in code, unobservable at
  filesystem depths.
