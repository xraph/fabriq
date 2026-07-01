# ADR 0008 — File plane: catalog in fabriq, bytes via the Trove library

**Status:** proposed · 2026-06-17

## Context

fabriq needs to support files (documents, images, attachments) to power a
consuming app's `files`/`explorer` extensions and to let files be first-class knowledge-
graph citizens. Files are two things with different storage shapes: a small,
queryable *catalog* (name, tree position, checksum, permissions) and large,
opaque *bytes*. fabriq is excellent at the former and deliberately stores none
of the latter.

A byte store cannot be regenerated from Postgres. That collides with fabriq's
most sacred invariant — *Postgres is the single linearizable anchor and
everything else is a derived, rebuildable read model* (ADR 0007). This ADR
records how files fit fabriq without diluting that invariant beyond one named,
fenced exception.

## Decision

**1. The catalog lives in fabriq; the bytes live in an object store reached
through the Trove byte engine used as a *library*.**

- fabriq owns static entities — `BlobObject`, `BlobCas`, `FsNode`,
  `FsPermission`, `FsShare`, `FsBookmark`, `BlobSource` — with `tenant_id` +
  `scope_id` + FORCE RLS, projected to graph (the folder tree, attachments) and
  search (filename/metadata).
- The byte plane is a new capability port `core/blob.Store` (pure fabriq
  vocabulary, zero trove types), implemented by `adapters/trove`, exposed as
  `f.Blob()` (shipped dark — nil unless configured, like Graph/Search).

**2. Depend on the Trove *library*, never the Trove *extension*.**

Permitted imports: `github.com/xraph/trove`, `trove/driver`, `trove/drivers/*`,
`trove/middleware/*`, `trove/cas`. Forbidden: `trove/extension`, `trove/store`,
`trove/model`, `trove/handler`, `trove/hooks`.

The core `trove.Trove` facade holds `router, driver, pool, resolver, cas,
vfsBucket` — no store, no Grove, no `trove_*` table; `Put` is
`middleware → router → driver.Put` and persists nothing. The DB-backed catalog
exists only in `trove/extension`. Using the library therefore guarantees, by
construction *and* by the compile-time dependency graph, that **Trove holds no
metadata and can never be a source of truth**. It also keeps fabriq's
dependency tree free of grove/forge/dashboard/the SQL+Mongo store backends — it
pulls only the cloud SDK for the configured driver.

DI and `config.yaml` datasource configuration are obtained from **Forge** (via a
`forgeext` storage provider that reuses Trove's pure `driver` registry +
`trove.Open` and does its own `vessel.Provide`/`ProvideNamed`), not from the
Trove extension.

**3. CAS ref-counts are fabriq's, not Trove's.** When dedup is enabled,
fabriq implements Trove's `cas.Index` over a `blob_cas` table, so even the dedup
ledger is fabriq state. Garbage collection folds into the reconciler.

**4. The write path is reserve → prepare → commit; the metadata command is the
sole authority.** Bytes are prepared out of band (server-ingest for
small/dedup, presigned client-direct for large media). The `BlobObject` command
in the Postgres transaction is the only authoritative, versioned write; bytes
prepared without a committed command are orphans the reconciler GCs.

## Amendment to ADR 0007

ADR 0007 says Postgres is the single anchor and everything else is derived and
rebuildable. This ADR introduces **exactly one** new, bounded category that is
*not* rebuildable: **referenced external blobs** — opaque bytes in an object
store, referenced by a fabriq catalog row.

The carve-out is fenced by three disciplines:

1. **Opaque, no catalog authority.** The store holds bytes only. There is no
   `trove_objects` table (we use the library); all queryable state and all
   relationships remain in Postgres. You query fabriq, never the byte store.
2. **Checksum-verifiable.** Every byte object is checksum-stamped (and, in
   server-ingest mode, content-addressed), so the Postgres↔bytes relationship is
   *verifiable* even though not *regenerable*.
3. **Reconciled, not rebuilt.** The reconciler treats the store as a checkable
   peer (existence + checksum correspondence + CAS ref-count integrity), a
   distinct semantics from projection rebuild.

ADR 0007's distributed-systems prohibitions (no multi-master writes, no own
consensus, no distributed transactions, no distributed query) are untouched.

## Tenancy

Catalog tenancy is the normal stamped-tx + FORCE RLS path (ADR 0002). The object
store cannot do RLS, so it is treated like fabriq's other non-RLS engines (Redis
key prefixes, FalkorDB graph-per-tenant, Elasticsearch index routing, Timescale
per ADR 0006): **structural key stamping** — bucket/key derived from tenant +
scope in one place — plus key-scoped, time-limited presigned URLs and a raw-
access guard that rejects cross-tenant keys.

## Consequences

- fabriq gains files without storing a byte, without a second metadata source of
  truth, and without inheriting Trove's grove/forge dependency surface.
- The consuming app's `files`/`explorer` extensions keep their policy (ACL enforcement,
  share/token logic, image transforms, HTTP) and swap persistence from Grove ORM
  to fabriq — files become versioned, scoped, live, graph-projected, searchable,
  and reconciled.
- A new reconcile mode and a grace-window discipline must be built and owned; the
  byte store's durability/availability becomes part of fabriq's operational
  surface.
- Rejected alternatives: bytes-in-Postgres (`bytea`) — breaks one-event fan-out
  and Timescale-class scaling; importing the Trove extension store-disabled —
  drags grove/forge/dashboard into fabriq for code that is switched off.

Design detail: `docs/superpowers/specs/2026-06-17-fabriq-file-plane-design.md`.
