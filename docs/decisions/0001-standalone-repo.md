# ADR 0001 — fabriq is a standalone Forge-ecosystem repo

**Status:** accepted · 2026-06-12

The original spec placed the fabric inside the twinos monorepo as
`<monorepo>/fabric`. The product owner renamed it **fabriq** and provided
a dedicated repository (`/Users/rexraphael/Work/TwinOS/fabriq`), so it is
built as its own module following the Forge ecosystem convention:

- Module path `github.com/xraph/fabriq` (ecosystem rule:
  `github.com/xraph/<folder-name>`), Go 1.25.7 (matching grove/forge).
- twinos consumes it like any other xraph dependency (a `replace` to the
  local checkout during co-development, as twinos already does for grove
  and forge).
- The strict `core/` / `adapters/` / `domain/` boundaries the spec wanted
  for "later extraction" are enforced from day one by depguard; extraction
  is already done.

The facade interface keeps the conceptual name `query.Fabric` (a fabriq
provides a Fabric); binaries, tables (`fabriq_*`), streams
(`fabriq:events`) and env vars (`FABRIQ_*`) use the product name.
