# ADR 0006 — No RLS on the telemetry hypertable

**Status:** accepted · 2026-06-12

TimescaleDB's columnstore refuses tables with row security
(`ERROR: columnstore cannot be used on table with row security`,
observed against timescaledb-ha pg16). Compressed telemetry is the reason
Timescale is in the stack at all — industrial tag readings at volume are
unaffordable uncompressed — so `tag_readings` keeps compression and drops
RLS.

Compensating controls:

1. The readings table is reachable ONLY through the TSQuerier
   (`BulkWrite`/`Range`), which stamps `tenant_id` into every statement
   structurally and validates the series name.
2. It is not a registry entity: the generic relational port cannot name
   it.
3. The raw-SQL escape hatch is guarded: any SQL referencing an
   unprotected tenant table without a literal `tenant_id` is rejected
   with `ErrTenantHookTripped` (counted, alertable).
4. Cross-tenant isolation is integration-tested
   (`TestPG_TimescaleBulkWriteAndRange`).

Revisit if Timescale lifts the restriction; the migration to add the
policy back is one `CREATE POLICY`.
