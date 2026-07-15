# Retire the no-attribute-cache generic CDC export path (#193)

**Issue:** [#193](https://github.com/Lychee-Technology/forma/issues/193) — follow-up from PR #191 (#173, epic #172)
**Date:** 2026-07-14
**Scope:** `internal/cdc` only

## Problem

PR #191 fixed the schema-aware CDC export to read the numeric EAV family from
`value_numeric` (bool as 1/0, dates as epoch ms), matching the mainline write
path. The **generic** export path — taken when a schema's attribute cache can't
be resolved (`resolveSchemaAttrCache` returns nil / `GetSchemaAttributeCacheByID`
errors) — still emits a `value_text`-only `attributes` struct.

This is latent today: the production federated reader already fails fast when no
attribute cache is loaded (`ErrSchemaMetadataCacheRequired`,
`internal/federated/duckdb_query.go:250`), so a no-cache schema can never be
queried. But the write path silently produces Parquet the reader could never
consume correctly, and it will bite whoever wires a schema without registry JSON.

## Decision

**Retire the generic path (fail fast), aborting the whole run via pre-flight
validation.**

Two decisions were settled during brainstorming:

1. **Retire, not align.** Correct numeric-family typing (bool→1/0, date→epoch-ms)
   cannot be derived without the attribute cache, so "align the output shape" is
   infeasible. And even a shape-aligned output would have no consumer, because
   the reader fails fast for no-cache schemas. Retiring makes the write path
   symmetric with the read path.
2. **Abort the whole run, all-or-nothing.** A single unresolvable schema aborts
   the entire flush/init run **before any side effect** (no Parquet writes, no
   `change_log` marking), rather than failing per-schema into the `errors.Join`
   aggregate. Realized as a **pre-flight validation** pass ahead of both schema
   loops.

## Contract

A CDC export — delta flush (`RunOnce`/`Runner.RunOnce`) or base init (`RunInit`) —
**requires a resolvable attribute cache for every schema it will touch.** If any
schema's cache is unresolvable, the run aborts before processing any schema.

"Unresolvable" mirrors the reader exactly: **lookup error OR empty cache**
(`len(cache) == 0`) both count. A real schema's attribute cache is never empty —
it maps hot fields as well as EAV attributes — so an empty cache means "not
found," the same assumption the reader already bakes in
(`duckdb_query.go:242`).

The top-level entrypoints already reject a nil registry
(`flusher.go` `RunOnce`, `runner.go` `Runner.RunOnce`), so the only live gap this
closes is the per-schema case: registry present, but one schema's lookup fails.

## Behavior — pre-flight, all-or-nothing

### Flush (`internal/cdc/flusher.go`)

- Add a pre-flight pass in `processSchemas` (ahead of the per-schema loop): for
  every `schemaID`, resolve its cache into a `map[int16]forma.SchemaAttributeCache`.
  Any error or empty cache → return immediately, naming the offending schema; no
  schema flushes.
- Thread the resolved map down so `executeFlush` reads the pre-resolved cache
  instead of re-resolving. The per-schema warn+fallback at `flusher.go:364-370`
  is removed — `executeFlush` never sees a nil cache.

### Init (`internal/cdc/init.go`)

- Symmetric pre-flight in `processInitSchemas` (ahead of the loop).
- `resolveSchemaAttrCache` changes from *warn → return nil* to *return
  `(cache, error)`*; pre-flight aborts on the first unresolvable schema before any
  `initSchema` runs.

## Defense in depth + dead-code removal

- `buildExportSQLPlan` (`duckdb_exporter.go:226`): convert the `len(attrCache)==0`
  branch into a hard error instead of building generic SQL. The invariant then
  holds even if a future caller bypasses pre-flight.
- Cascade-delete the now-dead code, verified by compile + lint:
  - `buildGenericExportSQL`
  - `exportModeSpec.defaultMainColumns` field + its `defaultDeltaMainColumns` /
    `defaultBaseMainColumns` assignments and the funcs
  - the generic `mainSelectCols` construction (duckdb_exporter.go:227-231)
- `init_sizing.go:36`'s `len(attrCache)==0` sizing fallback becomes dead too —
  optional trim (leave only if it complicates the diff).

## Error semantics

This is the operator / CDC path, **not** HTTP write-path validation — so a plain
operator-visible error, **not** wrapped in `forma.ErrInvalidInput`
(see `docs/error-handling.md`). Introduce a sentinel in the `cdc` package (e.g.
`ErrSchemaAttrCacheUnavailable`) for `errors.Is` matching in tests.

The message names the logical value (attribute metadata cache), the `schema_id`,
and the remedy (register the schema's JSON, or drop it from the flush set).

## Testing (acceptance: a regression test pins the chosen behavior)

- Invert `TestExecuteFlush_FallsBackToGenericProjectionWhenSchemaLookupFails` →
  `TestRunFlush_AbortsWhenSchemaCacheUnavailable`: asserts `errors.Is(sentinel)`,
  the error names the schema, and **no schema was flushed** (zero `executeSingle`
  invocations) — pins all-or-nothing.
- `TestRunInit_AbortsWhenSchemaCacheUnavailable` — symmetric for the init path.
- `TestBuildExportSQLPlan_ErrorsWithoutAttrCache` — pins the defense-in-depth
  hard error at the exporter boundary.
- Update the `internal/e2e_harness/production/multi_schema_isolation_e2e_test.go`
  comment: missing schema metadata is no longer a latent generic path; it is now
  a hard pre-flight abort owned by this change.

## Files touched

- `internal/cdc/flusher.go` — pre-flight in `processSchemas`; remove per-schema
  warn+fallback; thread resolved caches.
- `internal/cdc/init.go` — pre-flight in `processInitSchemas`;
  `resolveSchemaAttrCache` returns `(cache, error)`.
- `internal/cdc/duckdb_exporter.go` — hard error on empty cache; delete dead
  generic SQL builder + exclusive helpers.
- `internal/cdc/` sentinel declaration (`ErrSchemaAttrCacheUnavailable`).
- `internal/cdc/flusher_test.go` — invert existing test + new abort tests.
- `internal/e2e_harness/production/multi_schema_isolation_e2e_test.go` — comment
  update.
- `internal/cdc/init_sizing.go` — optional dead-branch trim.

## Out of scope

- The reader side (already fails fast).
- Any change to the schema-aware projection path (unaffected).
- Adding a Docker-backed e2e vector for missing metadata — the unit-level abort
  tests satisfy the acceptance criterion; an e2e vector is a possible follow-up.
