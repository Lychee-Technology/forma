# Retire the No-Attribute-Cache Generic CDC Export Path (#193) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CDC export (delta flush + base init) fail fast when a schema's attribute metadata cache is unresolvable, aborting the whole run before any side effect, and delete the dead `value_text`-only generic projection.

**Architecture:** A shared `resolveRequiredAttrCache` helper (new `internal/cdc/errors.go`) treats a registry lookup error or an empty cache as a hard failure carrying the sentinel `ErrSchemaAttrCacheUnavailable`. Both schema loops (`processSchemas` for flush, `processInitSchemas` for init) run a **pre-flight pass** that resolves every schema's cache up front and aborts on the first failure — nothing flushes. The per-schema warn+fallback is removed and the export SQL builder hard-errors on an empty cache as defense in depth, letting the generic SQL code path be deleted.

**Tech Stack:** Go, DuckDB (`postgres_query` export), testify (`require`), zap logging. Package `internal/cdc`.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (`coding-standard.md`).
- Always wrap errors with context: `fmt.Errorf("...: %w", err)` — never bare `return err`.
- This is an operator/CDC path error, NOT HTTP write-path validation: the sentinel is a plain error, **NOT** wrapped in `forma.ErrInvalidInput` (`docs/error-handling.md`).
- Error messages must name the logical value (attribute metadata cache), the `schema_id`, and the expected state / remedy.
- Match errors with `errors.Is`/`errors.As`, never string comparison.
- Test env for single-test runs (mirrors the Makefile): `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false`.
- Module path: `github.com/lychee-technology/forma`. Type: `forma.SchemaAttributeCache map[string]forma.AttributeMetadata`.

---

### Task 1: Sentinel error + shared required-cache resolver

**Files:**
- Create: `internal/cdc/errors.go`
- Create: `internal/cdc/errors_test.go`
- Modify: `internal/cdc/mocks_test.go` (add `stubSchemaRegistry` test helper)

**Interfaces:**
- Produces: `var ErrSchemaAttrCacheUnavailable error` (sentinel) and
  `func resolveRequiredAttrCache(reg forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, error)`.
  Returns `(nil, err)` wrapping the sentinel when `reg == nil`, when
  `reg.GetSchemaAttributeCacheByID` errors, or when the returned cache is
  empty (`len == 0`). Returns `(cache, nil)` when the cache is non-empty.
- Produces (test helper): `type stubSchemaRegistry struct{ cache forma.SchemaAttributeCache }` implementing `forma.SchemaRegistry`, returning `cache` with no error.

- [ ] **Step 1: Add the `stubSchemaRegistry` test helper to `mocks_test.go`**

Append to `internal/cdc/mocks_test.go` (after the `errorSchemaRegistry` methods):

```go
// stubSchemaRegistry returns a fixed attribute cache with no error. A nil/empty
// cache field models a schema whose lookup succeeds but carries no attributes.
type stubSchemaRegistry struct{ cache forma.SchemaAttributeCache }

func (r stubSchemaRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, r.cache, nil
}

func (r stubSchemaRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", r.cache, nil
}

func (r stubSchemaRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, nil
}

func (r stubSchemaRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, nil
}

func (r stubSchemaRegistry) ListSchemas() []string { return nil }

// testAttrCache is a minimal populated cache (one column-bound text attr, one
// EAV bool attr) shared by tests that need a resolvable schema.
func testAttrCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {
			AttributeName: "name",
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"flag": {
			AttributeName: "flag",
			AttributeID:   11,
			ValueType:     forma.ValueTypeBool,
		},
	}
}
```

- [ ] **Step 2: Write the failing test for the resolver**

Create `internal/cdc/errors_test.go`:

```go
package cdc

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestResolveRequiredAttrCache(t *testing.T) {
	t.Run("registry lookup error", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(errorSchemaRegistry{err: errors.New("boom")}, 7)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
		require.Contains(t, err.Error(), "7")
	})
	t.Run("empty cache", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(stubSchemaRegistry{cache: forma.SchemaAttributeCache{}}, 9)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
		require.Contains(t, err.Error(), "9")
	})
	t.Run("nil registry", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(nil, 3)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	})
	t.Run("populated cache", func(t *testing.T) {
		cache, err := resolveRequiredAttrCache(stubSchemaRegistry{cache: testAttrCache()}, 1)
		require.NoError(t, err)
		require.NotEmpty(t, cache)
	})
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestResolveRequiredAttrCache -v`
Expected: FAIL — `undefined: resolveRequiredAttrCache` / `undefined: ErrSchemaAttrCacheUnavailable`.

- [ ] **Step 4: Create `internal/cdc/errors.go`**

```go
package cdc

import (
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
)

// ErrSchemaAttrCacheUnavailable marks a CDC export that cannot proceed because a
// schema's attribute metadata cache could not be resolved — the registry lookup
// failed or the cache is empty. CDC export needs it to produce parquet the
// federated reader can consume; without it the reader fails fast with
// ErrSchemaMetadataCacheRequired (#193). This is an operator-visible
// configuration error, not write-path validation, so it is NOT wrapped in
// forma.ErrInvalidInput.
var ErrSchemaAttrCacheUnavailable = errors.New("schema attribute metadata cache unavailable")

// resolveRequiredAttrCache resolves schemaID's attribute cache, treating a nil
// registry, a lookup error, or an empty cache as a hard failure. A real schema's
// cache is never empty (it maps hot fields as well as EAV attributes), so an
// empty cache means the schema is not registered — the same assumption the
// federated reader makes (internal/federated/duckdb_query.go).
func resolveRequiredAttrCache(reg forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, error) {
	if reg == nil {
		return nil, fmt.Errorf("resolve attribute cache for schema %d: schema registry is nil: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
	_, cache, err := reg.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve attribute cache for schema %d: %w: %w", schemaID, ErrSchemaAttrCacheUnavailable, err)
	}
	if len(cache) == 0 {
		return nil, fmt.Errorf("schema %d has an empty attribute metadata cache: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
	return cache, nil
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestResolveRequiredAttrCache -v`
Expected: PASS (all four subtests).

- [ ] **Step 6: Commit**

```bash
git add internal/cdc/errors.go internal/cdc/errors_test.go internal/cdc/mocks_test.go
git commit -m "feat(cdc): add ErrSchemaAttrCacheUnavailable + resolveRequiredAttrCache (#193)"
```

---

### Task 2: Hard-error the export SQL builder on empty cache; delete generic path

**Files:**
- Modify: `internal/cdc/duckdb_exporter.go` (`buildExportSQLPlan` empty-cache branch → error; delete `buildGenericExportSQL`; delete `defaultMainColumns` field; fix dangling comment)
- Modify: `internal/cdc/export_sql_builder.go` (delete `defaultDeltaMainColumns`, `defaultBaseMainColumns`)
- Modify: `internal/cdc/init_exporter.go` (drop `defaultMainColumns` from the base `exportModeSpec` literal)
- Modify: `internal/cdc/duckdb_exporter_sql_test.go` (migrate `nil`-cache tests to `testAttrCache()`; add error test)
- Modify: `internal/cdc/init_exporter_sql_test.go` (migrate `nil`-cache tests to `testAttrCache()`; add error test)

**Interfaces:**
- Consumes: `ErrSchemaAttrCacheUnavailable` (Task 1), `testAttrCache()` (Task 1).
- Produces: `buildExportSQLPlan` returns `(exportSQLPlan{}, error wrapping ErrSchemaAttrCacheUnavailable)` when `len(attrCache) == 0`. `buildExportSQL` and `buildBaseExportSQL` propagate that error. The `exportModeSpec.defaultMainColumns` field and the two `default*MainColumns` funcs no longer exist.

- [ ] **Step 1: Write the failing error test (delta path)**

Add to `internal/cdc/duckdb_exporter_sql_test.go`:

```go
func TestBuildExportSQL_ErrorsWithoutAttrCache(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
	_, _, _, _, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "1")
}
```

Add the testify import to the file's import block if absent:

```go
	"github.com/stretchr/testify/require"
```

- [ ] **Step 2: Write the failing error test (base/init path)**

Add to `internal/cdc/init_exporter_sql_test.go` (add the `require` import if absent):

```go
func TestBuildBaseExportSQL_ErrorsWithoutAttrCache(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
	_, _, _, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, []uuid.UUID{rowID}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "1")
}
```

- [ ] **Step 3: Run both new tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run 'ErrorsWithoutAttrCache' -v`
Expected: FAIL — the builders currently return generic SQL and `err == nil`.

- [ ] **Step 4: Convert the empty-cache branch to a hard error in `duckdb_exporter.go`**

Replace the block at `internal/cdc/duckdb_exporter.go:226-233` (the `if len(attrCache) == 0 { ... return plan, nil }` generic branch):

```go
	if len(attrCache) == 0 {
		mainColumns := spec.defaultMainColumns()
		plan.mainQuery = buildMainEntityQuery(entityMain, schemaID, mainColumns, mFilter, spec.activeOnly)
		plan.eavQuery = buildEAVQuery(eavData, schemaID, eFilter, nil)
		mainSelectCols := append(spec.baseSelectColumns(), prefixColumns("m.", mainColumns[5:])...)
		plan.sql = buildGenericExportSQL(spec, opts, copyOptions, pgEsc, s3Esc, plan.changeLogQuery, plan.mainQuery, plan.eavQuery, mainSelectCols)
		return plan, nil
	}
```

with:

```go
	if len(attrCache) == 0 {
		// A resolvable attribute cache is mandatory: without it we cannot derive
		// the numeric-family typing (bool as 1/0, dates as epoch ms) the
		// federated reader expects, and the reader itself fails fast for no-cache
		// schemas (ErrSchemaMetadataCacheRequired). Pre-flight validation in the
		// flush/init loops should already have aborted; this is defense in depth
		// for any caller that bypasses it (#193).
		return exportSQLPlan{}, fmt.Errorf("build export SQL for schema %d: attribute metadata cache is required but empty: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
```

- [ ] **Step 5: Delete the now-dead `buildGenericExportSQL` function**

Delete the entire `func buildGenericExportSQL(...)` (currently `internal/cdc/duckdb_exporter.go:257-302`).

- [ ] **Step 6: Delete the dead `defaultMainColumns` field and fix the dangling comment**

In `internal/cdc/duckdb_exporter.go`, remove the field from `exportModeSpec` (line 26):

```go
	defaultMainColumns func() []string
```

In `buildExportSQL` (line 163-172), remove the `defaultMainColumns: defaultDeltaMainColumns,` line from the `exportModeSpec{...}` literal.

In `buildProjectedExportSQL`, update the comment that references the deleted function. Change:

```go
		// LEFT JOIN to entity_main so change_log tombstones of hard-deleted
		// rows are exported instead of silently dropped (see
		// buildGenericExportSQL, #173).
```

to:

```go
		// LEFT JOIN to entity_main so change_log tombstones of hard-deleted
		// rows are exported instead of silently dropped (#173).
```

- [ ] **Step 7: Delete the two dead column-default funcs**

In `internal/cdc/export_sql_builder.go`, delete `func defaultDeltaMainColumns() []string { ... }` (line 162) and `func defaultBaseMainColumns() []string { ... }` (line 179).

In `internal/cdc/init_exporter.go` (line 27-37), remove the `defaultMainColumns: defaultBaseMainColumns,` line from the base `exportModeSpec{...}` literal.

- [ ] **Step 8: Migrate the delta generic-SQL tests to a populated cache**

In `internal/cdc/duckdb_exporter_sql_test.go`:

In `TestBuildExportSQL_UsesRowIDsAndConfig`, change the `buildExportSQL(..., nil)` call (line 15) to pass `testAttrCache()` instead of the trailing `nil`. Then replace the projected-column assertion:

```go
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "attributes") {
		t.Fatalf("sql missing projected columns (changed_at/attributes): %s", sql)
	}
```

with (the schema-driven SQL projects the eav attribute by name, not the literal `attributes` list):

```go
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing projected columns (changed_at/flag): %s", sql)
	}
```

In `TestBuildExportSQL_UsesCustomTableNames`, change the `buildExportSQL(..., nil)` call (line 94) to pass `testAttrCache()`. Its assertions (custom `FROM` table names) are unchanged.

Leave `TestBuildExportSQL_ErrorsOnEmptyRowIDs` as-is — it passes `nil` row ids, which errors in `buildExportSQLPlan` before the cache check, so it still asserts `err != nil` correctly.

- [ ] **Step 9: Migrate the base/init generic-SQL tests to a populated cache**

In `internal/cdc/init_exporter_sql_test.go`:

In `TestBuildBaseExportSQL_UsesRowIDsAndConfig`, change the `buildBaseExportSQL(..., nil)` call (line 15) to pass `testAttrCache()`. Replace:

```go
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "attributes") {
		t.Fatalf("sql missing projected columns (changed_at/attributes): %s", sql)
	}
```

with:

```go
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing projected columns (changed_at/flag): %s", sql)
	}
```

In `TestBuildBaseExportSQL_UsesCustomTableNames`, change the `buildBaseExportSQL(..., nil)` call (line 90) to pass `testAttrCache()`. Its `FROM` assertions are unchanged.

Leave `TestBuildBaseExportSQL_ErrorsOnEmptyRowIDs` as-is (nil row ids error first).

- [ ] **Step 10: Run the full cdc package tests to verify green**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -v -run 'BuildExportSQL|BuildBaseExportSQL'`
Expected: PASS, including the two new `ErrorsWithoutAttrCache` tests. No `undefined: buildGenericExportSQL` / `undefined: defaultDeltaMainColumns` compile errors.

- [ ] **Step 11: Verify no dangling references to deleted symbols**

Run: `grep -rn "buildGenericExportSQL\|defaultMainColumns\|defaultDeltaMainColumns\|defaultBaseMainColumns" internal/cdc/`
Expected: no matches (empty output).

- [ ] **Step 12: Commit**

```bash
git add internal/cdc/duckdb_exporter.go internal/cdc/export_sql_builder.go internal/cdc/init_exporter.go internal/cdc/duckdb_exporter_sql_test.go internal/cdc/init_exporter_sql_test.go
git commit -m "feat(cdc): hard-error export SQL on empty attr cache; delete generic path (#193)"
```

---

### Task 3: Flush pre-flight — abort the whole run on any unresolvable cache

**Files:**
- Modify: `internal/cdc/flusher.go` (`schemaFlushContext` gains `attrCaches`; `processSchemas` pre-flight; `executeFlush` reads pre-resolved cache)
- Modify: `internal/cdc/flusher_test.go` (replace `TestExecuteFlush_FallsBackToGenericProjectionWhenSchemaLookupFails` with a pre-flight abort test + a positive pass-through test)

**Interfaces:**
- Consumes: `resolveRequiredAttrCache`, `ErrSchemaAttrCacheUnavailable` (Task 1); `stubSchemaRegistry`, `testAttrCache()` (Task 1).
- Produces: `processSchemas` returns an error wrapping `ErrSchemaAttrCacheUnavailable` (and never invokes `processSchema`) when any schema's cache is unresolvable. On success it populates `c.attrCaches[schemaID]` for every schema before the per-schema loop. `executeFlush` reads `c.attrCaches[schemaID]` instead of consulting the registry.

- [ ] **Step 1: Write the failing pre-flight abort test**

In `internal/cdc/flusher_test.go`, delete `TestExecuteFlush_FallsBackToGenericProjectionWhenSchemaLookupFails` (lines 414-451) and add:

```go
func TestProcessSchemas_AbortsWhenSchemaCacheUnavailable(t *testing.T) {
	processed := false
	flushCtx := &schemaFlushContext{
		logger:         zap.NewNop(),
		schemaRegistry: errorSchemaRegistry{err: errors.New("schema unavailable")},
		processSchemaFn: func(context.Context, int16) error {
			processed = true
			return nil
		},
	}

	err := flushCtx.processSchemas(context.Background(), []int64{7})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "7")
	require.False(t, processed, "no schema must be flushed when pre-flight aborts")
}

func TestProcessSchemas_ResolvesCachesBeforeProcessing(t *testing.T) {
	var processedIDs []int16
	flushCtx := &schemaFlushContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		processSchemaFn: func(_ context.Context, id int16) error {
			processedIDs = append(processedIDs, id)
			return nil
		},
	}

	err := flushCtx.processSchemas(context.Background(), []int64{7, 8})
	require.NoError(t, err)
	require.Equal(t, []int16{7, 8}, processedIDs)
	require.NotEmpty(t, flushCtx.attrCaches[7])
	require.NotEmpty(t, flushCtx.attrCaches[8])
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestProcessSchemas -v`
Expected: FAIL — `flushCtx.attrCaches` undefined, and no pre-flight yet so the abort test either processes the schema or does not error with the sentinel.

- [ ] **Step 3: Add the `attrCaches` field to `schemaFlushContext`**

In `internal/cdc/flusher.go`, add the field to the `schemaFlushContext` struct (after `schemaRegistry`, line 248):

```go
	attrCaches       map[int16]forma.SchemaAttributeCache
```

- [ ] **Step 4: Add the pre-flight pass to `processSchemas`**

In `internal/cdc/flusher.go`, in `processSchemas`, insert the pre-flight immediately after the `len(schemaIDs) == 0` early return (after line 261) and before `processSchema := c.processSchemaFn`:

```go
	caches := make(map[int16]forma.SchemaAttributeCache, len(schemaIDs))
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		cache, err := resolveRequiredAttrCache(c.schemaRegistry, schemaID)
		if err != nil {
			return fmt.Errorf("cdc flush pre-flight: %w", err)
		}
		caches[schemaID] = cache
	}
	c.attrCaches = caches
```

- [ ] **Step 5: Make `executeFlush` read the pre-resolved cache**

In `internal/cdc/flusher.go`, replace the registry-lookup block in `executeFlush` (lines 363-370):

```go
	var attrCache forma.SchemaAttributeCache
	if c.schemaRegistry != nil {
		if _, cache, err := c.schemaRegistry.GetSchemaAttributeCacheByID(schemaID); err != nil {
			c.logger.Sugar().Warnw("schema registry lookup failed, using generic projection", "schema_id", schemaID, "err", err)
		} else {
			attrCache = cache
		}
	}
```

with:

```go
	// Cache was resolved and validated by the processSchemas pre-flight (#193).
	attrCache := c.attrCaches[schemaID]
```

- [ ] **Step 6: Run the flush tests to verify green**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run 'TestProcessSchemas|TestExecuteFlush|TestExecuteBatch|TestRunOnce' -v`
Expected: PASS. The two direct-`executeFlush` tests (`flush_batch_test.go`, the remaining `flusher_test.go` case) still pass — they set no registry, so `c.attrCaches[schemaID]` reads nil from a nil map (safe) and their mocked executor never invokes the exporter.

- [ ] **Step 7: Commit**

```bash
git add internal/cdc/flusher.go internal/cdc/flusher_test.go
git commit -m "feat(cdc): pre-flight abort flush when any schema cache is unresolvable (#193)"
```

---

### Task 4: Init pre-flight — abort base export on any unresolvable cache

**Files:**
- Modify: `internal/cdc/init.go` (`initRunContext` gains `attrCaches`; `processInitSchemas` pre-flight; `prepareSchemaInit` reads pre-resolved cache; delete `resolveSchemaAttrCache`)
- Create: `internal/cdc/init_preflight_test.go`

**Interfaces:**
- Consumes: `resolveRequiredAttrCache`, `ErrSchemaAttrCacheUnavailable` (Task 1); `errorSchemaRegistry` (existing), `stubSchemaRegistry`, `testAttrCache()` (Task 1).
- Produces: `processInitSchemas(ctx, runCtx, schemaIDs)` returns `(InitSummary{}, error wrapping ErrSchemaAttrCacheUnavailable)` without running any `initSchema` when a cache is unresolvable. On success it populates `runCtx.attrCaches[schemaID]` before the loop. `prepareSchemaInit` reads `runCtx.attrCaches[schemaID]`. `resolveSchemaAttrCache` is removed.

- [ ] **Step 1: Write the failing init pre-flight abort test**

Create `internal/cdc/init_preflight_test.go`:

```go
package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProcessInitSchemas_AbortsWhenSchemaCacheUnavailable(t *testing.T) {
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: errorSchemaRegistry{err: errors.New("schema unavailable")},
	}

	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "7")
	require.Equal(t, InitSummary{}, summary)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestProcessInitSchemas_Aborts -v`
Expected: FAIL — no pre-flight, so `processInitSchemas` proceeds into `initSchema` (nil `db` panic or a non-sentinel error), not a clean sentinel abort.

- [ ] **Step 3: Add the `attrCaches` field to `initRunContext`**

In `internal/cdc/init.go`, add to the `initRunContext` struct (after `schemaRegistry`, line 43):

```go
	attrCaches           map[int16]forma.SchemaAttributeCache
```

- [ ] **Step 4: Add the pre-flight pass to `processInitSchemas`**

In `internal/cdc/init.go`, at the very top of `processInitSchemas` (before `summary := InitSummary{}`):

```go
	runCtx.attrCaches = make(map[int16]forma.SchemaAttributeCache, len(schemaIDs))
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		cache, err := resolveRequiredAttrCache(runCtx.schemaRegistry, schemaID)
		if err != nil {
			return InitSummary{}, fmt.Errorf("cdc init pre-flight: %w", err)
		}
		runCtx.attrCaches[schemaID] = cache
	}
```

- [ ] **Step 5: Make `prepareSchemaInit` read the pre-resolved cache and delete `resolveSchemaAttrCache`**

In `internal/cdc/init.go`, in `prepareSchemaInit`, replace line 275:

```go
	state.attrCache = resolveSchemaAttrCache(runCtx, schemaID)
```

with:

```go
	// Cache was resolved and validated by the processInitSchemas pre-flight (#193).
	state.attrCache = runCtx.attrCaches[schemaID]
```

Then delete the entire `func resolveSchemaAttrCache(runCtx *initRunContext, schemaID int16) forma.SchemaAttributeCache { ... }` (lines 280-290).

- [ ] **Step 6: Run the init tests to verify green**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run 'TestProcessInitSchemas|TestResolveInit|TestRunInit|Init' -v`
Expected: PASS. Confirm no `undefined: resolveSchemaAttrCache` reference remains:
`grep -rn "resolveSchemaAttrCache" internal/cdc/` → empty.

- [ ] **Step 7: Commit**

```bash
git add internal/cdc/init.go internal/cdc/init_preflight_test.go
git commit -m "feat(cdc): pre-flight abort base init when any schema cache is unresolvable (#193)"
```

---

### Task 5: Update the e2e comment and verify the full suite + lint

**Files:**
- Modify: `internal/e2e_harness/production/multi_schema_isolation_e2e_test.go` (comment only)

**Interfaces:**
- Consumes: all prior tasks.
- Produces: no code change beyond the comment; a clean `make test` + `make lint`.

- [ ] **Step 1: Update the stale comment in the isolation e2e**

In `internal/e2e_harness/production/multi_schema_isolation_e2e_test.go`, replace the comment block (lines 29-31):

```go
// Missing schema metadata is deliberately NOT a vector: the flusher only
// warns and falls back to the generic projection (internal/cdc/flusher.go),
// a latent path owned by #193.
```

with:

```go
// Missing schema metadata is not exercised here: as of #193 the flusher and
// init both pre-flight every schema's attribute cache and abort the whole run
// (ErrSchemaAttrCacheUnavailable) before any side effect, rather than falling
// back to a generic projection. That contract is pinned by unit tests in
// internal/cdc (TestProcessSchemas_AbortsWhenSchemaCacheUnavailable,
// TestProcessInitSchemas_AbortsWhenSchemaCacheUnavailable).
```

- [ ] **Step 2: Run the full cdc unit suite**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -count=1`
Expected: `ok  github.com/lychee-technology/forma/internal/cdc`.

- [ ] **Step 3: Build the e2e harness package (compile check for the comment file)**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet ./internal/e2e_harness/production/...`
Expected: no errors (comment-only change compiles).

- [ ] **Step 4: Run the project unit-test target**

Run: `make test`
Expected: PASS (wraps `go test . ./cdc ./cmd/... ./factory ./internal/...`).

- [ ] **Step 5: Run lint**

Run: `make lint`
Expected: clean — in particular no `unused` findings for deleted symbols and no `errcheck`/`wrapcheck` regressions in the edited files.

- [ ] **Step 6: Commit**

```bash
git add internal/e2e_harness/production/multi_schema_isolation_e2e_test.go
git commit -m "docs(cdc): pin #193 pre-flight-abort contract in isolation e2e comment"
```

---

## Self-Review

**Spec coverage:**
- "Retire, not align; fail fast" → Tasks 2, 3, 4 (hard error + pre-flight abort). ✓
- "Abort whole run via pre-flight validation" → Task 3 (`processSchemas`), Task 4 (`processInitSchemas`). ✓
- "Unresolvable = lookup error OR empty cache" → Task 1 `resolveRequiredAttrCache` (both branches) + test. ✓
- "Top-level entrypoints already reject nil registry; close the per-schema gap" → pre-flight runs per-schema; helper also guards nil registry defensively. ✓
- "Defense in depth: exporter hard-errors on empty cache" → Task 2 Step 4. ✓
- "Delete dead generic code (`buildGenericExportSQL`, `defaultMainColumns` + `default*MainColumns`)" → Task 2 Steps 5-7 + grep gate Step 11. ✓
- "Plain operator error, not `ErrInvalidInput`; names logical value + schema_id + remedy" → Task 1 `errors.go` (sentinel is plain `errors.New`; messages name schema id). ✓
- "Regression tests pin behavior" → Task 2 (`*ErrorsWithoutAttrCache`), Task 3 (`TestProcessSchemas_Aborts...`), Task 4 (`TestProcessInitSchemas_Aborts...`). ✓
- "Update multi_schema_isolation e2e comment" → Task 5 Step 1. ✓
- "init_sizing.go `len==0` fallback: optional trim" → **intentionally left in place** as harmless defensive code; removing it would churn `resolveInitBatchSize`/`estimateRowSizeBytes` tests (`init_test.go:27,35` call it with `nil`) for no behavioral gain. Documented decision, not a gap.

**Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N" — every code step shows complete code. ✓

**Type consistency:** `resolveRequiredAttrCache(reg, schemaID) (cache, error)` used identically in Tasks 3 and 4. `attrCaches map[int16]forma.SchemaAttributeCache` field name consistent on both `schemaFlushContext` and `initRunContext`. `ErrSchemaAttrCacheUnavailable` referenced consistently. `testAttrCache()`/`stubSchemaRegistry` defined in Task 1, consumed in Tasks 2-4. ✓
