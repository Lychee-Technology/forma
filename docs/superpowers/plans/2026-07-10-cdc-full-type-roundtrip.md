# CDC Full-Type Producer-Consumer Round-Trip (#174) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove (or pin down as filed bugs) that every supported Forma value type survives write → CDC export → Parquet → federated read without loss, including NULLs, boundary values, and both bound (main-column) and EAV storage — closing issue #174 (epic #172, Phase 2).

**Architecture:** Extend the existing production E2E harness (`internal/e2e_harness/production/`) — real Postgres + RustFS containers, real `EntityManager` writes, real `cdc.RunInit`/`Runner.RunOnce`, real federated DuckDB engine, independent event-log oracle. Follow the three-tier pattern established by `date_parity_e2e_test.go` (hot baseline → init cold → flush warm → parquet physical inspection → federated merge read). New coverage is added by (a) extending the `e2e_wide` fixture with the missing EAV numeric-family attributes, (b) two new e2e test files (typical full-type matrix; NULL/boundary matrix), (c) a list-type probe that either adds coverage or files a follow-up issue per the epic's established pattern (#192–#198).

**Tech Stack:** Go, testcontainers-go (Postgres 16 + RustFS), DuckDB (in-process, `read_parquet`), `//go:build e2e` tag, `make test-e2e-production`.

## Global Constraints

- Repo: `Lychee-Technology/forma`. Branch off up-to-date `main`; per workspace rules create a fresh worktree after pruning merged branches.
- `make lint` is golangci-lint **pinned to v1.64.8** — do not upgrade the pin.
- Source files ≤500 lines, functions ≤100 lines (applies to test files too — split helpers into a second file).
- Always wrap errors with context (`fmt.Errorf("...: %w", err)`); match sentinel errors with `errors.Is`/`errors.As`.
- Single-test runs need the Makefile env: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ...`.
- E2E runs need Docker. Production suite command: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -timeout=10m` (same as `make test-e2e-production`).
- Untagged files in `internal/e2e_harness/production/` (oracle, generator, schema) run under plain `make test` — keep them compiling without the `e2e` tag.
- Sub-agents: do not use Sonnet 5; use Haiku where possible, otherwise Opus 4.8 (workspace CLAUDE.md rule).
- Git: commit with explicit file paths (no `git add -A`); never bare `git stash` (shared stash stack across worktrees).
- PR must reference `Closes #174` and summarize the issue's three hypotheses with verdicts.

---

## Verified Context (read this before touching anything)

Facts below were verified against `main` at commit `84a5fb0` on 2026-07-10. All prerequisites of #174 are merged: #192 (`f87f2d0`), #194 (`524076b`), #197 (`8d363e7`).

**Issue #174's three hypotheses — code-level status today:**

1. *"CDC writes `time_slot` but reader expects `changed_at`"* — **already refuted in code**: both sides use `changed_at` (`internal/cdc/duckdb_exporter.go:153`, `internal/cdc/init_exporter.go:35`, reader `internal/sqlgen/duckdb_schema_projection.go:141-152`). `time_slot` exists only as manifest metadata (`internal/cdc/manifest.go:14-15`). Our test must still *pin* this: the parquet schema assertion requires a `changed_at` column.
2. *"EAV numerics exported from `value_text`"* — **already refuted for the schema-driven path**: `castEAVValue` (`internal/cdc/duckdb_exporter.go:430-443`) reads `value_numeric` for bool/date/datetime/numeric family, mirroring the reader's `eavValueColumn` (`internal/sqlgen/duckdb_schema_projection.go:242-251`). BUT the current `e2e_wide` fixture has **no EAV smallint/integer/bigint/numeric/uuid attributes at all** (only bound ones), so nothing exercises this end-to-end. Task 3 fixes that gap.
3. *"Array attributes may not round-trip"* — **almost certainly confirmed broken**: the CDC EAV export query selects only `(schema_id, row_id, attr_id, value_text, value_numeric)` — no `array_indices` (`internal/cdc/export_sql_builder.go:79-88`) — and pivots with `MAX(CASE WHEN attr_id = N THEN ... END)` (`export_sql_builder.go:135`), collapsing multi-row list attributes to one value. The federated reader likewise hardcodes `'array_indices': ''` (`internal/sqlgen/duckdb_schema_projection.go:328,691`). Task 6 probes this and files a follow-up issue (epic pattern: harness surfaces bug → new issue, not an inline fix).

**Harness-vs-production DDL drift (discovered while planning; Task 2):** the production harness DDL (`internal/e2e_harness/production/ddl.go`) diverges from the real `init-db` DDL (`cmd/tools/init_db.go:130-196`):

| Table | harness `ddl.go` | production `init_db.go` |
|---|---|---|
| `eav_data.value_numeric` | `DOUBLE PRECISION` | `NUMERIC` |
| `entity_main` bigint cols | `bigint_01..03` | `bigint_01..05` |
| `entity_main` double cols | `double_01..03` | `double_01..05` |
| `change_log.deleted_at` | `BIGINT DEFAULT 0` | `BIGINT` (nullable) |

This matters for #174 because `value_numeric`'s Postgres type changes what precision the CDC `TRY_CAST(value_numeric AS BIGINT)` sees. A "production-tracing" harness must use production's types.

**Type support surface** (`schema_registry.go:10-21`): `text, smallint, integer, bigint, numeric(double), date, datetime, uuid, bool, list`. No separate object type. Bool has no main-column type — bool is EAV-only in practice for our fixtures. Bound date/datetime use `unix_ms` encoding into bigint columns (#194 contract).

**Precision reality you must respect in boundary tests:**
- EAV values travel through Go as `*float64` (`internal/model/types.go` EAVRecord; `internal/transform/attribute_converter.go:489-496` does `int64(*record.ValueNumeric)`). Therefore the largest *exactly representable* EAV bigint is **±2^53**. `math.MaxInt64` cannot round-trip through EAV by design — the test asserts 2^53 for EAV and documents why.
- Bound bigint columns (`bigint_01`) are true BIGINT end-to-end (Postgres BIGINT → `TRY_CAST(... AS BIGINT)` → parquet BIGINT → `Int64Items`), so `math.MaxInt64` *should* round-trip there **if** the write path accepts an `int64` in the `Data` map. Task 5 Step 1 probes this; fallback boundary is `int64(1) << 62` (a power of two, exactly representable even as float64).
- The oracle normalizes all numerics to float64 (`production/oracle.go:347-370`), so `AssertQueryMatches` cannot prove int64 exactness — exactness assertions must read `record.Int64Items` / parquet directly (same trick as `collectBoundDates` in `date_parity_e2e_test.go:143-161`).

**Known open bug to avoid, not test:** #200 — federated date/datetime *predicates* bind `CAST(? AS TIMESTAMP)` against epoch-ms BIGINT columns. #174 is about projection round-trip, so **do not use date/datetime filters in any query in these tests**; full scans with `Limit: 100` suffice.

**Harness API you will use** (all in `internal/e2e_harness/production/`):
- `cluster := SharedCluster(t)`, `env := NewEnv(t, cluster)` — per-test DB + S3 prefix + DuckDB.
- `env.GenerateScript(ScriptSpec{Schema, Creates})`, `CreateEvent(schema, attrs)`, `env.ApplyEvents(ctx, evs...)` — real EntityManager writes.
- `env.Query(ctx, Query{...})` / `env.AssertQueryMatches(ctx, Query{...})` — engine (+oracle) reads. `QueryResult.Records` are `*model.PersistentRecord` with `TextItems/Int16Items/Int32Items/Int64Items/Float64Items/UUIDItems/OtherAttributes`.
- `env.RunInit(ctx, schema)` (cold base parquet), `env.RunFlush(ctx)` (warm delta parquet), `env.ExecSQL`, `env.loadManifests(ctx)`, `env.Duck.DB` (raw DuckDB for `read_parquet`), `rowIDs(events)` helper (`smoke_test.go:210`).
- Template test: `date_parity_e2e_test.go` — copy its tier choreography exactly.

## File Structure

- Modify: `internal/e2e_harness/production/ddl.go` — align with `cmd/tools/init_db.go` DDL.
- Modify: `internal/e2e_harness/production/schemas/e2e_wide.json` + `e2e_wide_attributes.json` — add EAV `level/qty/total/ratio/token` (+ `tags` in Task 6).
- Modify: `internal/e2e_harness/production/generator.go` — extend `FullTypeProfile`.
- Create: `internal/e2e_harness/production/full_type_roundtrip_e2e_test.go` — Task 4 test.
- Create: `internal/e2e_harness/production/full_type_parquet_e2e_test.go` — shared parquet schema/value helpers + truth builder (kept separate for the 500-line rule; both files `//go:build e2e`).
- Create: `internal/e2e_harness/production/boundary_roundtrip_e2e_test.go` — Task 5 test.
- Create: `internal/e2e_harness/production/list_roundtrip_e2e_test.go` — Task 6 probe.
- Modify: `internal/e2e_harness/production/README.md` — coverage matrix + known gaps.

---

### Task 1: Worktree + preflight

**Files:** none (setup only)

- [ ] **Step 1:** From the main checkout: delete local branches whose PRs are merged, `git pull` main, then `git worktree add ../forma-worktrees/fix-174-full-type-roundtrip -b fix-174-full-type-roundtrip main`. All later commands run inside that worktree.
- [ ] **Step 2:** Verify prerequisites present: `git log --oneline -5` must contain the #192/#194/#197 fix commits (`f87f2d0`, `524076b`, `8d363e7` or their descendants).
- [ ] **Step 3:** Baseline green: run `make test` and `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -timeout=10m`. Expected: PASS. If the baseline is red, stop and report — do not build on a red base.

### Task 2: Align production harness DDL with `init-db`

**Files:**
- Modify: `internal/e2e_harness/production/ddl.go`
- Reference (read-only): `cmd/tools/init_db.go:120-215`

**Interfaces:** Produces: `productionDDL` with production-identical column types. No signature changes.

- [ ] **Step 1:** Edit `ddl.go`: in `eav_data`, change `value_numeric DOUBLE PRECISION,` → `value_numeric NUMERIC,`. In `entity_main`, add `bigint_04 BIGINT, bigint_05 BIGINT,` after `bigint_03` and `double_04 DOUBLE PRECISION, double_05 DOUBLE PRECISION,` after `double_03`. In `change_log`, change `deleted_at BIGINT DEFAULT 0` → `deleted_at BIGINT` (production has no default; `readBackTimestamps` already `COALESCE`s). Update the header comment: the authority is now `cmd/tools/init_db.go` (production `init-db`), not the federated harness copy.
- [ ] **Step 2:** Run the full production suite (command from Global Constraints). Expected: PASS.
  - If any test regresses: capture the failure, `git checkout -- internal/e2e_harness/production/ddl.go` to revert, and file a follow-up issue titled `e2e: production harness DDL drifts from init-db (value_numeric NUMERIC vs DOUBLE PRECISION)` with the diff table from "Verified Context". Continue with the remaining tasks on the old DDL; note the issue number in the PR body.
- [ ] **Step 3:** Commit: `git add internal/e2e_harness/production/ddl.go && git commit -m "test(e2e): align production harness DDL with init-db (#174)"`

### Task 3: Extend `e2e_wide` with the missing EAV attribute types

**Files:**
- Modify: `internal/e2e_harness/production/schemas/e2e_wide.json`
- Modify: `internal/e2e_harness/production/schemas/e2e_wide_attributes.json`
- Modify: `internal/e2e_harness/production/generator.go` (`FullTypeProfile`)

**Interfaces:** Produces: five new EAV attributes usable by Tasks 4–5 — `level` (smallint, attrID 13), `qty` (integer, 14), `total` (bigint, 15), `ratio` (numeric, 16), `token` (uuid, 17). All EAV (no `column_binding`). Existing attrs/IDs are untouched, so `date_parity_e2e_test.go` and the smoke tests keep working.

- [ ] **Step 1:** Add to `e2e_wide.json` `properties`:

```json
    "level": { "type": "integer", "minimum": -32768, "maximum": 32767 },
    "qty": { "type": "integer" },
    "total": { "type": "integer" },
    "ratio": { "type": "number" },
    "token": { "type": "string", "format": "uuid" }
```

- [ ] **Step 2:** Add to `e2e_wide_attributes.json` (after `touched`):

```json
  "level": { "attributeID": 13, "valueType": "smallint" },
  "qty": { "attributeID": 14, "valueType": "integer" },
  "total": { "attributeID": 15, "valueType": "bigint" },
  "ratio": { "attributeID": 16, "valueType": "numeric" },
  "token": { "attributeID": 17, "valueType": "uuid" }
```

- [ ] **Step 3:** In `generator.go` `FullTypeProfile`, inside the `if partial { return attrs }`-guarded full branch, add:

```go
		attrs["level"] = float64(r.Intn(200) - 100)
		attrs["qty"] = float64(ordinal*7 + r.Intn(7))
		attrs["total"] = float64(r.Int63n(1_000_000_000))
		attrs["ratio"] = float64(r.Intn(4000)) / 4 // .25 steps stay float-exact
		attrs["token"] = deterministicUUID(r).String()
```

- [ ] **Step 4:** Run `make test` (untagged oracle/generator/schema tests) — expected PASS — then the full production e2e suite — expected PASS (existing tests must be unaffected; the oracle only normalizes attrs that events actually set, and `compareAttributes` yields nil==nil for unset attrs).
- [ ] **Step 5:** Commit: `git add internal/e2e_harness/production/schemas/e2e_wide.json internal/e2e_harness/production/schemas/e2e_wide_attributes.json internal/e2e_harness/production/generator.go && git commit -m "test(e2e): add EAV numeric-family and uuid attrs to e2e_wide fixture (#174)"`

### Task 4: Full-type three-tier round-trip test (typical values)

**Files:**
- Create: `internal/e2e_harness/production/full_type_parquet_e2e_test.go` (helpers)
- Create: `internal/e2e_harness/production/full_type_roundtrip_e2e_test.go` (test)

**Interfaces:**
- Consumes: Task 3 fixture; harness API listed in Verified Context.
- Produces: `wideParquetTypes map[string]string`, `wideTruth(t, events) map[uuid.UUID]*wideVals`, `assertWideParquetSchema(ctx, t, env, key, tier)`, `assertWideParquetValues(ctx, t, env, key, tier, truth) int` — reused by Task 5.

- [ ] **Step 1:** Write the helpers file:

```go
//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// wideParquetTypes pins the physical parquet schema of e2e_wide exports.
// Attribute types come from cdc.duckTypeForValue / castMainValue /
// castEAVValue (internal/cdc/duckdb_exporter.go); note the deliberate
// asymmetry: bound uuid (ref) is physical UUID, EAV uuid (token) is VARCHAR.
var wideParquetTypes = map[string]string{
	"schema_id": "SMALLINT", "row_id": "UUID",
	"changed_at": "BIGINT", "deleted_at": "BIGINT",
	"ltbase_created_at": "BIGINT", "ltbase_updated_at": "BIGINT", "ltbase_deleted_at": "BIGINT",
	"title": "VARCHAR", "rank": "SMALLINT", "count": "INTEGER", "amount": "BIGINT",
	"score": "DOUBLE", "ref": "UUID", "joined": "BIGINT", "touched": "BIGINT",
	"note": "VARCHAR", "active": "BOOLEAN", "born": "BIGINT", "seen": "BIGINT",
	"level": "SMALLINT", "qty": "INTEGER", "total": "BIGINT", "ratio": "DOUBLE", "token": "VARCHAR",
}

// wideVals is the parquet-physical expected row: pointers are nil for NULL.
type wideVals struct {
	title, note, ref, token          *string
	rank                             *int16
	count                            *int32
	amount, joined, touched          *int64
	born, seen                       *int64
	score, ratio                     *float64
	active                           *bool
}

// wideTruth derives per-row expected parquet values straight from event
// attributes — independent of every storage path (create-only scripts).
func wideTruth(t *testing.T, events []*Event) map[uuid.UUID]*wideVals {
	t.Helper()
	truth := make(map[uuid.UUID]*wideVals, len(events))
	for _, ev := range events {
		if ev.Kind != EventCreate {
			t.Fatalf("wideTruth only supports create-only scripts, got %s", ev.Kind)
		}
		a := ev.Attrs
		v := &wideVals{
			title: strAttr(t, a, "title"), note: strAttr(t, a, "note"),
			ref: uuidAttr(t, a, "ref"), token: uuidAttr(t, a, "token"),
			rank: int16Attr(t, a, "rank"), level: int16Attr(t, a, "level"),
			count: int32Attr(t, a, "count"), qty: int32Attr(t, a, "qty"),
			amount: int64Attr(t, a, "amount"), total: int64Attr(t, a, "total"),
			score: f64Attr(t, a, "score"), ratio: f64Attr(t, a, "ratio"),
			active: boolAttr(t, a, "active"),
			born: dateMSAttr(t, a, "born"), joined: dateMSAttr(t, a, "joined"),
			seen: datetimeMSAttr(t, a, "seen"), touched: datetimeMSAttr(t, a, "touched"),
		}
		truth[ev.RowID] = v
	}
	return truth
}
```

(the `wideVals` struct must therefore also carry `level *int16`, `qty *int32`, `total *int64` — final struct has all 17 attrs.)

The extraction helpers, written out completely:

```go
func strAttr(t *testing.T, a map[string]any, k string) *string {
	if raw, ok := a[k]; ok {
		s, isStr := raw.(string)
		if !isStr {
			t.Fatalf("attr %s is %T, want string", k, raw)
		}
		return &s
	}
	return nil
}

func uuidAttr(t *testing.T, a map[string]any, k string) *string {
	s := strAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := uuid.Parse(*s)
	if err != nil {
		t.Fatalf("attr %s = %q is not a uuid: %v", k, *s, err)
	}
	canon := parsed.String()
	return &canon
}

func f64Attr(t *testing.T, a map[string]any, k string) *float64 {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case float64:
		return &v
	case int64:
		f := float64(v)
		return &f
	default:
		t.Fatalf("attr %s is %T, want numeric", k, raw)
		return nil
	}
}

func int16Attr(t *testing.T, a map[string]any, k string) *int16 {
	f := f64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int16(*f)
	return &v
}

func int32Attr(t *testing.T, a map[string]any, k string) *int32 {
	f := f64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int32(*f)
	return &v
}

// int64Attr keeps exact int64s exact: int64 passes through, float64 converts.
func int64Attr(t *testing.T, a map[string]any, k string) *int64 {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case int64:
		return &v
	case float64:
		i := int64(v)
		return &i
	default:
		t.Fatalf("attr %s is %T, want numeric", k, raw)
		return nil
	}
}

func boolAttr(t *testing.T, a map[string]any, k string) *bool {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	b, isBool := raw.(bool)
	if !isBool {
		t.Fatalf("attr %s is %T, want bool", k, raw)
	}
	return &b
}

func dateMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := strAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := time.ParseInLocation("2006-01-02", *s, time.UTC)
	if err != nil {
		t.Fatalf("attr %s = %q: %v", k, *s, err)
	}
	ms := parsed.UnixMilli()
	return &ms
}

func datetimeMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := strAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		t.Fatalf("attr %s = %q: %v", k, *s, err)
	}
	ms := parsed.UnixMilli()
	return &ms
}
```

- [ ] **Step 2:** Add the schema assertion (same file):

```go
// assertWideParquetSchema pins the physical column set + types (#174
// hypothesis 1: the reader's changed_at expectation is met by the exporter).
func assertWideParquetSchema(ctx context.Context, t *testing.T, env *Env, key, tier string) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		t.Fatalf("%s parquet describe: %v", tier, err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, key2, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key2, &def, &extra); err != nil {
			t.Fatalf("%s describe scan: %v", tier, err)
		}
		got[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s describe rows: %v", tier, err)
	}
	for col, want := range wideParquetTypes {
		if got[col] != want {
			t.Errorf("%s parquet column %q = %q, want %q", tier, col, got[col], want)
		}
	}
	for col := range got {
		if _, ok := wideParquetTypes[col]; !ok {
			t.Errorf("%s parquet has unexpected column %q (type %s)", tier, col, got[col])
		}
	}
}
```

- [ ] **Step 3:** Add the value assertion (same file). Quote `"count"` (DuckDB keyword-safe) and cast UUIDs to VARCHAR for scanning:

```go
// assertWideParquetValues reads every attribute column of one parquet file
// and compares row-by-row against the event-derived truth. Returns the row
// count so callers can assert per-tier coverage.
func assertWideParquetValues(ctx context.Context, t *testing.T, env *Env, key, tier string, truth map[uuid.UUID]*wideVals) int {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT CAST(row_id AS VARCHAR), "title", "rank", "count", "amount", "score",
		        CAST("ref" AS VARCHAR), "joined", "touched", "note", "active",
		        "born", "seen", "level", "qty", "total", "ratio", "token"
		 FROM read_parquet('%s')`, path))
	if err != nil {
		t.Fatalf("%s parquet scan: %v", tier, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var rowIDStr string
		var title, ref, note, token sql.NullString
		var rank, level sql.NullInt16
		var count, qty sql.NullInt32
		var amount, joined, touched, born, seen, total sql.NullInt64
		var score, ratio sql.NullFloat64
		var active sql.NullBool
		if err := rows.Scan(&rowIDStr, &title, &rank, &count, &amount, &score,
			&ref, &joined, &touched, &note, &active,
			&born, &seen, &level, &qty, &total, &ratio, &token); err != nil {
			t.Fatalf("%s parquet row scan: %v", tier, err)
		}
		rowID, err := uuid.Parse(rowIDStr)
		if err != nil {
			t.Fatalf("%s parquet row_id %q: %v", tier, rowIDStr, err)
		}
		want, ok := truth[rowID]
		if !ok {
			t.Fatalf("%s parquet holds unknown row %s", tier, rowID)
		}
		checkStr(t, tier, rowID, "title", title, want.title)
		checkStr(t, tier, rowID, "note", note, want.note)
		checkStr(t, tier, rowID, "ref", ref, want.ref)
		checkStr(t, tier, rowID, "token", token, want.token)
		checkI16(t, tier, rowID, "rank", rank, want.rank)
		checkI16(t, tier, rowID, "level", level, want.level)
		checkI32(t, tier, rowID, "count", count, want.count)
		checkI32(t, tier, rowID, "qty", qty, want.qty)
		checkI64(t, tier, rowID, "amount", amount, want.amount)
		checkI64(t, tier, rowID, "joined", joined, want.joined)
		checkI64(t, tier, rowID, "touched", touched, want.touched)
		checkI64(t, tier, rowID, "born", born, want.born)
		checkI64(t, tier, rowID, "seen", seen, want.seen)
		checkI64(t, tier, rowID, "total", total, want.total)
		checkF64(t, tier, rowID, "score", score, want.score)
		checkF64(t, tier, rowID, "ratio", ratio, want.ratio)
		checkBool(t, tier, rowID, "active", active, want.active)
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s parquet rows: %v", tier, err)
	}
	return n
}
```

Write the five `check*` helpers with one shared shape (NULL must match nil, value must match value; report both directions):

```go
func checkStr(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullString, want *string) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %q, want NULL", tier, rowID, col, got.String)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %q", tier, rowID, col, *want)
	case want != nil && got.String != *want:
		t.Errorf("%s %s.%s = %q, want %q", tier, rowID, col, got.String, *want)
	}
}
```

(`checkI16`/`checkI32`/`checkI64`/`checkF64`/`checkBool` are the identical pattern over `sql.NullInt16/NullInt32/NullInt64/NullFloat64/NullBool` — write each out fully; float comparison is `==`, all fixture values are float-exact by construction.)

- [ ] **Step 4:** Write the test file:

```go
//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestFullTypeRoundTripAcrossTiers proves issue #174's success criteria for
// typical values: every supported scalar type — bound and EAV — survives
// write → CDC export → parquet → federated merge-on-read intact, across all
// three tiers. Physical layer: exact parquet column set/types + row values
// vs event-derived truth. Logical layer: AssertQueryMatches compares every
// attribute of every row against the independent oracle, hot and federated.
func TestFullTypeRoundTripAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	// Hot baseline: full-attribute oracle comparison from Postgres.
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})

	// Cold tier: base parquet becomes the only source for creates[0:6].
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(creates[0:6]))

	// Warm tier: flush creates[6:12] into a delta file.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Hot tier: two more creates that stay unflushed.
	hotCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, hotCreates...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// Physical layer: schema + values of every parquet file.
	truth := wideTruth(t, creates)
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	tierRows := map[string]int{}
	for _, f := range m.Files {
		assertWideParquetSchema(ctx, t, env, f.Path, f.Tier)
		tierRows[f.Tier] += assertWideParquetValues(ctx, t, env, f.Path, f.Tier, truth)
	}
	if tierRows["base"] != 12 {
		t.Errorf("base parquet rows = %d, want 12", tierRows["base"])
	}
	if tierRows["delta"] != 6 {
		t.Errorf("delta parquet rows = %d, want 6", tierRows["delta"])
	}

	// Logical layer: federated merge read equals the oracle, row for row,
	// attribute for attribute, and actually routes through DuckDB.
	fed := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if fed == nil {
		return // AssertQueryMatches already failed the test
	}
	if !fed.Plan.Routing.UseDuckDB {
		t.Errorf("federated query did not route to duckdb: %+v", fed.Plan.Routing)
	}
	if len(fed.Records) != len(creates)+len(hotCreates) {
		t.Errorf("federated rows = %d, want %d", len(fed.Records), len(creates)+len(hotCreates))
	}
}
```

- [ ] **Step 5:** Run it: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -run TestFullTypeRoundTripAcrossTiers -timeout=10m`.
  Expected: PASS, **or** a small set of `wideParquetTypes` mismatches on system columns (e.g. `schema_id`/`deleted_at` physical width depends on the `postgres_query` mapping). Triage rule for every mismatch: read the exporter (`internal/cdc/duckdb_exporter.go`, `export_sql_builder.go`) and the reader (`internal/sqlgen/duckdb_schema_projection.go`); if exporter and reader agree and only the pinned expectation is wrong → fix the table in the test; if exporter and reader *disagree* → that is a production bug in the #194 family: stop, file an issue with both code references, and `t.Skipf` the affected assertion referencing it. Do not "fix" production code inline.
- [ ] **Step 6:** Sabotage check (guards against a test that can't fail — the epic's #125 lesson): temporarily change `"active": "BOOLEAN"` to `"VARCHAR"` in `wideParquetTypes` and one truth value (e.g. negate `want.active` handling), rerun, confirm both produce failures, then revert. Expected: FAIL then PASS after revert.
- [ ] **Step 7:** Run the whole production suite once (regression) + `make lint`. Expected: PASS. Check both new files are under 500 lines; if the helpers file exceeds, move the `check*` helpers into `full_type_checks_e2e_test.go`.
- [ ] **Step 8:** Commit: `git add internal/e2e_harness/production/full_type_roundtrip_e2e_test.go internal/e2e_harness/production/full_type_parquet_e2e_test.go && git commit -m "test(e2e): full-type CDC round-trip across tiers (#174)"`

### Task 5: NULL and boundary-value round-trip test

**Files:**
- Create: `internal/e2e_harness/production/boundary_roundtrip_e2e_test.go`

**Interfaces:** Consumes Task 4 helpers (`wideTruth`, `assertWideParquetValues`) and Task 3 attrs.

- [ ] **Step 1 (probe — decides the bigint boundary):** Write a minimal probe inside the new test file first:

```go
//go:build e2e

package production

import (
	"context"
	"math"
	"testing"

	"github.com/google/uuid"
)

// maxBoundBigint is the boundary asserted for the bound bigint column.
// Preferred: math.MaxInt64 (true BIGINT end to end). If the write path
// rejects int64 payloads (probe below), fall back to 1<<62 — a power of
// two that survives float64 — and record the finding in the PR.
var maxBoundBigint = int64(math.MaxInt64)

func TestBoundBigintAcceptsInt64Probe(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title":  "int64-probe",
		"amount": int64(math.MaxInt64),
	})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("write path rejected int64 payload: %v", err)
	}
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	got, ok := hot.Records[0].Int64Items["bigint_01"]
	if !ok || got != math.MaxInt64 {
		t.Fatalf("bound bigint stored %d (present=%t), want MaxInt64", got, ok)
	}
}
```

Run just this probe. If it fails at the write or storage step: change `maxBoundBigint` to `int64(1) << 62`, adjust the probe's expectation, keep the probe as documentation of the contract, and note the finding for the PR body (production JSON APIs marshal numbers as float64, so MaxInt64 may be unrepresentable by design — that is an *answer* to the issue's "max int64" bullet, not a failure).

- [ ] **Step 2:** Write the boundary matrix test in the same file. Hand-crafted create events (no generator), placed deliberately per tier:

```go
const zeroUUID = "00000000-0000-0000-0000-000000000000"
const maxUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// float64-exact EAV bigint bound: EAV values travel as float64 through the
// Go model (transform.extractValueFromEAVRecord), so 2^53 is the largest
// exactly-representable EAV integer. Bound bigints don't share this limit.
const maxEAVInt = float64(1 << 53)

func boundaryEvents(wide SchemaRef) (nullRow, zeroRow, maxRow, minRow *Event) {
	nullRow = CreateEvent(wide, map[string]any{"title": "b-null"})
	zeroRow = CreateEvent(wide, map[string]any{
		"title": "", "note": "", "ref": zeroUUID, "token": zeroUUID,
		"rank": float64(0), "count": float64(0), "amount": float64(0),
		"score": float64(0), "level": float64(0), "qty": float64(0),
		"total": float64(0), "ratio": float64(0), "active": false,
		"born": "1970-01-01", "joined": "1970-01-01",
		"seen": "1970-01-01T00:00:00Z", "touched": "1970-01-01T00:00:00Z",
	})
	maxRow = CreateEvent(wide, map[string]any{
		"title": "标题-🍋-boundary-max", "note": "ünïcode ✓ 空白  两个空格",
		"ref": maxUUID, "token": maxUUID,
		"rank": float64(32767), "count": float64(2147483647),
		"amount": maxBoundBigint,
		"score": math.MaxFloat64, "level": float64(32767),
		"qty": float64(2147483647), "total": maxEAVInt,
		"ratio": float64(1048576.25), "active": true,
		"born": "9999-12-31", "joined": "9999-12-31",
		"seen": "9999-12-31T23:59:59Z", "touched": "9999-12-31T23:59:59Z",
	})
	minRow = CreateEvent(wide, map[string]any{
		"title": "b-min", "note": "b-min",
		"rank": float64(-32768), "count": float64(-2147483648),
		"amount": -maxBoundBigint,
		"score": -math.MaxFloat64, "level": float64(-32768),
		"qty": float64(-2147483648), "total": -maxEAVInt,
		"ratio": float64(-1048576.25), "active": false,
		// pre-epoch: negative epoch-ms must survive every tier
		"born": "1900-01-01", "joined": "1900-01-01",
		"seen": "1900-01-01T00:00:00Z", "touched": "1900-01-01T00:00:00Z",
	})
	return
}
```

Test choreography (mirror Task 4; write it out fully in the file):

```go
func TestNullAndBoundaryRoundTripAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	nullRow, zeroRow, maxRow, minRow := boundaryEvents(wide)
	batch := []*Event{nullRow, zeroRow, maxRow, minRow}
	if err := env.ApplyEvents(ctx, batch...); err != nil {
		t.Fatalf("apply boundary creates: %v", err)
	}

	// Hot baseline: oracle parity + exact typed assertions.
	hot := env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
	assertBoundaryRecords(t, "hot", hot, nullRow, zeroRow, maxRow, minRow)

	// Tier split: null+max cold-only, zero+min warm.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs([]*Event{nullRow, maxRow}))
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Hot tier: fresh copies of the null and max shapes stay unflushed.
	hotNull, _, hotMax, _ := boundaryEvents(wide)
	if err := env.ApplyEvents(ctx, hotNull, hotMax); err != nil {
		t.Fatalf("apply hot boundary creates: %v", err)
	}

	// Physical layer: reuse Task 4's per-file value assertion (NULL columns
	// included), then the NULL-vs-zero distinction per boundary row.
	truth := wideTruth(t, append(batch, hotNull, hotMax))
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	for _, f := range m.Files {
		assertWideParquetValues(ctx, t, env, f.Path, f.Tier, truth)
	}

	// Logical layer: federated merge equals oracle; exact typed spot checks.
	fed := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if fed == nil {
		return
	}
	assertBoundaryRecords(t, "federated", fed, nullRow, zeroRow, maxRow, minRow, hotNull, hotMax)
}
```

`assertBoundaryRecords` does the float64-proof exactness the oracle can't (write it out fully; the shape below is the contract):

```go
// assertBoundaryRecords asserts exact typed values that the oracle's
// float64 normalization cannot distinguish (MaxInt64) and the null/zero/
// empty-string distinctions.
func assertBoundaryRecords(t *testing.T, label string, res *QueryResult, evs ...*Event) {
	t.Helper()
	recs := map[uuid.UUID]*model.PersistentRecord{}
	for _, r := range res.Records {
		recs[r.RowID] = r
	}
	for _, ev := range evs {
		rec := recs[ev.RowID]
		if rec == nil {
			t.Errorf("%s: missing boundary row %s (%s)", label, ev.RowID, ev.Attrs["title"])
			continue
		}
		switch ev.Attrs["title"] {
		case "b-null":
			// only text_01 set; every other bound column absent, no EAV rows
			if _, ok := rec.Int64Items["bigint_01"]; ok {
				t.Errorf("%s b-null: amount present, want absent", label)
			}
			if len(rec.OtherAttributes) != 0 {
				t.Errorf("%s b-null: %d EAV attrs, want 0", label, len(rec.OtherAttributes))
			}
		case "":
			// empty string is a value, not NULL
			v, ok := rec.TextItems["text_01"]
			if !ok || v != "" {
				t.Errorf("%s b-zero: title = %q (present=%t), want empty string", label, v, ok)
			}
		case "标题-🍋-boundary-max":
			if got := rec.Int64Items["bigint_01"]; got != maxBoundBigint {
				t.Errorf("%s b-max: amount = %d, want %d", label, got, maxBoundBigint)
			}
			if got := rec.UUIDItems["uuid_01"]; got.String() != maxUUID {
				t.Errorf("%s b-max: ref = %s, want %s", label, got, maxUUID)
			}
			assertEAVNumeric(t, label+" b-max", rec, 15, maxEAVInt)      // total
			assertEAVText(t, label+" b-max", rec, 17, maxUUID)           // token
		case "b-min":
			if got := rec.Int64Items["bigint_01"]; got != -maxBoundBigint {
				t.Errorf("%s b-min: amount = %d, want %d", label, got, -maxBoundBigint)
			}
			assertEAVNumeric(t, label+" b-min", rec, 15, -maxEAVInt)
			// pre-epoch date: negative epoch-ms
			if got := rec.Int64Items["bigint_02"]; got != -2208988800000 {
				t.Errorf("%s b-min: joined = %d, want -2208988800000", label, got)
			}
		}
	}
}

func assertEAVNumeric(t *testing.T, label string, rec *model.PersistentRecord, attrID int16, want float64) {
	t.Helper()
	for _, a := range rec.OtherAttributes {
		if a.AttrID == attrID {
			if a.ValueNumeric == nil || *a.ValueNumeric != want {
				t.Errorf("%s: eav attr %d numeric = %v, want %v", label, attrID, a.ValueNumeric, want)
			}
			return
		}
	}
	t.Errorf("%s: eav attr %d missing", label, attrID)
}

func assertEAVText(t *testing.T, label string, rec *model.PersistentRecord, attrID int16, want string) {
	t.Helper()
	for _, a := range rec.OtherAttributes {
		if a.AttrID == attrID {
			if a.ValueText == nil || *a.ValueText != want {
				t.Errorf("%s: eav attr %d text = %v, want %q", label, attrID, a.ValueText, want)
			}
			return
		}
	}
	t.Errorf("%s: eav attr %d missing", label, attrID)
}
```

(Import `"github.com/lychee-technology/forma/internal/model"`. If federated records surface EAV values differently than hot records — e.g. attr values folded into typed maps — adapt the two `assertEAV*` helpers after the first run, but the *expected values* must not change.)

- [ ] **Step 3:** Run the test. Expected outcomes and triage:
  - PASS → done.
  - `score: math.MaxFloat64` diverging anywhere → likely a decimal text-representation hop in `postgres_query`; if exporter+reader agree it's expectation error, relax to `1e300` with a comment naming where the hop is; if they disagree → file an issue (do not fix inline).
  - Any NULL arriving as 0 (or '' as NULL) in warm/cold but not hot → production bug in the export path → file an issue with the failing column + code reference, `t.Skipf` that assertion referencing the issue, keep the rest green. This is exactly the class of finding #174 exists to surface.
- [ ] **Step 4:** Sabotage check: temporarily change `-2208988800000` to `-2208988800001`, confirm FAIL, revert, confirm PASS.
- [ ] **Step 5:** Full production suite + `make lint`. Expected: PASS.
- [ ] **Step 6:** Commit: `git add internal/e2e_harness/production/boundary_roundtrip_e2e_test.go && git commit -m "test(e2e): NULL and boundary-value CDC round-trip (#174)"`

### Task 6: List-type probe → coverage or follow-up issue

**Files:**
- Modify: `internal/e2e_harness/production/schemas/e2e_wide.json` + `e2e_wide_attributes.json` (add `tags`)
- Create: `internal/e2e_harness/production/list_roundtrip_e2e_test.go`

**Interfaces:** Produces: `tags` attr (attrID 18, valueType `list`, EAV). CRITICAL: the oracle (`production/oracle.go` `normalizeValue`) does **not** support `list` — any event carrying `tags` must never flow through `AssertQueryMatches`/`ExpectedState`. This test uses `env.Query` directly and its own `Env`, so other tests are unaffected (their events never set `tags`).

- [ ] **Step 1:** Fixture additions — `e2e_wide.json`:

```json
    "tags": { "type": "array", "items": { "type": "string" } }
```

`e2e_wide_attributes.json`:

```json
  "tags": { "attributeID": 18, "valueType": "list" }
```

Run the full production suite immediately: adding a never-set attribute must not break existing tests (`compareAttributes` yields nil==nil). If it does break — e.g. `BuildSchemaProjection` chokes on `list` — that itself is a finding: capture it, remove the fixture change, and fold the finding into the follow-up issue below.

- [ ] **Step 2:** Write the probe:

```go
//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestListAttributeRoundTrip probes issue #174 hypothesis 3: do list
// (array) attributes survive the CDC → parquet → federated path? Static
// analysis says no: the CDC EAV export drops array_indices and collapses
// multi-row attributes via MAX(CASE ...) (internal/cdc/export_sql_builder.go:79-137),
// and the reader hardcodes array_indices='' (internal/sqlgen/duckdb_schema_projection.go:328).
// The oracle has no list support, so this test asserts directly.
func TestListAttributeRoundTrip(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title": "list-probe",
		"tags":  []any{"alpha", "beta", "gamma"},
	})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("write path rejected list payload: %v", err)
	}

	// Hot: the EAV rows must carry all three elements with array indices.
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	hotTags := collectListValues(hot, 18)
	if len(hotTags) != 3 {
		t.Fatalf("hot list values = %v, want 3 elements", hotTags)
	}

	// Warm: flush, then inspect the raw parquet column and federated read.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	for _, f := range manifests[wide.ID].Files {
		path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(f.Path, "/"))
		var tags any
		if err := env.Duck.DB.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT "tags" FROM read_parquet('%s')`, path)).Scan(&tags); err != nil {
			t.Fatalf("parquet tags scan: %v", err)
		}
		t.Logf("%s parquet tags column = %#v", f.Tier, tags)
	}

	fed, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	fedTags := collectListValues(fed, 18)
	if len(fedTags) != 3 {
		t.Fatalf("federated list values = %v, want the 3 hot-identical elements", fedTags)
	}
}

func collectListValues(res *QueryResult, attrID int16) []string {
	var out []string
	for _, rec := range res.Records {
		for _, a := range rec.OtherAttributes {
			if a.AttrID == attrID && a.ValueText != nil {
				out = append(out, *a.ValueText)
			}
		}
	}
	return out
}
```

- [ ] **Step 3:** Run it and branch on the outcome:
  - **Expected outcome (broken):** hot works, federated/parquet collapses to one element (or the write path itself rejects lists). File a follow-up issue on `Lychee-Technology/forma`:
    - Title: `cdc+sqlgen: list attributes do not survive CDC export / federated read (array_indices dropped, MAX-collapse)`
    - Body must include: probe output; producer refs `internal/cdc/export_sql_builder.go:79-88` (no `array_indices` in the EAV select) and `:135` (`MAX(CASE ...)` collapse); consumer refs `internal/sqlgen/duckdb_schema_projection.go:328,691` (hardcoded `array_indices: ''`); note that `production/README.md` already lists list as uncovered; label `federated-query`; reference epic #172 and issue #174 ("surfaced by the #174 round-trip suite, same pattern as #192–#198").
    - Then gate the broken half of the test with `t.Skipf("warm/cold list round-trip blocked by #<NNN>")` placed *after* the hot-tier assertions (hot coverage stays live), and pin the hot behavior as the regression contract.
  - **Unexpected outcome (works):** keep the full test as-is, extend `wideParquetTypes` with the observed `tags` physical type, and note in the PR that hypothesis 3 was rejected.
- [ ] **Step 4:** Full production suite + `make test` + `make lint`. Expected: PASS (with the skip in place if broken).
- [ ] **Step 5:** Commit: `git add internal/e2e_harness/production/schemas/e2e_wide.json internal/e2e_harness/production/schemas/e2e_wide_attributes.json internal/e2e_harness/production/list_roundtrip_e2e_test.go && git commit -m "test(e2e): probe list-attribute CDC round-trip (#174)"`

### Task 7: Docs, final verification, PR

**Files:**
- Modify: `internal/e2e_harness/production/README.md`

- [ ] **Step 1:** Update the README's coverage notes: full-type matrix now covered (list all 17 scalar attrs bound/EAV split); NULL/boundary semantics covered; `list` status = covered-hot / blocked-by-#NNN (or covered, per Task 6 outcome); note the DDL alignment (Task 2) or its follow-up issue.
- [ ] **Step 2:** Full verification, in order: `make lint`, `make test`, `make test-e2e-production` (or the explicit go test command). All must PASS. Paste outputs into the PR description's verification section — claims without command output don't count.
- [ ] **Step 3:** Push branch, open PR:
  - Title: `test(e2e): CDC full-type producer-consumer round-trip (#174)`
  - Body: `Closes #174`; summary of the two new test files + fixture/DDL changes; a **hypothesis verdict table** — (1) `time_slot` vs `changed_at`: refuted, pinned by the parquet schema assertion; (2) EAV numerics from `value_text`: refuted, pinned by the EAV numeric-family round-trip; (3) list round-trip: verdict per Task 6 with the follow-up issue link; the bound-bigint MaxInt64 finding from Task 5 Step 1; any issues filed along the way. End with the standard Claude Code attribution line.
- [ ] **Step 4:** Comment on epic #172 noting #174's PR is up and listing any newly filed follow-up issues so they can be woven into the execution order (the epic maintainer keeps that list current).

---

## Self-Review Notes (done at planning time)

- **Spec coverage:** all-types incl. bound+EAV → Tasks 3–4; NULL + boundary values (max int64, empty string, zero UUID, epoch dates) → Task 5; production `cdc-flush` → `RunInit`/`RunFlush` wrap the real `internal/cdc` runner; parquet schema/physical-type/value inspection → Task 4 helpers; production federated query + field-level match → `AssertQueryMatches` (oracle) + typed exactness helpers; hypothesis 1 & 2 pinned by tests, hypothesis 3 → Task 6.
- **Known deltas from the issue text:** "epoch dates" covered as epoch-zero AND pre-epoch negative values; "max int64" is answered honestly (bound column exact; EAV bounded at 2^53 by the float64 model; API acceptance probed first). "object attributes" don't exist as a Forma type — `list` is the array type; stated in PR body.
- **Type consistency check:** attr IDs 13–17 (Task 3) match `assertEAVNumeric(..., 15, ...)`/`assertEAVText(..., 17, ...)` (Task 5) and attrID 18 (Task 6); `wideVals` fields match the scan order in `assertWideParquetValues`; `maxBoundBigint`/`maxEAVInt`/`zeroUUID`/`maxUUID` defined once in Task 5's file and used only there.
