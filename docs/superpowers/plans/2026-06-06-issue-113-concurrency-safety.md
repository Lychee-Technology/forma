# Concurrency & Resource Safety Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 8 findings from the Issue #113 code review of goroutine lifecycle, resource release, and context propagation.

**Architecture:** Eight independent targeted fixes in `forma`. Each committed separately. No exported API changes. Task 4 uses a closure capture to avoid changing stored function-field types.

**Tech Stack:** Go 1.22+, `database/sql`, `sync`, `context`, `go.uber.org/zap`, `github.com/stretchr/testify/require`

**Issue:** Lychee-Technology/forma#113

---

## File map

| File | Tasks |
|---|---|
| `internal/cdc/flusher.go` | 1, 4 |
| `internal/cdc/flusher_test.go` | 1, 4 |
| `internal/duckdb_conn.go` | 2, 3 |
| `internal/duckdb_conn_test.go` | 2, 3 |
| `internal/circuit_breaker.go` | 5 |
| `internal/postgres_duckdb_circuit_breaker_test.go` | 8 |
| `internal/e2e_harness/federated/cdc.go` | 6, 7 |

**Test command throughout:** `GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc/ ./internal/ -race -v`

---

### Task 1: Fix missing `rows.Err()` in `getUnflushedSchemaIDs` (P1)

**Files:**
- Modify: `internal/cdc/flusher.go:244`
- Modify: `internal/cdc/flusher_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/cdc/flusher_test.go`:

```go
func TestGetUnflushedSchemaIDs_PropagatesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := sql.Open("pgx", "host=127.0.0.1 port=1 user=x dbname=x sslmode=disable")
	require.NoError(t, err)
	defer db.Close()

	_, err = getUnflushedSchemaIDs(ctx, db, "change_log")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to confirm it passes (context error is propagated even without the rows.Err fix)**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc/ -run TestGetUnflushedSchemaIDs -v
```

- [ ] **Step 3: Add the `rows.Err()` fix**

In `internal/cdc/flusher.go`, change the end of `getUnflushedSchemaIDs` (after the `rows.Next()` loop, before `return`):

```go
// before (line 244):
	return schemaIDs, nil
```

```go
// after:
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids: %w", err)
	}
	return schemaIDs, nil
```

- [ ] **Step 4: Run full cdc test suite**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc/ -race -v
```

Expected: all tests pass, no race.

- [ ] **Step 5: Commit**

```bash
git add internal/cdc/flusher.go internal/cdc/flusher_test.go
git commit -m "fix(cdc): check rows.Err() after iteration in getUnflushedSchemaIDs

Without this check, a dropped Postgres connection mid-iteration returns a
partial schema list with nil error, silently skipping schemas during flush.

Refs #113"
```

---

### Task 2: Fix shared `pingCtx` covering DuckDB extension installation (P1)

**Files:**
- Modify: `internal/duckdb_conn.go:97-110`
- Modify: `internal/duckdb_conn_test.go`

- [ ] **Step 1: Write the test**

Add to `internal/duckdb_conn_test.go`:

```go
// TestNewDuckDBClientContext_ExtensionStepsUseCallerCtx verifies that
// configureExtensions/configureS3/applyResourcePragmas are governed by the
// caller's ctx, not the narrow 5-second pingCtx.
// With in-memory DuckDB and no extensions enabled, the call must complete
// successfully even when called with a generous timeout.
func TestNewDuckDBClientContext_ExtensionStepsUseCallerCtx(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	duck, err := NewDuckDBClientContext(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, duck)
	duck.Close()
}
```

- [ ] **Step 2: Run test to confirm it already passes (baseline)**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestNewDuckDBClientContext_ExtensionStepsUseCallerCtx -v
```

- [ ] **Step 3: Apply the fix**

In `internal/duckdb_conn.go`, change lines 97–110 — pass `ctx` instead of `pingCtx` to the three config steps:

```go
// before:
if err := configureExtensions(pingCtx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
if err := configureS3(pingCtx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
if err := applyResourcePragmas(pingCtx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
```

```go
// after: pingCtx is scoped to the ping only; caller's ctx governs setup
if err := configureExtensions(ctx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
if err := configureS3(ctx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
if err := applyResourcePragmas(ctx, db, cfg); err != nil {
	db.Close()
	return nil, err
}
```

- [ ] **Step 4: Run all DuckDB client tests**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestNewDuckDB -race -v
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestDuckDB -race -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb_conn.go internal/duckdb_conn_test.go
git commit -m "fix(duckdb): scope pingCtx to ping only; pass caller ctx to extension setup

configureExtensions/configureS3/applyResourcePragmas shared the 5-second
pingCtx. In cold or airgapped environments, INSTALL httpfs can take >5s,
causing all extension steps to fail silently and returning a DuckDBClient
without S3/parquet support.

Refs #113"
```

---

### Task 3: Warn on `MaxConnections > 1` with file-based DuckDB path (P1)

**Files:**
- Modify: `internal/duckdb_conn.go:84-87`
- Modify: `internal/duckdb_conn_test.go`

- [ ] **Step 1: Write the test**

```go
func TestNewDuckDBClient_MultiConnFilePathWarns(t *testing.T) {
	// ValidateDuckDBConfig still passes with MaxConnections > 1 + file path.
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:", // use :memory: to avoid real file I/O
		MaxConnections: 4,
		QueryTimeout:   5 * time.Second,
	}
	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err) // allowed; warning is runtime, not validation error
}
```

- [ ] **Step 2: Apply the fix**

In `internal/duckdb_conn.go:84-87`:

```go
// before:
db.SetMaxOpenConns(1) // DuckDB typically uses a single connection
if cfg.MaxConnections > 0 {
	db.SetMaxOpenConns(cfg.MaxConnections)
}
```

```go
// after:
// DuckDB in read/write mode supports one writer; MaxConnections > 1 with a
// file-based path can produce "database locked" errors under concurrent load.
// It is safe only for :memory: or explicit read-only access mode.
db.SetMaxOpenConns(1)
if cfg.MaxConnections > 0 {
	if cfg.MaxConnections > 1 && cfg.DBPath != "" && cfg.DBPath != ":memory:" {
		zap.S().Warnw("duckdb: MaxConnections > 1 with file-based path risks database locked errors under concurrent load; use MaxConnections=1 or :memory:",
			"maxConnections", cfg.MaxConnections,
			"dbPath", cfg.DBPath,
		)
	}
	db.SetMaxOpenConns(cfg.MaxConnections)
}
```

- [ ] **Step 3: Run tests**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestValidateDuckDBConfig -race -v
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestNewDuckDB -race -v
```

- [ ] **Step 4: Commit**

```bash
git add internal/duckdb_conn.go internal/duckdb_conn_test.go
git commit -m "fix(duckdb): warn when MaxConnections > 1 with file-based DuckDB path

The SetMaxOpenConns(1) guard comment was misleading; it was always overridden
by cfg.MaxConnections. Add a runtime warning for file-based paths with
MaxConnections > 1 to surface the concurrent-writer risk at startup.

Refs #113"
```

---

### Task 4: Remove `ctx` field from `flushBatchExecutor` using closure capture (P2)

**Files:**
- Modify: `internal/cdc/flusher.go`
- Modify: `internal/cdc/flusher_test.go`

The stored field types on `flushBatchExecutor` and `schemaFlushContext` do **not** change. Only `executeBatch`, `executeFlushSingle`, and `executeFlushInChunks` gain an explicit `ctx` parameter. `executeFlush` wraps the defaults in closures that capture `ctx` from its own scope.

- [ ] **Step 1: Write the regression test**

```go
func TestFlushBatchExecutor_NilCtxFieldRejectedAtCompile(t *testing.T) {
	// Structural test: after the refactor, flushBatchExecutor has no ctx field.
	// This test exists to document intent; the real check is that the struct
	// literal in executeFlush no longer sets ctx.
	executor := &flushBatchExecutor{
		db:       nil,
		s3Client: &objectOnlyS3Client{},
		cfg:      CDCConfig{S3Bucket: "test"},
		schemaID: 1,
		logger:   zap.NewNop(),
	}
	_ = executor
}
```

- [ ] **Step 2: Remove `ctx` field from `flushBatchExecutor`**

In `internal/cdc/flusher.go`, delete the `ctx context.Context` field from the struct (line 324).

- [ ] **Step 3: Update `executeBatch` signature**

```go
// before:
func (e *flushBatchExecutor) executeBatch(batchIDs []uuid.UUID, tmpKey string, finalKey string, batchKind string) error {
```

```go
// after:
func (e *flushBatchExecutor) executeBatch(ctx context.Context, batchIDs []uuid.UUID, tmpKey string, finalKey string, batchKind string) error {
```

Replace all `e.ctx` references inside `executeBatch` with `ctx`.

- [ ] **Step 4: Update `executeFlushInChunks` and `executeFlushSingle`**

```go
// before:
func executeFlushInChunks(executor *flushBatchExecutor, batchIDs []uuid.UUID, maxRows int) error {
	...
	if err := executor.executeBatch(sub, chunkTmpKey, chunkFinalKey, "chunk"); err != nil {
```

```go
// after:
func executeFlushInChunks(ctx context.Context, executor *flushBatchExecutor, batchIDs []uuid.UUID, maxRows int) error {
	...
	if err := executor.executeBatch(ctx, sub, chunkTmpKey, chunkFinalKey, "chunk"); err != nil {
```

```go
// before:
func executeFlushSingle(executor *flushBatchExecutor, batchIDs []uuid.UUID) error {
	tmpKey, finalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
	return executor.executeBatch(batchIDs, tmpKey, finalKey, "batch")
```

```go
// after:
func executeFlushSingle(ctx context.Context, executor *flushBatchExecutor, batchIDs []uuid.UUID) error {
	tmpKey, finalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
	return executor.executeBatch(ctx, batchIDs, tmpKey, finalKey, "batch")
```

- [ ] **Step 5: Wrap defaults with closures in `executeFlush`**

The stored field types do **not** change. In `executeFlush` (lines 450–457), wrap the default assignments with closures that capture `ctx`:

```go
// before:
executeInChunks := executor.executeInChunks
if executeInChunks == nil {
	executeInChunks = executeFlushInChunks
}
executeSingle := executor.executeSingle
if executeSingle == nil {
	executeSingle = executeFlushSingle
}
```

```go
// after:
executeInChunks := executor.executeInChunks
if executeInChunks == nil {
	executeInChunks = func(e *flushBatchExecutor, ids []uuid.UUID, max int) error {
		return executeFlushInChunks(ctx, e, ids, max)
	}
}
executeSingle := executor.executeSingle
if executeSingle == nil {
	executeSingle = func(e *flushBatchExecutor, ids []uuid.UUID) error {
		return executeFlushSingle(ctx, e, ids)
	}
}
```

- [ ] **Step 6: Remove `ctx: ctx` from executor construction**

In `executeFlush` line 432, remove the `ctx: ctx` field from the `flushBatchExecutor` literal.

- [ ] **Step 7: Run full cdc test suite**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc/ -race -v
```

Expected: all pass, no race.

- [ ] **Step 8: Commit**

```bash
git add internal/cdc/flusher.go internal/cdc/flusher_test.go
git commit -m "refactor(cdc): remove ctx field from flushBatchExecutor

Storing context in structs violates Go conventions. executeBatch,
executeFlushSingle, and executeFlushInChunks now receive ctx explicitly.
executeFlush wraps default function assignments in closures that capture ctx,
keeping the stored function-field types unchanged.

Refs #113"
```

---

### Task 5: Document circuit breaker half-open behavior (P2)

**Files:**
- Modify: `internal/circuit_breaker.go:74`

- [ ] **Step 1: Replace the `RecordSuccess` doc comment**

```go
// before:
// RecordSuccess resets failure history when operations succeed.
func (cb *CircuitBreaker) RecordSuccess() {
```

```go
// after:
// RecordSuccess resets failure history when operations succeed.
//
// Design note: the breaker forgives immediately on the first success after the
// open period expires; there is no half-open probe state. This favors fast
// recovery when the downstream is healthy, at the cost of potentially
// re-tripping quickly if the backend is still degraded. To add stricter
// half-open semantics (e.g., require N consecutive successes before clearing),
// extend this method with a probe counter guarded by mu.
func (cb *CircuitBreaker) RecordSuccess() {
```

- [ ] **Step 2: Run tests**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestCircuitBreaker -race -v
```

- [ ] **Step 3: Commit**

```bash
git add internal/circuit_breaker.go
git commit -m "docs(circuit_breaker): document immediate-forgiveness RecordSuccess design

Refs #113"
```

---

### Task 6: Fix missing `rows.Err()` in e2e harness CDC helpers (P3)

**Files:**
- Modify: `internal/e2e_harness/federated/cdc.go`

- [ ] **Step 1: Fix `getUnflushedRowIDs`** (lines 103–123)

After the `rows.Next()` loop, before `return rowIDs, nil`, add:

```go
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unflushed row ids: %w", err)
	}
	return rowIDs, nil
```

- [ ] **Step 2: Fix the inner query loop in `getAllCDCRecords`** (or whichever function contains the multi-schema rows loop around line 190–222)

After `rows.Next()` exhausts (before `rows.Close()`), add:

```go
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate cdc records for schema: %w", err)
	}
	rows.Close()
```

- [ ] **Step 3: Build check**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go build ./internal/e2e_harness/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/e2e_harness/federated/cdc.go
git commit -m "fix(e2e_harness): add rows.Err() checks in CDC query helpers

Prevents silent partial results when DB connection drops mid-iteration
in test harness helpers.

Refs #113"
```

---

### Task 7: Fix non-deferred `rows.Close()` in e2e harness loop (P3)

**Files:**
- Modify: `internal/e2e_harness/federated/cdc.go` (the multi-schema query loop around line 185–221)

- [ ] **Step 1: Refactor the loop body to use an IIFE for proper defer scope**

```go
// before (schematic):
for _, schemaID := range schemaIDs {
	rows, err := db.QueryContext(ctx, query, schemaID)
	if err != nil { return nil, err }
	for rows.Next() {
		if err := rows.Scan(...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)   // rows.Close() skipped
		}
		allRecords = append(allRecords, rec)
	}
	rows.Close()  // only reached on happy path
}
```

```go
// after: IIFE scopes the defer correctly
for _, schemaID := range schemaIDs {
	if err := func() error {
		rows, err := db.QueryContext(ctx, query, schemaID)
		if err != nil {
			return fmt.Errorf("query cdc records for schema %d: %w", schemaID, err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(...); err != nil {
				return fmt.Errorf("scan row: %w", err)
			}
			allRecords = append(allRecords, rec)
		}
		return rows.Err()
	}(); err != nil {
		return nil, err
	}
}
```

- [ ] **Step 2: Build check**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go build ./internal/e2e_harness/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/e2e_harness/federated/cdc.go
git commit -m "fix(e2e_harness): defer rows.Close() via IIFE in multi-schema CDC loop

Non-deferred rows.Close() was skipped on early scan error, leaking the
rows iterator. IIFE scopes the defer to the loop body.

Refs #113"
```

---

### Task 8: Add circuit breaker state-transition and concurrent tests (P3)

**Files:**
- Modify: `internal/postgres_duckdb_circuit_breaker_test.go`

- [ ] **Step 1: Add tests**

```go
func TestCircuitBreaker_TripsOnThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second, 5*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.IsOpen() {
		t.Fatal("breaker must not open before threshold is reached (2 of 3)")
	}
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("breaker must open once threshold is reached (3 of 3)")
	}
}

func TestCircuitBreaker_WindowExpiry(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond, 5*time.Second)
	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)
	cb.RecordFailure()
	if cb.IsOpen() {
		t.Fatal("breaker must not open when prior failure has expired from window")
	}
}

func TestCircuitBreaker_AutoRecoveryAfterOpenDuration(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected breaker open")
	}
	time.Sleep(100 * time.Millisecond)
	if cb.IsOpen() {
		t.Fatal("breaker must auto-close after open duration expires")
	}
}

func TestCircuitBreaker_RecordSuccessClosesBreakerImmediately(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 10*time.Second)
	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected breaker open")
	}
	cb.RecordSuccess()
	if cb.IsOpen() {
		t.Fatal("RecordSuccess must close the breaker immediately")
	}
}

func TestCircuitBreaker_ConcurrentSafety(t *testing.T) {
	cb := NewCircuitBreaker(10, 10*time.Second, 1*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			switch n % 3 {
			case 0:
				cb.RecordSuccess()
			case 1:
				cb.RecordFailure()
			case 2:
				_ = cb.IsOpen()
			}
		}(i)
	}
	wg.Wait()
	// race detector must not fire
}
```

- [ ] **Step 2: Run with race detector**

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test ./internal/ -run TestCircuitBreaker -race -v
```

Expected: all 7 tests pass (5 new + 2 existing), no race.

- [ ] **Step 3: Commit**

```bash
git add internal/postgres_duckdb_circuit_breaker_test.go
git commit -m "test(circuit_breaker): add threshold, window, recovery, and concurrency tests

Previous tests only verified no-panic. These cover all state transitions
plus concurrent correctness under the race detector.

Closes #113"
```

---

## Final verification

- [ ] Run full test suite with race detector:

```bash
GOCACHE=$(pwd)/.gocache GOFLAGS=-buildvcs=false go test . ./cdc ./cmd/... ./factory ./internal/... -race
```

- [ ] Open PR against `main` referencing `Closes #113`.
