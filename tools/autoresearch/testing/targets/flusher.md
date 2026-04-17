# Target: `internal/cdc/flusher.go`

## Why This Target

`flusher.go` is a high-value CDC path with clear branch structure and direct production impact.
It already has a solid starting test file, which makes incremental expansion cheap and reviewable.

## Primary Test Files

- `internal/cdc/flusher_test.go`
- `internal/e2e_harness/federated/cdc_flush_test.go`

## Functions In Scope

- `processSchema`
- `shouldFlush`
- `executeFlush`
- `executeBatch`
- `executeFlushInChunks`
- `executeFlushSingle`

## Priority Scenarios

1. Given another worker already owns the schema lock, when `processSchema` runs, then it exits cleanly without flushing.
2. Given changelog stats cannot be read, when `processSchema` runs, then the error is returned with no partial progress.
3. Given there are no pending rows, when `processSchema` runs, then no flush work starts.
4. Given thresholds are not met, when `processSchema` runs, then the flush is skipped.
5. Given no row ids are selected, when `executeFlush` runs, then it exits without export or copy work.
6. Given schema registry lookup fails, when `executeFlush` builds projection data, then it falls back to the generic projection path.
7. Given byte-based batching is enabled, when a large flush is executed, then rows are split into multiple batches.
8. Given `dryRun` is enabled, when a flush batch succeeds, then mark-flushed and manifest updates are skipped.
9. Given mark-flushed returns no updated ids, when `executeBatch` completes, then the result reflects that nothing was committed.
10. Given manifest update fails after a successful flush, when `executeBatch` finishes, then the flush still succeeds.
11. Given fewer rows are marked flushed than requested, when `executeBatch` completes, then the partial update is surfaced correctly.
12. Given export or copy fails, when the batch runs, then the error is returned and downstream state is not advanced.

## Keep Bias

Prefer candidates that cover scenarios 7, 10, and 12 first when they are directly testable with the current local setup.
If those higher-priority scenarios require non-S3 concrete-type seams that do not exist, immediately fall back to scenarios 1 through 6 instead of discarding the whole run.
Do not treat a blocked high-priority scenario as proof that the entire target is blocked.

## Runtime Preferences

- For scenarios that need real S3 behavior, prefer a Docker-backed RustFS instance over writing S3 mocks.
- Existing local defaults in this repo use RustFS at `http://localhost:19000` with access key `minio` and secret `minio` or `minio_password` depending on the harness path.
- Reuse the repo's existing Docker/RustFS setup and environment conventions before considering a scenario blocked.

## Blocked Scenarios (do not attempt)

These scenarios require production code interface extraction or mocking infrastructure that is not available. Attempting them wastes iterations.

- **Scenario 8** (dry-run path / `executeBatch` with dryRun=true): still blocked because it needs `DuckDBExecutor` behavior without a seam and RustFS does not help here
- **Scenario 9** (partial mark-flushed ids): still blocked because it needs `DuckDBExecutor` behavior without a seam and RustFS does not help here
- **Scenario 11** (partial flush with fewer updated ids): still blocked because it needs `DuckDBExecutor` behavior without a seam and RustFS does not help here

If you encounter these scenarios, write `status=discard` immediately and move to the next scenario.

## Attempted & Blocked

Record any scenario you tried and discarded due to missing interfaces here.
The model should not retry any scenario listed here.

- Scenario 8: attempted [Apr 15], blocked — DuckDBExecutor concrete type cannot be mocked
- Scenario 9: attempted [Apr 15], blocked — DuckDBExecutor concrete type cannot be mocked
- Scenario 11: attempted [Apr 15], blocked — DuckDBExecutor concrete type cannot be mocked

## Constraints

- Extend `internal/cdc/flusher_test.go` unless there is a strong reason not to.
- Avoid asserting on exact log text.
- Prefer direct function-level tests before adding new E2E coverage.
- Write tests so the scenario is readable from the test name or subtest name.
- If a scenario needs real S3 semantics such as copy, delete, manifest persistence, or bucket/object interactions, do not introduce a fake `S3ObjectClient`; use local RustFS instead.
- Only use pure in-process test doubles for S3 when the scenario is explicitly about argument plumbing rather than real storage behavior.
- If a scenario still requires mocking a non-S3 concrete type without an interface, discard it and try the next scenario.
- Do not retry any scenario listed in Attempted & Blocked.

## Recommended Fallback Order

When scenarios 7 through 12 are blocked or too expensive for the current local setup, immediately try the next available scenario in this order:

1. Scenario 5: no row ids selected in `executeFlush`
2. Scenario 6: schema registry lookup falls back to generic projection
3. Scenario 2: changelog stats read failure
4. Scenario 1: schema lock already held
5. Scenario 3: no pending rows
6. Scenario 4: threshold skip
