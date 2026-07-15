# Issue #198 — Migrate federated e2e harness from archived MinIO image to RustFS

- **Issue:** [#198](https://github.com/Lychee-Technology/forma/issues/198) (enhancement) — follow-up from PR #191 review
- **Date:** 2026-07-15
- **Scope:** `internal/e2e_harness/` only

## Problem

The official MinIO docker image (`minio/minio:latest`) is archived and no longer
receives updates. PR #191 added `e2e_harness.StartRustFS` (the `rustfs/rustfs`
image already used by `deploy/docker-compose.yml`) and switched the **production**
harness to it, but `internal/e2e_harness/harness.go` still exposes `StartS3`,
which starts `minio/minio:latest`. `StartS3` is the S3 backend for the ~68-case
federated suite (`internal/e2e_harness/federated/`) and the minimal smoke test.

The federated suite therefore remains pinned to an archived image.

## Goal / Acceptance

- Full federated e2e suite green on RustFS.
- No remaining testcontainers reference to the archived MinIO image
  (`grep -rn "minio/minio" --include='*.go' .` returns nothing).

## Current State (verified 2026-07-15)

- `minio/minio:latest` appears in **exactly one place**: `harness.go:200`, inside
  `StartS3`.
- `StartS3` has **two callers**:
  - `federated/harness.go:310` (`startContainers`) — the ~68-case suite.
  - `e2e_test.go:25` (`TestE2EHarnessMinimal`) — smoke test. Its error string and
    inline comments already misleadingly say "rustfs".
- `StartRustFS` (`rustfs.go`) sets the **same** harness fields as `StartS3`:
  `h.S3Container` and `h.S3Endpoint`. Credentials `RustFSAccessKey`/
  `RustFSSecretKey` both equal `"minioadmin"`, matching the MinIO defaults, so the
  11 hardcoded `"minioadmin"` literals in the harness keep working unchanged.
- `StopS3` is **backend-agnostic**: it terminates whatever is in `h.S3Container`.
  Production (RustFS), federated, and the smoke test all already call `StopS3` to
  tear down. It stays; only its doc comment is MinIO-specific.
- The federated harness creates the test bucket explicitly via the S3 client
  (`s3Client.CreateBucket`, `federated/harness.go:240`), independent of the
  backend image.

## Design

All changes are in `internal/e2e_harness/`.

### 1. `federated/harness.go` — `startContainers` (~L310)

Redirect the container startup:

- `base.StartS3(ctx)` → `base.StartRustFS(ctx)`.
- Correct the error string `"start s3"` → `"start rustfs"` for accuracy.

Everything downstream is unchanged:
- `startDuckDB(base, base.S3Endpoint, "minioadmin", "minioadmin", "us-east-1", opts)`
  — credentials match RustFS.
- Cleanup already calls `StopS3`, which now terminates the RustFS container in
  `h.S3Container`.

### 2. `e2e_test.go` — `TestE2EHarnessMinimal` (L25)

- `h.StartS3(ctx)` → `h.StartRustFS(ctx)`.
- Fix the stale comments `// Start S3` and `// Upload to MinIO` to name S3/RustFS.
  (The error string already reads "start rustfs".)

### 3. `harness.go`

- Delete `StartS3` (L197–228) — the sole `minio/minio:latest` reference.
- Update `StopS3`'s doc comment: "Stops the MinIO container." → wording that
  reflects it stops the RustFS/S3 container.
- Keep `StopS3`, `S3Endpoint`, `S3Container`, and any other `S3*` names — all
  backend-agnostic and referenced throughout the harness.

### 4. `rustfs.go`

- Update the `StartRustFS` doc comment that currently claims "StartS3 (MinIO)
  remains for the existing federated suite" — no longer true after removal.

## Verification (in order)

1. `make lint` and `make test` — confirm the `e2e_harness` package still compiles
   after `StartS3` removal, and unit tests pass.
2. **Full federated suite:**
   `go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`.
   - Run **in isolation** — not concurrently with `make test`/unit tests, which
     has previously caused resource-contention avalanche false failures.
   - Must be green. This is the load-bearing acceptance check.
3. Smoke: `go test -v ./internal/e2e_harness/... -run TestE2EHarnessMinimal -timeout=5m`.
4. Final grep: `grep -rn "minio/minio" --include='*.go' .` → empty.

## Risks / Watch-items

- **Startup time / wall-clock:** RustFS starts more slowly than MinIO (the 60s
  `WithStartupTimeout` in `StartRustFS` already accounts for this). Each of the
  ~68 cases spins up its own container, so the full run is long; the 30m timeout
  covers it.
- **Conditional PUT (If-Match):** Federated manifest writes rely on the store
  returning HTTP 412 on a stale `If-Match`. RustFS's 412 behavior was already
  validated by the production suite (CopyObject, manifest conditional PUT,
  ListObjectsV2, DuckDB httpfs). The full federated run confirms it holds across
  all federated cases; if any case surfaces a behavioral gap, treat it as a real
  compatibility finding, not a flake.

## Out of Scope

- Renaming `S3*`-prefixed fields/functions to a backend-neutral name.
- Replacing the 11 hardcoded `"minioadmin"` literals with the
  `RustFSAccessKey`/`RustFSSecretKey` constants.
- `deploy/docker-compose.yml` (already on `rustfs/rustfs`).

## Workflow

Per repo `AGENTS.md`: isolated worktree + feature branch, PR body referencing
#198, do **not** auto-merge.
