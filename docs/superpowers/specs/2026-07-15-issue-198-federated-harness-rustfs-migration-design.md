# Issue #198 — Migrate federated e2e harness from archived MinIO image to RustFS

- **Issue:** [#198](https://github.com/Lychee-Technology/forma/issues/198) (enhancement) — follow-up from PR #191 review
- **Date:** 2026-07-15
- **Scope:** `internal/e2e_harness/` (incl. `federated/`, `production/`)

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

## Approach

Rather than migrate call sites off `StartS3` and delete it, **keep `StartS3`/
`StopS3` as the backend-agnostic method names and swap the image inside `StartS3`
to RustFS.** This leaves all `StartS3` call sites (the two federated ones and the
smoke test) untouched. `StartS3` and `StartRustFS` would then be duplicates
starting the same image, so we **consolidate on `StartS3`** and remove
`StartRustFS`.

## Current State (verified 2026-07-15)

- `minio/minio:latest` appears in **exactly one place**: `harness.go:200`, inside
  `StartS3`. Its request also carries MinIO-specific `MINIO_ROOT_USER/PASSWORD`
  env, a `server /data` `Cmd`, and a 30s startup wait.
- `StartS3` callers (all remain unchanged by this design):
  - `federated/harness.go:310` (`startContainers`) — the ~68-case suite.
  - `e2e_test.go:25` (`TestE2EHarnessMinimal`) — smoke test. Its error string and
    inline comments already misleadingly say "rustfs".
- `StartRustFS` (`rustfs.go`) starts `rustfs/rustfs:latest` with
  `RUSTFS_ACCESS_KEY/SECRET_KEY` env and a 60s wait, and sets the **same** harness
  fields as `StartS3`: `h.S3Container`, `h.S3Endpoint`. Its only caller is
  production `cluster.go:153`.
- Credential constants `RustFSAccessKey`/`RustFSSecretKey` both equal
  `"minioadmin"`, matching the MinIO defaults, so the hardcoded `"minioadmin"`
  literals throughout the harness keep working unchanged.
- `StopS3` is **backend-agnostic**: it terminates whatever is in `h.S3Container`.
  Production, federated, and the smoke test all already call it. Only its doc
  comment is MinIO-specific.
- The federated harness creates the test bucket explicitly via the S3 client
  (`s3Client.CreateBucket`, `federated/harness.go:240`), independent of backend.

## Design

### 1. `harness.go` — repoint `StartS3` at RustFS

Replace the container request body of `StartS3` with the RustFS spec (adopting
`StartRustFS`'s current body), keeping the function name and signature:

- `Image`: `minio/minio:latest` → `rustfs/rustfs:latest`.
- `Env`: `MINIO_ROOT_USER/PASSWORD` → `RUSTFS_ACCESS_KEY/SECRET_KEY` (values from
  the shared credential constants, see step 3).
- Drop the MinIO-specific `Cmd` (`server /data --address :9000`); RustFS needs none.
- `WaitingFor`: keep `ForListeningPort("9000/tcp")`, bump the startup timeout
  30s → 60s (RustFS starts more slowly).

`h.S3Container`/`h.S3Endpoint` assignment and the return value are unchanged.

Update `StopS3`'s doc comment: "Stops the MinIO container." → wording that
reflects it stops the S3 (RustFS) container.

### 2. Remove `StartRustFS`, repoint production

- Delete `StartRustFS` and its now-empty file `rustfs.go`.
- `production/cluster.go:153`: `c.Base.StartRustFS(ctx)` → `c.Base.StartS3(ctx)`;
  adjust the surrounding error string ("start rustfs container") if it names the
  function.

### 3. Relocate the shared credential constants

- Move `RustFSAccessKey`/`RustFSSecretKey` (`= "minioadmin"`) from the deleted
  `rustfs.go` into `harness.go` (co-located with `StartS3`). Keep the names — they
  still accurately describe the RustFS backend and any existing references remain
  valid — and have `StartS3`'s `Env` reference them so credentials live in one
  place. Verify no other references to these constants break.

### 4. Call sites unchanged

`federated/harness.go` and `e2e_test.go` continue to call `StartS3`/`StopS3` — no
edits. (Optional nicety, not required: fix `e2e_test.go`'s stale `// Upload to
MinIO` comment. Include only if it keeps the diff tidy.)

## Verification (in order)

1. `make lint` and `make test` — confirm the `e2e_harness`, `federated`, and
   `production` packages still compile after the `StartRustFS` removal and
   constant move, and unit tests pass.
2. **Full federated suite:**
   `go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`.
   - Run **in isolation** — not concurrently with `make test`/unit tests, which
     has previously caused resource-contention avalanche false failures.
   - Must be green. This is the load-bearing acceptance check.
3. Production suite (guards the `cluster.go` repoint):
   `go test -v ./internal/e2e_harness/production/... -timeout=15m` (or
   `make test-e2e-production`).
4. Smoke: `go test -v ./internal/e2e_harness/... -run TestE2EHarnessMinimal -timeout=5m`.
5. Final grep: `grep -rn "minio/minio" --include='*.go' .` → empty.

## Risks / Watch-items

- **Startup time / wall-clock:** RustFS starts more slowly than MinIO (hence the
  60s wait). Each of the ~68 cases spins up its own container, so the full run is
  long; the 30m timeout covers it.
- **Conditional PUT (If-Match):** Federated manifest writes rely on the store
  returning HTTP 412 on a stale `If-Match`. RustFS's 412 behavior was already
  validated by the production suite (CopyObject, manifest conditional PUT,
  ListObjectsV2, DuckDB httpfs). The full federated run confirms it holds across
  all federated cases; treat any behavioral gap as a real compatibility finding,
  not a flake.
- **Production repoint:** `StartS3` and the former `StartRustFS` bodies are
  identical after step 1, so production behavior is unchanged — the production
  suite in verification step 3 confirms this.

## Out of Scope

- Renaming `S3*`-prefixed fields/functions to a backend-neutral name.
- Replacing the hardcoded `"minioadmin"` literals in `federated/harness.go` /
  `e2e_test.go` with the credential constants.
- `deploy/docker-compose.yml` (already on `rustfs/rustfs`).

## Workflow

Per repo `AGENTS.md`: isolated worktree + feature branch, PR body referencing
#198, do **not** auto-merge.
