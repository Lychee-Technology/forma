# Federated e2e Harness MinIO→RustFS Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repoint the `e2e_harness.StartS3` container at the RustFS image so the ~68-case federated suite stops depending on the archived `minio/minio:latest`, and consolidate the redundant `StartRustFS` into `StartS3`.

**Architecture:** Keep the backend-agnostic `StartS3`/`StopS3` method names and swap the image inside `StartS3` from `minio/minio:latest` to `rustfs/rustfs:latest` (adopting the request body of the existing `StartRustFS`). Because `StartS3` and `StartRustFS` then start the same image, delete `StartRustFS`, repoint its sole production caller to `StartS3`, and relocate the shared credential constants. All existing `StartS3` call sites (federated suite + smoke test) are untouched. Verification is existing e2e suites staying green — this is an infra swap, not new behavior.

**Tech Stack:** Go, testcontainers-go, RustFS (`rustfs/rustfs`), DuckDB httpfs, Postgres.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (`coding-standard.md`).
- Wrap errors with context: `fmt.Errorf("...: %w", err)` — never bare `return err`.
- Do NOT rename `S3*`-prefixed fields/functions, and do NOT replace the hardcoded `"minioadmin"` literals in `federated/harness.go` / `e2e_test.go`. Out of scope.
- Do NOT touch `deploy/docker-compose.yml` (already on `rustfs/rustfs`).
- Credential constants `RustFSAccessKey`/`RustFSSecretKey` both equal `"minioadmin"` and must keep that value (matches MinIO defaults still hardcoded elsewhere).
- Package for `harness.go`, `rustfs.go`, `e2e_test.go`: `e2e_harness`. All three share the package namespace — constants defined in one are visible in the others.
- Do NOT auto-merge the PR. PR body references #198.
- Run e2e suites **in isolation** — never concurrently with `make test`/unit tests (prior resource-contention avalanche false-failures).

---

## Pre-flight: Isolated Worktree

- [ ] **Step 0: Create the worktree (execution time)**

Per repo `AGENTS.md`: clean up local branches for already-merged PRs, update local `main`, then create the worktree. Use the `superpowers:using-git-worktrees` skill.

```bash
git -C /Users/ruoshi/code/Lychee/LTBase/forma fetch origin
git -C /Users/ruoshi/code/Lychee/LTBase/forma worktree add -b feat/issue-198-rustfs-federated-harness \
  ../forma-issue-198 origin/main
```

Work happens in `../forma-issue-198`. The spec is already committed on `main`
(`docs/superpowers/specs/2026-07-15-issue-198-federated-harness-rustfs-migration-design.md`);
copy/cherry-pick it into the branch if the reviewer wants it colocated, otherwise leave it on main.

---

## Task 1: Repoint `StartS3` at the RustFS image

**Files:**
- Modify: `internal/e2e_harness/harness.go:197-239` (`StartS3` body + `StopS3` doc comment)
- Verify against: `internal/e2e_harness/e2e_test.go` (`TestE2EHarnessMinimal`, unchanged — its existing `StartS3` call now exercises RustFS)

**Interfaces:**
- Consumes: `RustFSAccessKey`, `RustFSSecretKey` constants — currently defined in `internal/e2e_harness/rustfs.go` (same package, so referenceable). They are relocated into `harness.go` in Task 2, not here.
- Produces: `StartS3(ctx) (string, error)` — unchanged signature, now starts `rustfs/rustfs:latest`, sets `h.S3Container` and `h.S3Endpoint`. All callers unchanged.

- [ ] **Step 1: Replace the `StartS3` container request body**

In `internal/e2e_harness/harness.go`, replace the `StartS3` function (currently lines 197-228) with the RustFS-backed version. Update the doc comment; swap the image, env, and startup wait; drop the MinIO-specific `Cmd`. The credential values come from the existing `RustFSAccessKey`/`RustFSSecretKey` constants (defined in `rustfs.go` this task; moved to `harness.go` in Task 2).

```go
// StartS3 starts an S3-compatible object store container (RustFS, the same
// image as deploy/docker-compose.yml) and returns its endpoint. RustFS replaced
// the archived minio/minio image, which no longer receives updates. Caller is
// responsible for calling StopS3.
func (h *TestHarness) StartS3(ctx context.Context) (string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": RustFSAccessKey,
			"RUSTFS_SECRET_KEY": RustFSSecretKey,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", err
	}
	h.S3Container = container
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return "", err
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, mapped.Port())
	h.S3Endpoint = endpoint
	return endpoint, nil
}
```

- [ ] **Step 2: Update the `StopS3` doc comment**

In the same file, the `StopS3` comment currently reads `// StopS3 stops the MinIO container.` Change it to:

```go
// StopS3 stops the S3 (RustFS) container.
```

- [ ] **Step 3: Verify the package still compiles**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go build ./...`
Expected: exit 0, no errors. (`StartRustFS` in `rustfs.go` still exists and also starts `rustfs/rustfs` — a transient duplicate that compiles fine; Task 2 removes it.)

- [ ] **Step 4: Run the smoke test on RustFS (Docker required)**

Run in isolation: `go test -v ./internal/e2e_harness/ -run TestE2EHarnessMinimal -timeout=5m`
Expected: PASS. This test's unchanged `StartS3` call now spins a RustFS container, seeds parquet, and reads it back through DuckDB httpfs — proving basic RustFS + client + DuckDB wiring.

- [ ] **Step 5: Commit**

```bash
git add internal/e2e_harness/harness.go
git commit -m "test(e2e): repoint StartS3 at rustfs image (#198)"
```

---

## Task 2: Remove `StartRustFS`, repoint production, relocate constants

**Files:**
- Delete: `internal/e2e_harness/rustfs.go`
- Modify: `internal/e2e_harness/harness.go` (add the relocated credential constants above `StartS3`)
- Modify: `internal/e2e_harness/production/cluster.go:153-156` (`StartRustFS` → `StartS3`)

**Interfaces:**
- Consumes: `StartS3(ctx) (string, error)` from Task 1.
- Produces: `RustFSAccessKey`/`RustFSSecretKey` constants, now owned by `harness.go`. `StartRustFS` no longer exists.

- [ ] **Step 1: Repoint the production caller**

In `internal/e2e_harness/production/cluster.go`, the block at lines 153-156 reads:

```go
	if _, err := c.Base.StartRustFS(ctx); err != nil {
		_ = c.Base.StopPostgres(ctx)
		return fmt.Errorf("start rustfs container: %w", err)
	}
```

Change the call to `StartS3` (the error string stays accurate — RustFS is what starts):

```go
	if _, err := c.Base.StartS3(ctx); err != nil {
		_ = c.Base.StopPostgres(ctx)
		return fmt.Errorf("start rustfs container: %w", err)
	}
```

- [ ] **Step 2: Add the credential constants to `harness.go`**

In `internal/e2e_harness/harness.go`, add this const block immediately above the `StartS3` function you edited in Task 1:

```go
// RustFS credentials used by StartS3. They intentionally match the historical
// MinIO defaults so callers can switch S3 backends without touching client
// configuration.
const (
	RustFSAccessKey = "minioadmin"
	RustFSSecretKey = "minioadmin"
)
```

- [ ] **Step 3: Delete `rustfs.go`**

The file now holds only the (now-duplicated) constants and the redundant `StartRustFS`. Remove the whole file:

```bash
git rm internal/e2e_harness/rustfs.go
```

- [ ] **Step 4: Verify the whole module compiles**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go build ./...`
Expected: exit 0. Confirms: no duplicate `RustFSAccessKey`/`RustFSSecretKey` declaration, no dangling `StartRustFS` reference, production `cluster.go` resolves `StartS3`.

Also confirm no stray references remain:

Run: `grep -rn "StartRustFS" --include='*.go' .`
Expected: no output.

- [ ] **Step 5: Vet the production package**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet ./internal/e2e_harness/...`
Expected: exit 0, no diagnostics.

- [ ] **Step 6: Commit**

```bash
git add internal/e2e_harness/harness.go internal/e2e_harness/production/cluster.go
git rm internal/e2e_harness/rustfs.go
git commit -m "test(e2e): consolidate StartRustFS into StartS3, drop rustfs.go (#198)"
```

---

## Task 3: Full-suite verification + PR

**Files:** none modified — this task runs the acceptance checks and opens the PR.

**Interfaces:** none.

- [ ] **Step 1: Lint + unit tests**

Run: `make lint && make test`
Expected: both PASS. (Confirms nothing else in the module references the removed symbols and unit suites are unaffected.)

- [ ] **Step 2: Full federated suite on RustFS (Docker required, ~30m, run in isolation)**

Run: `go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`
Expected: PASS — all ~68 cases green. This is the load-bearing acceptance check (issue #198: "Full federated e2e suite green on RustFS").
If any case fails on a store-behavior difference (e.g. conditional PUT / `If-Match` 412, ListObjectsV2 pagination, CopyObject), treat it as a real RustFS compatibility finding to diagnose — not a flake. Do NOT run this concurrently with Step 1.

- [ ] **Step 3: Production suite (guards the `cluster.go` repoint)**

Run: `make test-e2e-production`
Expected: PASS. `StartS3` and the former `StartRustFS` bodies are identical, so production behavior is unchanged; this confirms it.

- [ ] **Step 4: Confirm no MinIO image reference remains (acceptance criterion 2)**

Run: `grep -rn "minio/minio" --include='*.go' .`
Expected: no output.

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin feat/issue-198-rustfs-federated-harness
gh pr create --repo Lychee-Technology/forma \
  --title "test(e2e): migrate federated harness from archived MinIO image to RustFS (#198)" \
  --body "$(cat <<'EOF'
Closes #198.

Repoints `e2e_harness.StartS3` at the `rustfs/rustfs` image (the same image the
production harness and deploy/docker-compose.yml already use), so the ~68-case
federated suite no longer depends on the archived `minio/minio` image. Keeps the
backend-agnostic `StartS3`/`StopS3` names; all call sites unchanged.

Consolidates the now-redundant `StartRustFS` into `StartS3`: repointed the sole
production caller (`cluster.go`), relocated the shared credential constants into
`harness.go`, and deleted `rustfs.go`.

## Acceptance
- [x] Full federated e2e suite green on RustFS (`-tags=e2e`).
- [x] No remaining testcontainers reference to `minio/minio`.

## Verification
- `make lint && make test`
- `go test ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`
- `make test-e2e-production`
- `grep -rn "minio/minio" --include='*.go' .` → empty

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Do NOT auto-merge (per `AGENTS.md`). After merge: clean up worktree/local branch, fast-forward `main`, close #198.

---

## Notes on TDD framing

This plan has no red-green new-test cycle because it swaps an infrastructure image behind an existing interface — there is no new behavior to specify with a failing unit test. The existing suites are the tests: the smoke test (Task 1, fast feedback) and the full federated + production suites (Task 3, acceptance). Each task still ends with an independently verifiable green gate.
