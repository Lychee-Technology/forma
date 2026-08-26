# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Forma Is

Forma is a general-purpose data management system on PostgreSQL. Entities are defined by JSON Schema (no DDL migrations) and stored in a dual model: a "hot fields" main table for indexed columns plus an EAV table for dynamic attributes. On top of that sits a lakehouse tier: CDC flushes flush hot data to S3 Parquet (delta), compaction merges delta into base, and DuckDB performs federated merge-on-read across all three tiers.

## Commands

```bash
make test          # unit tests (wraps: go test . ./cdc ./cmd/... ./factory ./internal/...)
make fmt-check     # gofmt gate over git-listed Go files — CI runs it ahead of the linter
make lint          # golangci-lint, pinned to v1.64.8 — same as CI; covers the root AND infra/ modules; do not upgrade the pin
make check-infra   # build + vet the infra/ Pulumi module (a separate Go module the root gates never reach)
make coverage      # unit tests + coverage report in build/
make build-all     # build server/tools/sample into build/ with platform symlinks
./scripts/test_with_container_runtime.sh  # auto-detect Docker/Podman, configure E2E, run make test
```

Run a single test (mirror the Makefile's env — GOTOOLCHAIN included, see the note below):

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false GOTOOLCHAIN=go1.26.0+auto go test ./internal/sqlgen -run TestName -v
```

The Makefile pins `GOTOOLCHAIN=go1.26.0+auto` (#448). The `go1.26.0` floor fixes the
selected toolchain at go1.26.0 — unlike `GOTOOLCHAIN=auto`, which never downgrades and
so lets a newer local Go break `make lint` (golangci-lint v1.64.8 cannot read newer
export data) and `make test` (stdlib error-text assertions). The floor must name a full
toolchain (`go1.26.0`, not `go1.26`): a bare `go1.26` is a language version, not a
toolchain name, and no longer resolves now that go.mod's directive is `go 1.26` without a
patch. The `+auto` suffix still follows a higher `go`/`toolchain` directive in go.mod if
one is added, so a future bump needs no change here. Direct `go test` invocations bypass
the Makefile, so set it yourself as above. Probe a newer toolchain deliberately with
`GOTOOLCHAIN=auto make test`. On a machine whose local Go is newer than 1.26.0, the first
run downloads the go1.26.0 toolchain once.

E2E (Docker or rootless Podman; testcontainers-based):

```bash
./scripts/test_with_container_runtime.sh
GOTOOLCHAIN=go1.26.0+auto go test -v ./internal/e2e_harness/... -timeout=5m                      # infra smoke
GOTOOLCHAIN=go1.26.0+auto go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m # full federated suite
make test-e2e-production                                                    # production harness + oracle
```

`scripts/test_with_container_runtime.sh` honors an existing `DOCKER_HOST`. Otherwise it
uses a reachable Docker daemon, or starts the rootless Podman socket at
`$XDG_RUNTIME_DIR/podman/podman.sock` and exports the Docker-compatible endpoint.
Rootless Podman runs set `TESTCONTAINERS_RYUK_DISABLED=true` because the Ryuk reaper
cannot reliably connect to the rootless API socket. The script then runs `make test`.

Bun black-box E2E and k6 load tests live in `tests/e2e/` (see its README). Benchmarks: `make benchmark-smoke` (CI), `benchmark-regression`, `benchmark-heavy`; `benchmark-heavy-live` and `benchmark-concurrency` take hours and are operator-initiated only.

Local server: `./scripts/local_server.sh` (Docker Compose Postgres + init-db + server on :8080).

CI (`.github/workflows/ci.yml`) runs lint, unit tests with `-race`, the e2e smoke + `-tags=e2e -short` federated suite, the production harness, and `benchmark-smoke`. Note `-short` skips 37 of the 124 federated e2e tests (deduplication, merge-on-read, data-tier, soft-delete, performance); their CI signal is the nightly full-suite run (`.github/workflows/nightly-e2e.yml`), which files a `nightly-e2e`-labeled issue on failure (#434).

## Architecture

Module: `github.com/lychee-technology/forma`.

**Root package `forma`** — public API surface: schema registry, config, storage interfaces, shared types, and sentinel errors (`errors.go`). Match errors with `errors.Is`/`errors.As`, never string comparison.

**`factory/`** — composition root: wires a pgxpool + metadata cache into a ready `EntityManager`. Start here to see how the pieces connect.

**`internal/`** — implementation. Key areas:

- `entity_manager.go` + `entity_*_service.go` — core business logic, split into CRUD, batch, query, and relation services.
- `postgres_persistent_repository*.go` — the dual write/read path: main (hot fields) table + EAV table. SQL generation for OLTP queries uses CTE + JSON_AGG to avoid N+1.
- `httpapi/` — HTTP server (thin handlers). `bootstrap/` — config/DB setup shared by `cmd/` entrypoints.
- `schemameta/` — metadata cache mapping JSON Schema attributes to physical storage; `transform/` — value conversion between JSON and typed EAV columns.

**Three-tier federated query path** (the most cross-cutting subsystem — see `docs/federated-query/design.md`):

- Tiers: Hot = Postgres rows with `change_log.flushed_at = 0`; Warm = S3 `/delta/` Parquet; Cold = S3 `/base/` Parquet.
- Consistency uses an anti-join "dirty set": any S3 row whose `row_id` is still unflushed in Postgres is discarded and replaced by the hot version — never timestamp comparison.
- `internal/sqlgen/` — dual-path SQL generation: the same condition DSL renders to both PostgreSQL and DuckDB dialects (`dualpath_sql_generator.go`, `duckdb_*`), plus predicate IR/normalization and a plan cache.
- `internal/federated/` — DuckDB engine: connection management, query execution, keyset pagination, tier merge, circuit breaker.
- `cdc/` + `internal/cdc/` — CDC flush (hot → delta Parquet). `internal/compaction/` — delta → base merge. `internal/manifest/` — S3 file manifests consumed by federated reads.
- `internal/queryplan/` — plan-cache primitives keyed on stable query shapes.

**`cmd/`** — `server` (HTTP API), `tools` (subcommands: `init-db`, `cdc-flush`, `cdc-init`, `compactor`, `validate-schema-consistency`), `lambda` (ARM64 CGO build), `benchmark`, `sample`.

## Coding Standards

`coding-standard.md` is the authority. The load-bearing rules:

- Source files ≤500 lines, functions ≤100 lines. If a change would exceed these, refactor instead.
- Always wrap errors with context: `fmt.Errorf("failed to process user %s: %w", userID, err)` — never bare `return err`.
- Early returns, flat structure; avoid >3 levels of nesting.
- When touching code that doesn't meet standards, fix the immediate area and suggest follow-up refactoring — don't rewrite unrelated code unasked.

## Error Semantics

Two distinct error classes (`docs/error-handling.md`):

- **Write-path validation** builds `forma.InvalidInputf` (or `NotFoundf`/`Conflictf`) carriers → surfaces as user-facing 4xx whose body is the carrier's published message (#313). A bare `%w` wrap of a sentinel earns the status but answers a redacted body, and fails the `TestNoBareSentinelWraps` guard.
- **Read-path consistency** errors (metadata drift, storage column mismatch) are plain errors → operator-visible failures, not 4xx.

Error messages must name the logical value type, the offending column/attribute, and the expected state. Detail an operator needs but a caller cannot act on goes behind `forma.WithOperatorDetail` — it stays in `Error()` and the log, never in the published message.

## Specs and Design Docs

Every non-code artifact produced while working an issue goes to that issue as a comment — never into the repo (`docs/superpowers/` is gitignored). This covers design drafts, specs, implementation plans, investigation and verification notes, and rulings made mid-execution. Post the full content, not a summary, and post it when it is produced: a design draft goes up as a draft (say so), a plan goes up when written, a mid-execution decision goes up at the moment it is taken. The issue is the complete decision record; nothing load-bearing may live only in a chat transcript or a local file.

Scope: per-issue artifacts only. Reference documentation of the system itself (e.g. `docs/federated-query/design.md`) lives in `docs/` and is committed as before. Review artifacts belong on the PR — see Pull Request Rules.

## Pull Request Rules

* 请不要自动合并PR
* When review a PR, add findings / spec / standards / assessment / observations / verification / summary as comments to the PR
* After PR merged, clean up local branches, worktrees and fast forward main branches, then update and close related issues
