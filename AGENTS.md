# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Forma Is

Forma is a general-purpose data management system on PostgreSQL. Entities are defined by JSON Schema (no DDL migrations) and stored in a dual model: a "hot fields" main table for indexed columns plus an EAV table for dynamic attributes. On top of that sits a lakehouse tier: CDC flushes flush hot data to S3 Parquet (delta), compaction merges delta into base, and DuckDB performs federated merge-on-read across all three tiers.

## Commands

```bash
make test          # unit tests (wraps: go test . ./cdc ./cmd/... ./factory ./internal/...)
make lint          # golangci-lint, pinned to v1.64.8 — same as CI; do not upgrade the pin
make coverage      # unit tests + coverage report in build/
make build-all     # build server/tools/sample into build/ with platform symlinks
```

Run a single test (mirror the Makefile's env to avoid sandbox cache/VCS errors):

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/sqlgen -run TestName -v
```

E2E (requires Docker; testcontainers-based):

```bash
go test -v ./internal/e2e_harness/... -timeout=5m                          # infra smoke
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m     # full federated suite
make test-e2e-production                                                    # production harness + oracle
```

Bun black-box E2E and k6 load tests live in `tests/e2e/` (see its README). Benchmarks: `make benchmark-smoke` (CI), `benchmark-regression`, `benchmark-heavy`; `benchmark-heavy-live` and `benchmark-concurrency` take hours and are operator-initiated only.

Local server: `./scripts/local_server.sh` (Docker Compose Postgres + init-db + server on :8080).

CI (`.github/workflows/ci.yml`) runs lint, unit tests with `-race`, the e2e smoke + `-tags=e2e -short` federated suite, the production harness, and `benchmark-smoke`.

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

## Pull Request Rules

* 请不要自动合并PR
* When review a PR, add findings / spec / standards / assessment / observations / verification / summary as comments to the PR
* After PR merged, clean up local branches, worktrees and fast forward main branches, then update and close related issues
