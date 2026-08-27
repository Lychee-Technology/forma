# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Forma is

Forma is a general-purpose data management system on PostgreSQL. Entities are defined by JSON Schema (no DDL migrations) and stored in a dual model: a "hot fields" main table for indexed columns plus an EAV table for dynamic attributes. On top of that sits a lakehouse tier: CDC flushes hot data to S3 Parquet (delta), compaction merges delta into base, and DuckDB performs federated merge-on-read across all three tiers.

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

Run a single test (mirror the Makefile's env, GOTOOLCHAIN included; see the note below):

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false GOTOOLCHAIN=go1.26.0+auto go test ./internal/sqlgen -run TestName -v
```

The Makefile pins `GOTOOLCHAIN=go1.26.0+auto` (#448). The `go1.26.0` floor fixes the
selected toolchain at go1.26.0. Plain `GOTOOLCHAIN=auto` never downgrades, so it lets a
newer local Go break `make lint` (golangci-lint v1.64.8 cannot read newer export data)
and `make test` (stdlib error-text assertions). The floor must name a full toolchain
(`go1.26.0`, not `go1.26`): a bare `go1.26` is a language version, not a toolchain name,
and no longer resolves now that go.mod's directive is `go 1.26` without a patch. The
`+auto` suffix still follows a higher `go`/`toolchain` directive in go.mod if one is
added, so a future bump needs no change here. Direct `go test` invocations bypass the
Makefile, so set the variable yourself as above. To probe a newer toolchain
deliberately, run `GOTOOLCHAIN=auto make test`. On a machine whose local Go is newer
than 1.26.0, the first run downloads the go1.26.0 toolchain once.

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

CI (`.github/workflows/ci.yml`) runs lint, unit tests with `-race`, the e2e smoke + `-tags=e2e -short` federated suite, the production harness, and `benchmark-smoke`. Note `-short` skips 37 of the 124 federated e2e tests (deduplication, merge-on-read, data-tier, soft-delete, performance); their CI signal is the nightly workflow (`.github/workflows/nightly-e2e.yml`), which runs the full federated suite (#434) and the production suite under `-race` (#410), filing a `nightly-e2e`- / `nightly-e2e-race`-labeled issue respectively on failure.

## Architecture

Module: `github.com/lychee-technology/forma`.

The root package `forma` is the public API surface: schema registry, config, storage interfaces, shared types, and sentinel errors (`errors.go`). Match errors with `errors.Is`/`errors.As`, never string comparison.

`factory/` is the composition root: it wires a pgxpool and metadata cache into a ready `EntityManager`. Start here to see how the pieces connect.

`internal/` holds the implementation. Key areas:

- `entity_manager.go` and `entity_*_service.go`: core business logic, split into CRUD, batch, query, and relation services.
- `postgres_persistent_repository*.go`: the dual write/read path, main (hot fields) table plus EAV table. SQL generation for OLTP queries uses CTE + JSON_AGG to avoid N+1.
- `httpapi/`: HTTP server (thin handlers). `bootstrap/`: config/DB setup shared by `cmd/` entrypoints.
- `schemameta/`: metadata cache mapping JSON Schema attributes to physical storage. `transform/`: value conversion between JSON and typed EAV columns.

The three-tier federated query path is the most cross-cutting subsystem (see `docs/federated-query/design.md`):

- Tiers: Hot is Postgres rows with `change_log.flushed_at = 0`; Warm is S3 `/delta/` Parquet; Cold is S3 `/base/` Parquet.
- Consistency uses an anti-join "dirty set": any S3 row whose `row_id` is still unflushed in Postgres is discarded and replaced by the hot version. Timestamps are never compared.
- `internal/sqlgen/`: dual-path SQL generation. The same condition DSL renders to both PostgreSQL and DuckDB dialects (`dualpath_sql_generator.go`, `duckdb_*`), plus predicate IR/normalization and a plan cache.
- `internal/federated/`: the DuckDB engine: connection management, query execution, keyset pagination, tier merge, circuit breaker.
- `cdc/` and `internal/cdc/`: CDC flush (hot tier to delta Parquet). `internal/compaction/`: delta-to-base merge. `internal/manifest/`: S3 file manifests consumed by federated reads.
- `internal/queryplan/`: plan-cache primitives keyed on stable query shapes.

`cmd/` contains `server` (HTTP API), `tools` (subcommands: `init-db`, `cdc-flush`, `cdc-init`, `compactor`, `validate-schema-consistency`), `lambda` (ARM64 CGO build), `benchmark`, and `sample`.

## Coding standards

`coding-standard.md` is the authority. The load-bearing rules:

- Source files stay at or under 500 lines, functions at or under 100. If a change would exceed these limits, refactor instead.
- Always wrap errors with context: `fmt.Errorf("failed to process user %s: %w", userID, err)`, never a bare `return err`.
- Use early returns and flat structure; avoid more than three levels of nesting.
- When touching code that doesn't meet standards, fix the immediate area and suggest follow-up refactoring. Don't rewrite unrelated code unasked.

## Error semantics

There are two distinct error classes (`docs/error-handling.md`):

- Write-path validation builds `forma.InvalidInputf` (or `NotFoundf`/`Conflictf`) carriers, which surface as user-facing 4xx responses whose body is the carrier's published message (#313). A bare `%w` wrap of a sentinel earns the status but answers with a redacted body, and fails the `TestNoBareSentinelWraps` guard.
- Read-path consistency errors (metadata drift, storage column mismatch) are plain errors: operator-visible failures, not 4xx.

Error messages must name the logical value type, the offending column or attribute, and the expected state. Detail an operator needs but a caller cannot act on goes behind `forma.WithOperatorDetail`: it stays in `Error()` and the log, and never appears in the published message.

## Non-code artifacts

Anything produced while working an issue that is not code must end up on GitHub, not just on disk. This covers design drafts, specs, implementation plans, research notes, investigation and verification notes, assessments, and rulings made mid-execution. Post each one as a comment on the relevant issue, never into the repo (`docs/superpowers/` is gitignored). If the work has no issue yet, create one first; if the artifact is about changes already under review, post it to the PR instead.

- Write non-code artifacts in English by default.
- Post the full content, not a summary or a file path, and post it when it is produced: a design draft goes up as a draft (say so), a plan goes up when written, a mid-execution decision goes up the moment it is taken. The issue is the complete decision record; nothing load-bearing may live only in a chat transcript or a local file.
- A local working copy is fine, but it is invisible to everyone else and does not survive the branch. Several child repos keep planning notes in gitignored local directories (for example `__ref__/plan/` in `ltbase.api`, see #497). Do not force-add gitignored planning files to make them shareable; the issue comment is the sharing mechanism.
- Say in the comment which artifact it is and where the working copy lives, so a later reader knows whether they are looking at a plan, a spec, or a review.

Scope: per-issue artifacts only. Reference documentation of the system itself (e.g. `docs/federated-query/design.md`) lives in `docs/` and is committed as before. Anything that must become a durable repository convention also belongs in `docs/` (an ADR, runbook, or reference page): the issue comment records the thinking, and `docs/` records the decision. Review artifacts belong on the PR; see PR rules.

## PR rules

- Do not merge a PR unless I explicitly ask you to.
- When reviewing a PR, post everything (findings, spec and standards checks, assessment, observations, verification, summary) as one comment on the PR.
- When I ask you to merge a PR, squash-merge by default unless I ask for something else.
- After a PR is merged, clean up local branches and worktrees, fast-forward main, then update and close related issues.

## Git conventions

Never include AI attribution in commit messages, PR titles, or PR descriptions, in any form. That means no

- `Co-Authored-By: ...`
- `Generated with ...` footers
- sign-offs or footers naming an LLM or AI agent (OpenAI, GPT, Claude, Anthropic, and the like)

When squash-merging, write a clean commit message that describes only the change itself.
