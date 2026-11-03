# Spike: type-aware SQL call-site coverage for the DSQL lint gate

Question: can a type-aware inventory of SQL execution sites, joined with runtime corpus evidence, replace PR #801's SQL-text scanner as the guard for #740's criterion 4 ("newly introduced DSQL-reachable SQL cannot quietly evade the inventory")?

This is an investigation with a disposable prototype. Nothing here is production code. The scanner in PR #801 is untouched, and no branch was pushed.

| Revision | Commit |
|---|---|
| `origin/main` | `7ebe2b53a928` (also the PR's merge base) |
| PR #801 HEAD | `9776c2bbb24f` |
| Spike | local branch `spike/dsql-typed-callsite`, forked from the PR HEAD so the existing corpus recorders could be reused |

Environment: Go 1.26.0 through `GOTOOLCHAIN=go1.26.0+auto` (the local Go is 1.27.3), linux/amd64, 8 CPUs, 15 GiB of RAM, and dsql-lint 0.2.22 from the PR's pinned wheel. Every number below was measured in this environment unless it is marked otherwise. The raw outputs are in `docs/spikes/dsql-typed-callsite-data/`, and the summary is in `docs/spikes/dsql-typed-callsite-results.json`.

## 1. Executive summary

**Verdict: CONDITIONAL GO.**

The typed call-site inventory does what the text scanner could not. It enumerates where Forma hands SQL to a driver: 126 sites in the shipped closure. It does this from resolved symbols, with a fixed catalogue of about 40 driver methods and no SQL grammar.

- **Mutation tests.** Across 38 adversarial mutations, nothing was missed silently.
  - Every failure shape from R2–R12, plus the four bypasses found in the recovery review, is caught. The SQL text does not matter.
  - The patterns the inventory cannot resolve (method values, function-valued calls, interfaces with no implementer) fail closed.
- **Runtime attribution.** Stack capture in the existing corpus recorders attributes 61/61 real corpus statements and 20/20 synthetic ones to the right site, with zero ambiguity. The result is identical with inlining on, inlining off (`-gcflags=all=-l`) and `-race`.
- **Evidence join.** The join between inventory, manifest and corpus fails closed on every required condition, both in unit tests and against real data.
- **Cost.** The static pass takes 0.6 s on a warm cache and 17.9 s on an empty one. Runtime capture adds no measurable time.

The guarantee is about **execution sites, not SQL texts**. Three classes stay outside it, measured and documented below:
- a new SQL variant reaching an existing site, through a new branch or a struct field;
- a new caller of a helper that rewrites SQL before sending it;
- reflection.

A variant at an *exercised* `dsql` site is still caught, by the runtime lint rather than the inventory. This was demonstrated with PR #801's own gate.

The conditions for GO (section 13) are policy decisions and bounded work, not open research:
- a wrapper-following mode;
- client-side capture for SQL that pgx writes;
- a COMMIT/ROLLBACK family;
- a batch policy;
- expression-hash site IDs;
- an emptiness check;
- converting 45 generator-only corpus statements into executed ones.

## 2. Requirements and scope

From #740:
- **Criterion 3:** representative generator SQL is linted, and an inventory says what is examined.
- **Criterion 4:** new DSQL-reachable SQL cannot quietly evade the inventory.
- **Criterion 7:** diagnostics name the generating source.
- **Fail-closed:** extraction errors, zero files and zero statements must fail.

The recovery review on #801 proposed restating criterion 4 as "every driver call site in shipped code is classified, every `dsql` site is exercised by the corpus, and every recorded statement maps to a `dsql` site." This spike tests that proposal. It does not assume it.

Out of scope: production implementation, changes to #740 or #801, and runtime DSQL behaviour (#739).

## 3. Baseline: PR #801

| Measure | Value | Source |
|---|---|---|
| Scanner sites (symbols) | 239: 111 "calls a database method", 117 "holds SQL text", 5 fragments, 6 transaction options | measured (`TestSpikeBaselineScan`) |
| Inventory entries | 204: rendered 64, uncovered 34, passthrough 25, postgres-only 4, duckdb 35, not-dsql 42 (plus 5 gap lines) | measured |
| Corpus | 16 units, 106 statements, 64 generators | measured (gate run) |
| …recorded through a driver mock (`Recorder`) | 61 | measured (capture run) |
| …from direct generator calls in tests | 45 | 106 − 61 |
| Scanner time | 0.92 s, best of 5 | measured |
| Whole gate, warm | 6.8 s; corpus export alone 5.2 s | measured |
| Known scanner limits | R12-F1 still open; the four recovery-review bypasses (keyword from a helper, constant from another package, embedded template, new SQL in a classified symbol) | recovery review, re-confirmed below |

## 4. SQL sink matrix (actual Forma code)

| Family | Actual location (example) | Static discovery | Dynamic evidence | Limitations |
|---|---|---|---|---|
| pgx exec/query | `internal/postgres_persistent_repository.go` (`deleteRecordInTx`: `pgx.Tx.Exec`, `QueryRow`); 57 sites | resolved method declared in `pgx/v5` or `pgxpool` | pgxmock matcher stack | — |
| pgx transactions | `internal/postgres_persistent_repository_tx.go:23` `inWriteTx` → `persistentRecordPool.BeginTx(ctx, opts)`; `cmd/tools/init_db.go:457` `withTx`; `internal/cdc/export_pg_stage.go:54` | Begin/BeginTx/BeginFunc sites, options expression recorded | `WireConn`, with stack captured on the **client write** | Options often arrive as a parameter (`opts`) and depend on the engine; only runtime shows the BEGIN text |
| COMMIT/ROLLBACK | `inWriteTx`, `stageInSnapshot` | `tx-end` family (13 sites) | `WireConn` client write | Without this family, 7 of 81 records went unattributed |
| pgx batch | **not used in shipped code** (`meteredTx.SendBatch` refuses batches, `internal/postgres_tx_meter.go:151`) | `Batch.Queue` and `SendBatch` sites (synthetic fixture) | pgxmock matches at `SendBatch` and again at result read | Runtime attributes to `SendBatch`, not `Queue`; the mock double-records |
| Prepare | none in shipped code | pgx `Prepare` (SQL at index 2), `sql.*.Prepare*` | — | `Stmt.Exec` has no SQL argument and is correctly not a site |
| Bulk COPY | `meteredTx.CopyFrom` only (forwarding) | `pgx-copy` family | — | No production COPY of DSQL tables found |
| database/sql | `internal/cdc/schema_lease.go:90` and others; 32 sites | resolved method in `database/sql` | sqlmock matcher stack | The handle type does not reveal PostgreSQL vs DuckDB; this needs a manifest class |
| SQL wrappers | 16 verbatim, e.g. `meteredTx.Exec`, `txStatementPool.Query`, `scanRowPage`, widthaudit `query*Findings`, `DuckDBClientQueryExecutor.Query` | fixpoint: a function that passes its own string parameter unchanged is a sink; its callers are sites | innermost frame at a site (wrapper frames have no site and are skipped) | Wrappers that *modify* SQL (38 functions under taint) — see section 7.1 |
| Project interfaces | 18 resolved, e.g. `persistentRecordPool.Query`, `schemaledger.Querier.Query`, `widthaudit.Querier.Query`, `declaredindex.Conn.Exec` | interface method → candidate implementers (`types.Implements`) among project types and driver handles | same | An interface with no implementer is flagged `unresolved-interface` |
| Method values | none in shipped code (verbatim mode); taint mode flags `internal/cdc/mark_flushed_dsql.go:39,41` (`markFlushedFor` returns `MarkFlushedVersions` as a value) | `sink-as-value` finding | — | Always fails closed |
| Dynamic builders | `fmt.Sprintf` in most sites (67 local-var arguments, 19 calls, 2 concatenations) | irrelevant to discovery: the site is the driver call | — | Generator naming is best effort (section 8) |
| Embedded SQL | no `go:embed` in shipped code; 1 tracked `.sql` file outside testdata | sites are found where embedded text is executed (mutation a10) | — | `.sql` files still need a classification check (PR #801 already has one) |
| PostgreSQL-only | `internal/cdc/schema_lock.go:92,102` (`TrySchemaLock`), advisory/row-locking paths | manifest class | a `postgres-only` site in the corpus fails | The truth of the label is only checked if the corpus reaches the site |
| DuckDB | 24 sites in `internal/federated/duckdb*`, `compaction`, `cdc/duckdb_exporter.go` | manifest class | same | Same |

Name collisions on real code: PR #801 lists `entityManager.Query`, `entityQueryService.queryRecords` and `txSession.Query` as sites and classifies them `not-dsql` by hand. The typed inventory does not report them at all, because their resolved symbols are not driver methods.

## 5. Prototype architecture

The prototype lives in `internal/dsqllint/spike/`, a nested Go module (`forma-dsql-spike`). Forma's `go.mod`, `./...`, `make test` and CI never see it. Test-side hooks are in Forma, behind a build tag or an environment variable.

- `sites/`: the inventory.
  - Load the shipped closure: the module packages imported by root, `factory`, `cdc`, `cmd/server`, `cmd/tools` and `cmd/lambda`. That is 38 packages, loaded with `go/packages` (syntax and types for module packages, export data for dependencies).
  - Resolve every call's `*types.Func` and look it up in a driver catalogue keyed by *declaring package* and method. Only a string parameter in the documented position counts.
  - Promote verbatim wrappers and project interface methods to a fixpoint.
  - Emit sites with three candidate ID schemes, the SQL argument, its kind, and builder candidates.
  - Report unresolvable patterns as findings.
  - The analysis level needed was **AST + go/types + a local fixpoint + `types.Implements`**. No SSA, call graph or whole-program data flow was needed.
- `corpus/spike_capture.go` (Forma, active only when `SPIKE_ATTR_OUT` is set) plus a 12-line hook in `corpus.go`/`wire.go`: captures `runtime.Callers` in `Recorder.Match` for mock connections, and on the **client** `net.Conn` write for `WireConn`. `serveWire` runs in its own goroutine, so a capture there sees no production frame. That is from reading `wire.go:35`.
- `attr/`: maps each captured stack to the innermost module, non-test frame inside a site's line span.
- `evidence/`: the manifest parser and the join, with the three coverage levels kept separate.
- `internal/dsqllint/spikefix/` (tag `dsqlspike`): labelled synthetic code for the attribution scenarios that real code lacks.

## 6. Static discovery results

| Mode | Sites | Wrappers | Interface sinks | Findings | Warm time |
|---|---|---|---|---|---|
| verbatim, with `tx-end` | **126** | 16 (16 forwarding calls folded) | 18 | **0** | 0.59–0.61 s |
| verbatim, without `tx-end` | 113 | 16 | 18 | 0 | — |
| taint, without `tx-end` | 182 | 54 | 20 | 2 (`sink-as-value`) | 0.62 s |

By family (verbatim with `tx-end`): pgx-exec 22, pgx-query 35, pgx-begin 6, pgx-copy 1, sql-exec 9, sql-query 23, tx-end 13, wrapper calls 17. By resolution: driver 80, interface 36, wrapper 10.

Compared with PR #801's untyped DB-call detection on the same packages:
- 3 name collisions are correctly absent.
- 6 wrapper-caller sites appear that the PR had no entry for at all. Its `passthrough` class hid them; they are exactly the R2–R12 bypass shape.
- 5 transaction-option *constructors*, such as `guardRowLocking.writeTxOptions`, are not sites. The `BeginTx` that consumes them is.

### Site identity

Each scheme was measured by how many pre-existing IDs disappear under an edit to real code (`internal/cdc/schema_lease.go`, `helpers.go`).

| Edit | `file:line:col` | `pkg.Func:callee#ordinal` | `pkg.Func:callee:hash(SQL-arg expr)` |
|---|---|---|---|
| Insert an unrelated statement | 3 lost | 0 | 0 |
| Reformat (split a call, add blank lines) | 3 | 0 | 0 |
| Insert another call to the same callee earlier | 3 | 0 lost, **but `#1` silently moves to the new call**, and the old call reappears as "new" `#2` | 0 (the new call gets its own ID) |
| Rename the local SQL variable | 0 | 0 | 1 |
| Move the function to another file | 3 | 0 | 0 |
| Rename the function | 0 | 1 | 1 |
| Two sinks on one line | distinct | distinct | distinct |

Recommendation: **expression-hash IDs**. Ordinals look stabler, but they can transfer a classification from one call to another without any visible change, which is unacceptable for a fail-closed inventory. Expression hashes churn only on renames, and that churn is visible: a stale entry plus an unclassified one. Duplicates within a function (identical argument text) fall back to an ordinal suffix, so they share the ordinal weakness in that narrow case.

## 7. Wrappers, batches, transactions

**7.1 Wrappers.** Verbatim forwarding is followed through:
- multiple callers (a12);
- nesting (a17; the synthetic `NestedCaller` goes through two levels);
- another package (a18);
- project interfaces (a16, plus 18 real ones);
- generic type parameters (a26).

A wrapper that *modifies* the SQL (`sql + " FOR UPDATE"`) behaves as follows:
- Its own inner call is a site (a13), whatever the mode.
- A **new caller of an existing modifying helper** is missed in verbatim mode (a13b, `SelectBatchRowIDs`).

Taint mode (local data flow from string parameters) catches a13b, at a price:
- 54 instead of 16 wrappers, and 69 more sites. Most are table-name parameters, such as `insertMainRow` and `upsertChangeLog`.
- One real fail-closed finding (`markFlushedFor`), which would need a code change or an annotation.

Taint mode cannot tell a table-name parameter from a SQL-fragment parameter. Verbatim is the cheaper default. Its gap is new SQL *shapes* fed into existing helpers, and that gap is covered at runtime when exercised.

**7.2 Batch.** Statically, `Batch.Queue` (SQL entry) and `SendBatch` (execution) are both sites. At runtime, pgxmock matches queued SQL at `SendBatch` and **again** when results are read, so the PR's `Recorder` double-records batches. The stack points at `SendBatch`, not the `Queue` line. Forma has no batches today and `meteredTx` refuses them. Recommended policy: keep `SendBatch` as a fail-closed finding until a batch is actually needed.

**7.3 Transaction options.** The SQL that pgx writes (BEGIN with options, COMMIT) is visible only on the wire.
- The real DSQL write path's options arrive as a parameter, so static analysis cannot tell what BEGIN will be sent.
- Client-side capture attributed all 12 wire records correctly. Both write-path BEGINs were plain `begin`; the CDC stage's was `begin isolation level repeatable read read only`.
- dsql-lint flags `begin isolation level read committed` (`transaction_isolation`) and accepts REPEATABLE READ (probe).
- PostgreSQL-only and DSQL behaviour are separated by the corpus running under the `aurora-dsql` engine, plus the `postgres-only` class failing when it is reached.
- `Commit`/`Rollback` need their own `tx-end` family. Without it, 7 of 81 records were unattributable.

**7.4 Method values and indirect calls.** `f := tx.Exec` (a14), a wrapper passed as a value (a28), a call through a function-typed value that returns a DB result type (a21) and an interface with no implementer (a24) are all reported as findings. None is ignored silently.

## 8. Runtime attribution

| Scenario | Expected site | Observed | Correct? | Ambiguous? |
|---|---|---|---|---|
| Direct pgx (`DirectExec`) | `DirectExec` Exec | same | yes | no |
| pgxmock (write path) and sqlmock (CDC reads), real corpus: 49 records | the executing call, e.g. `insertMainRow`, `leaseSchemaLocker.acquire` | 49/49 at that site | yes | no |
| WireConn BEGIN/COMMIT (12 records) | `inWriteTx` BeginTx/Commit, `stageInSnapshot` | same (client-write capture) | yes | no |
| WireConn, capture in `Match` (server goroutine) | — | no production frame | **no** | — |
| Verbatim wrapper, two callers | each caller | each caller | yes | no |
| Nested wrappers | outermost caller | `NestedCaller` | yes | no |
| Two calls in one function | each call | each call (distinct lines) | yes | no |
| Two sinks on one line | each call | resolved by literal match only | yes here | would be **yes** with non-literal arguments |
| Multi-line call | the call | same (span containment) | yes | no |
| Builder-produced SQL (`UsesBuilder`) | exec site; builder `buildDDL` | exec site; builder recovered statically | yes | no |
| Batch | `Queue` sites | `SendBatch` site | function yes, construction site **no** | no |
| Closure in `pgx.BeginFunc` | inner Exec; COMMIT → `BeginFunc` | same | yes | no |
| DSQL production builders (`buildInsertMainStatement` etc.) | builder named in diagnostics | exec site; builder found in 57/61 records | 57 verifiable, 4 asserted | no |

Totals: 81/81 records attributed to the execution site, 0 ambiguous, 0 failed (after `tx-end`), identical under default, `-gcflags=all=-l` and `-race`. Go's `runtime.CallersFrames` expands inlined frames, which is why disabling inlining changed nothing.

**What criterion 7 needs.** "The offending SQL and its generating source" needs the *construction* site. The *execution* site is necessary, because it is what the inventory classifies, but it is not sufficient. A hybrid works:
- the corpus test may still name the generator, as PR #801 does;
- the gate verifies that the name is in the attributed site's static builder closure (the builders in the SQL argument plus range sources, transitively 4 levels);
- a name the gate cannot verify is reported as *asserted*.

On real data, 60/64 of PR #801's generators link statically: 46 directly and 14 through the closure. All 14 init-db DDL builders link to `ensureTables`' Exec. The 4 that do not are the 3 CDC export builders, whose SQL travels in the `exportStage.query` struct field, and `writeTxOptions`, a forwarded parameter.

## 9. Static-to-dynamic evidence

The join (`evidence/`) fails closed on each required check, in unit tests on a labelled synthetic inventory (10/10 pass):
- an unclassified or new site;
- a stale entry;
- a `dsql` site with no evidence, **even when a sibling site in the same function has evidence**;
- a record with no site;
- a PostgreSQL-only or DuckDB site reached by the corpus;
- ambiguous attribution;
- `uncovered` without an `#issue`;
- an unlisted main package;
- any unsupported-pattern finding.

**Real data.** With a *generated sample* manifest (not a reviewed classification), the join passes:
- 126/126 sites classified;
- 37 `dsql` sites, all with evidence;
- 61/61 records attributed, 55 of them to `dsql` sites and 6 to boundaries.

Three negative controls on real data each fail with the expected message:
- relabel `insertMainRow` as postgres-only;
- delete the `renew` entry;
- claim `readStoredState` is `dsql`.

Coverage levels, kept apart:
- **Static inventory coverage:** 126/126 discovered sites classified, by construction of the gate.
- **Dynamic execution coverage:** 37/37 `dsql` sites have evidence, but only because the sample made "exercised" the definition of `dsql`. The honest figure is that of the 64 generators PR #801 renders, **11 are linked to sites with no execution evidence**: `withTx`/`ensureTables`, `declaredindex` reads and `CreateSQL`, `buildUpdateMainStatement` and six widthaudit census builders. Their 45 corpus statements come from direct generator calls, which prove lint coverage but not execution.
- **SQL compatibility coverage:** the 106 statements dsql-lint parses today, subject to its limits: it flags neither advisory locks nor `FOR UPDATE`, and it reports `DROP INDEX CONCURRENTLY` as a parse error.

## 10. Adversarial tests

All mutations are synthetic, applied to a disposable worktree and reset after each case. The same mutations were replayed against PR #801's `TestInventoryCoversRepository`.

| Case | Typed inventory | PR #801 scanner |
|---|---|---|
| a1–a6: comment prefix, `\v`, lowercase, CTE column list, `SELECT(1)`, fragments (with a direct `tx.Exec` in the new function) | detected | flagged (by method name) |
| a7: keyword from a helper | detected | flagged (by method name) |
| a8: constant from a new package | detected | flagged |
| a9: extra `ExecContext` inside the classified `acquire` | **detected** | **not flagged** |
| a10: `go:embed` template | detected | flagged |
| a11: unrelated type with `Exec(ctx, string, ...any)` | correctly ignored | flagged (false positive) |
| a12, a17, a18: forwarding helper, nested, cross-package | detected | flagged |
| a13: new modifying wrapper | detected | flagged |
| a13b: new caller of existing modifying helper | not detected (documented; taint mode detects) | not flagged |
| a14, a21, a24, a28: method value, func-value call, no implementer, wrapper as value | rejected conservatively | flagged |
| a15a: new shipped package with a sink | detected | flagged |
| a15b: package no shipped code imports | correctly ignored (outside scope) | flagged |
| a15c: new main package | detected (main-package check) | not flagged |
| a16, a26, a27: project interface, generic type parameter, `PgConn().Exec` | detected | flagged |
| a19, a20, a22, a23, a29: two sinks on one line, batch, BeginFunc closure, database/sql, BeginTx options | detected | flagged |
| a25: reflection | not detected (documented) | flagged (literal) |
| a30: new SQL through a struct field into an existing site | not detected (documented) | flagged (literal) |
| a31: new branch changing SQL at an existing site | not detected (documented) | not flagged |
| **g1a**: generator (no DB call) → new caller of the existing wrapper `scanRowPage` | detected | flagged |
| **g1b**: same, generator text starts with `\v` | **detected** | **not flagged** (the R12 shape in real code) |
| **g2a**: existing rendered site `GetChangeLogStats` switched to a new generator | runtime only (documented) | flagged |
| **g2b**: same with `\v` text | runtime only | **not flagged** |

Totals for the typed inventory over 38 cases: 26 detected, 4 rejected conservatively, 2 correctly ignored, 4 not detected (documented), 2 runtime-only (documented), **0 silent**. PR #801's scanner did not flag 6 of the 38 and raised 1 false positive.

The g-cases are the fair comparison for the R2–R12 shape. a1–a8 also call `tx.Exec` in the new function, which PR #801 catches by name.

**Runtime-only, verified.** g2 was applied with parse-invalid SQL (`\vSELEC …`) and PR #801's full gate run on the mutated tree. It failed with `parse_error` attributed to `internal/cdc/helpers.go:GetChangeLogStats`. So a variant at an exercised site is linted whatever its text. A `FOR UPDATE NOWAIT` variant passed, because dsql-lint does not flag it; that is a linter limit, not a coverage one.

## 11. Statement structure versus semantics

- **Structure.** The capture records (`records.jsonl`) carry site, origin label, SQL, mechanism and stack as data. No metadata is rebuilt from SQL comments, and each record is exactly one driver call.
- **dsql-lint gives no emptiness signal.** Empty, whitespace-only (`\v\f`), comment-only (nested) and `;;;` inputs all exit 0 with no diagnostics, just like a valid statement. It also reports two statements on one line both at line 1, so line-based origin mapping requires one statement per line.
- **DuckDB's parser as an emptiness oracle.** DuckDB is already a direct dependency and is linked into the corpus test binaries.
  - `PrepareContext` returns a distinct empty-query error, and classified all 12 probes correctly, including nested comments and `\v`/`\f`.
  - It cannot judge validity: it raises parser errors on 3/106 real corpus statements (pgx's `begin … read only`, `CREATE INDEX CONCURRENTLY`), and on DSQL's `CREATE INDEX ASYNC`.
  - The usable rule is therefore: *empty means fail; a parse error is deferred to dsql-lint*.
- **Minimum validation needed:**
  1. every `dsql` site has at least one record (structural, already in the join);
  2. each record is non-empty under the DuckDB oracle;
  3. dsql-lint's positive control fires in every run.

  This needs no new grammar. The residual risk is a lexical difference between DuckDB and PostgreSQL on exotic comment or whitespace forms. It was not measured beyond the probes.

## 12. Performance and complexity

| Metric | Value |
|---|---|
| Package load, empty `GOCACHE` | 17.3 s (17.9 s wall; compiles dependencies, including DuckDB cgo, for export data) |
| Package load, warm | 0.26–0.28 s; whole static run 0.59–0.61 s wall (5 runs). Target was ~10 s |
| Analysis | 86–97 ms |
| Peak RSS (analyzer process) | 89–100 MB, from `VmHWM`; excludes `go list` children |
| Sites | 126 (verbatim) / 182 (taint) |
| Supported vs unresolved on real code | 126 / 0 (verbatim) |
| Runtime capture overhead | within noise (corpus run 5.3–5.8 s with or without) |
| Attribution | 81 correct, 0 ambiguous, 0 failed |
| Mutation detection | 38 cases, 0 silent misses |
| Identity churn | see section 6 |
| PR #801 scanner | 0.92 s; 908 non-test + 1,629 test lines, plus an 89-line statement counter |
| Prototype | 1,096 non-test lines in `sites/` (including experiment-only taint mode, three ID schemes and the builder closure), 135 in `attr/`, 177 in `evidence/`; 551 test lines |

The prototype is not smaller than the scanner in lines. The difference is in what has to grow:
- **The scanner** encodes PostgreSQL's command grammar and is open-ended. Each review round added patterns.
- **The typed catalogue** is the driver API, about 40 methods, and changes only when Forma adopts a new driver entry point.

## 13. Decision gates

| Gate | Result | Evidence |
|---|---|---|
| **A. Sink discovery** | **PASS** | 126 sites on real code, 0 unresolved. The unsupported set is closed and fails closed: `sink-as-value`, `indirect-db-call`, `unresolved-interface`. Name collisions were excluded on real code (3) and synthetic code (a11). |
| **B. New-site detection** | **PASS** | Every new execution site was detected (26) or rejected (4), and none was missed silently. This holds for execution sites only: a new SQL variant at an existing site is not a new site (a30, a31, g2). |
| **C. Wrappers, batch, transactions** | **CONDITIONAL** | Verbatim wrappers and interfaces work without SSA or a call graph. The conditions are (1) choose verbatim, or taint plus an annotation for `markFlushedFor`; (2) a `tx-end` family; (3) client-side wire capture for SQL that pgx writes; (4) keep `SendBatch` a finding until batches exist, because runtime attribution lands on `SendBatch`, not `Queue`, and pgxmock double-matches. |
| **D. Runtime attribution** | **PASS** | 81/81 to the execution site, unaffected by inlining or `-race`. Generator naming for criterion 7 is a verified-or-asserted hybrid: 57/61 records and 60/64 generators are verifiable. |
| **E. Fail-closed** | **CONDITIONAL** | Every join check fails closed, both in unit tests and in three real-data negative controls. "Zero meaningful SQL" holds structurally per site. The per-record emptiness check, the DuckDB oracle (12/12 probes), is demonstrated but not wired into the join. |
| **F. Cost and maintenance** | **CONDITIONAL** | Warm 0.6 s, cold 17.9 s. No new module (`golang.org/x/tools` is already in `go.mod`, as indirect). The catalogue is finite. The conditions: expression-hash IDs churn on function renames, which the 100-line rule makes common, so a manifest regenerate/rename helper is needed; and 45 generator-only corpus statements must become executed ones before every `dsql` site can show evidence. |

## 14. Comparison with PR #801's scanner

| | PR #801 scanner | Typed call sites + evidence |
|---|---|---|
| Unit of inventory | a symbol whose text looks like SQL, or that calls a method with a DB-like name | a resolved driver call |
| Grows with | PostgreSQL syntax and Go string construction | the driver API |
| New SQL in a classified function | missed (a9) | detected |
| Generator text it cannot read (`\v`) | missed (g1b, g2b) | detected (g1b); runtime-linted when exercised (g2b) |
| New SQL variant at an existing site | flagged when the text is recognised (a30, g2a) | runtime only |
| Name collisions | manual `not-dsql` entries | none |
| Origin of a diagnostic | asserted by the test author | execution site derived; generator verified for 57/61 |
| Statement counting | three hand-written lexers | per-call records plus an emptiness oracle |
| False positives seen in the spike | a11, and the spike's own analyzer (`"Begin"` in the catalogue read as a `BEGIN` statement; `load.go` read as fragments), which needed `not-dsql` entries on the spike branch | none on real code |

The one place the scanner does better is a30/g2a: a new SQL *text* that it can recognise, reaching an existing site. The typed design trades that for runtime linting at exercised sites. The trade loses only for variants that are both unexercised and recognisable text.

## 15. Recommended architecture

Proceed with the typed call-site inventory, with these settings:
- verbatim wrapper following;
- expression-hash IDs;
- the `tx-end` family;
- `SendBatch` as a finding;
- a manifest with `dsql`, `postgres-only`, `duckdb`, `uncovered #issue`, `boundary` and `unshipped-main` lines.

Join it to runtime evidence from the existing recorders. Capture stacks in `Recorder.Match` for mock connections, and on the client side of `WireConn`. Add the DuckDB emptiness check per record. Name generators with the verified-or-asserted hybrid.

State the guarantee as follows.

> Every driver call in shipped code is classified. Every `dsql` site was exercised under `aurora-dsql`, and everything it sent was linted. A new SQL variant at an existing site is linted only if the corpus exercises it.

## 16. Risks and open questions

1. **Variants at existing sites.** New branches, struct-fed SQL and new callers of modifying helpers are outside the static guarantee. Protection depends on corpus representativeness, which is the same limit PR #801 has, but now stated explicitly. Owner question: is this the acceptable reading of criterion 4?
2. **Migration cost.** 45/106 statements come from direct generator calls and provide no execution evidence. Converting them (`ensureTables`, `declaredindex.Ensure`, widthaudit) needs recording connections, and mock fidelity matters: zero-value rows, and pgxmock's batch double-match.
3. **Manifest churn.** Function and local renames re-key sites. Unmeasured: how often this happens across a month of real commits. A replay over `git log` would measure it.
4. **Unmeasured.** DuckDB versus PostgreSQL lexing beyond 12 probes; behaviour under `go test -cover` instrumentation; build-tagged files outside the default build; and SQL that cgo or `unsafe` code sends.
5. **Taint mode.** Taint mode closes a13b but cannot tell table names from SQL fragments. Owner decision.

## Reproduction

From the spike worktree:

```bash
internal/dsqllint/spike/run_spike.sh   # OUT=<dir> to choose the output directory
```

Individual steps, with `GOFLAGS=-buildvcs=false GOTOOLCHAIN=go1.26.0+auto`:

```bash
# baseline (PR #801 scanner)
go test -tags dsqlspike -count=1 -run TestSpikeBaselineScan -v ./internal/dsqllint
# prototype build, unit tests and static inventory
cd internal/dsqllint/spike && GOFLAGS=-mod=mod go test ./sites ./evidence && GOFLAGS=-mod=mod go run ./cmd/sqlsites -dir ../../.. -mode verbatim -out /tmp/sites.json
# runtime capture (repeat with -gcflags=all=-l and -race)
SPIKE_ATTR_OUT=/tmp/cap go test -count=1 -run '^TestDSQLCorpus' ./cmd/tools ./internal ./internal/cdc ./internal/declaredindex ./internal/federated ./internal/widthaudit
SPIKE_ATTR_OUT=/tmp/fix go test -tags dsqlspike -count=1 ./internal/dsqllint/spikefix
go run ./cmd/attribute -sites /tmp/sites.json -records /tmp/cap/records.jsonl -root ../../.. -out /tmp/attr.json
go run ./cmd/evidence -sites /tmp/sites.json -outcomes /tmp/attr.json -manifest ../../../docs/spikes/dsql-typed-callsite-data/manifest-sample.txt
# mutations (needs a disposable worktree; the harness resets it after every case)
SPIKE_MUT_DIR=<worktree> go test ./sites -run 'TestMutations$|TestMutationsPRScanner' -count=1 -v -timeout 60m
# DuckDB emptiness oracle (inputs built from a kept PR #801 corpus; see data/)
SPIKE_PARSE_IN=<parse-inputs.json> go test -tags dsqlspike -count=1 -run TestSpikeDuckDBParseOracle -v ./internal/dsqllint/spikefix
```
