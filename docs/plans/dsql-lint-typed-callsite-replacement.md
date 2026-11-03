# Plan: replace PR #801's inventory with typed SQL call-site coverage

This plan implements `docs/architecture/adr-dsql-typed-callsite-coverage.md` for #740. It was written before implementation.

- **Base:** `origin/main` at `7ebe2b5`, unchanged since PR #801 was opened.
- **Sources of reused code:** PR #801 at `9776c2b` and the spike at `ab3f1ea`. Neither is merged or cherry-picked wholesale.

## 0. Issue map (owner decision, 2026-11-03)

The work is split into six issues, sequenced by GitHub "blocked by" links:

| Issue | Content | Blocked by |
|---|---|---|
| #740 (1/5) | Lint baseline (M1): pinned dsql-lint, the production SQL corpus as typed records, exact-count known findings, emptiness check, CI wiring. No inventory; criterion 4 is not claimed | — |
| #803 | Engine seam: a DSQL-subset shared core, a narrow engine interface, engine packages, a DDL seam carrying #690's strategy. First step: an executed update/merge corpus unit | #740 |
| #804 (2/5) | Typed call-site discovery (M2) | #740 (can run in parallel with #803) |
| #805 (3/5) | Audited site manifest (M3), done once on #803's layout | #804, #803 |
| #806 (4/5) | Attribution and evidence join (M4); re-keys known findings | #805 |
| #807 (5/5) | Corpus migration and close-out (M5 + the remainder of M6) | #806 |

```
#740 ──► #803 ──┐
   └───► #804 ──┴─► #805 ──► #806 ──► #807
```

Why this order:
- #803 moves most SQL into new packages, which re-keys almost every site ID. The manifest audit (#805) is therefore done once, after it.
- #740's baseline does not depend on site IDs. It lands first, as the safety net for #803.
- M6's main task, deleting PR #801's SQL-text scanner, does not exist on `main`, so the rest of M6 is folded into #807.

## 1. Owner decisions

### Already made (D1–D7)

1. Typed discovery.
2. Verbatim wrapper following.
3. Keep runtime evidence.
4. Report the four coverage dimensions separately.
5. Fail closed.
6. Reuse PR #801's infrastructure.
7. No further regex rounds on #801.

### Design choices this plan makes within D1–D7

- `Batch.Queue` and `SendBatch` are refused as unsupported forms; Forma ships no batches.
- `Commit` and `Rollback` form the `boundary` class.
- Site IDs are expression hashes. A duplicate key fails until it carries a `//dsqllint:site` directive.
- Emptiness is checked by DuckDB's empty-query result only.
- Known findings match on exact counts.
- `executed` is the default evidence kind.

### Decision requests (owner)

| ID | Question | Recommendation | Blocks |
|---|---|---|---|
| **OD-1** | Approve the operational interpretation of criterion 4 (ADR §5–6): new *sites* are caught statically; new *variants* at existing sites only when the corpus represents them | Approve | #804 onward. Does not block #740 |
| **OD-2** | Which tool subcommands are supported on DSQL: validate-schema-consistency, baseline-schema, apply-schema, the widthaudit census, manifest-reconcile, build-unique-keys | If undecided, classify their sites `uncovered #802`. Never classify them `dsql` without evidence | Manifest audit (#805); the default is safe |
| **OD-3** | Forma-specific blocking rules | Block `FOR SHARE` and `FOR NO KEY UPDATE`, which AWS documents as unsupported. Block advisory-lock calls in the DSQL corpus under Forma's own engine contract (`docs/aurora-dsql.md`, #633), not on an AWS claim. Do **not** block `FOR UPDATE` or `FOR KEY SHARE` | #807; can follow later |
| **OD-4** | May a PR add a new DSQL-reachable site as `uncovered #issue`? | Yes, if the issue is open and names the site. Review it like code | #805 |
| OD-5 | Executed coverage of the update/merge write transaction | **Resolved:** it is the first step of #803, before the write path is refactored | — |
| OD-6 | Delivery topology | **Resolved by the owner:** six issues (§0) instead of one replacement PR | — |

## 2. Coverage and evidence model

### 2.1 Scope

Scope S is the non-test files of module packages in the import closure of the entry points `.`, `./factory`, `./cdc`, `./cmd/server`, `./cmd/tools` and `./cmd/lambda`. They are loaded in CI's build context: linux/amd64, `CGO_ENABLED=1`, no tags, `GOTOOLCHAIN=go1.26.0+auto` via `$(GOENV)`. Today that is 38 packages, and none has a tagged file.

The gate fails when:
- a `main` package is neither an entry point nor `unshipped-main`;
- a file in S is excluded by a build constraint;
- `internal/dsqllint` or `golang.org/x/tools` is imported by S.

### 2.2 Manifest

The manifest is `internal/dsqllint/manifest.txt`. It replaces `inventory.txt`, with one line per item:

```
<class> <site-id> [evidence=executed|rendered] [issue=#N] -- <reason>
unshipped-main <import-path> -- <reason>
sql-file <path> <dsql|postgres-only|duckdb|uncovered> [issue=#N] -- <reason>
```

| Class | Meaning | Required evidence | Linted | Justification | Issue required | The gate fails when |
|---|---|---|---|---|---|---|
| `dsql` | Reachable on `aurora-dsql`; its SQL is checked | `executed` (default) or `rendered` (only with a statically linked generator, §2.4) | yes | reason | no | no evidence of the declared kind; `rendered` declared without a linked generator; any of its records fails G3 |
| `postgres-only` | Runs only under the PostgreSQL engine | none | no | reason naming the engine switch (e.g. `rowLockingFor`, `markFlushedFor`) | no | any executed record under `aurora-dsql` attributes to it |
| `duckdb` | Sent to DuckDB, never to PostgreSQL or DSQL | none | no | reason | no | the DSQL corpus reaches it |
| `uncovered` | DSQL-reachable, not yet evidenced | none | no | reason | **yes** (`#N`, open) | the DSQL corpus reaches it (reclassify it `dsql`); the issue is missing |
| `boundary` | `Commit`/`Rollback` (the `tx-end` family only) | optional | yes, when recorded | none | no | used on a non-`tx-end` site |

In every class, the gate fails on a duplicate line, an unknown class, a missing reason, a stale ID, or a site that has no line.

**Initial manifest.** A generated draft is only a starting point. Every line is audited at source level in #805:
- the reviewer reads each site and records the engine switch or the reason;
- the audit table goes into the PR description;
- evidence does *not* decide the class. The spike's sample manifest is circular and is discarded.

### 2.3 Site identity

The key is `<pkg-rel>.<Func>:<callee>:<h8>`.
- `Func` is the enclosing `FuncDecl` (`Type.Method`); closures inherit it.
- `callee` is `pkgname.Recv.Method`, or `wrap:<file>:<Func>` for a wrapper.
- `h8` is the first 8 hex digits of sha256 over `go/types.ExprString` of the SQL argument, plus `|` and the options expression for a begin.

| Change | Effect | Safe? |
|---|---|---|
| Reformat, unrelated statement, move to another file | no change (measured: 0 lost) | yes |
| A new call to the same receiver anywhere in the function | new key → unclassified | yes |
| Rename the SQL variable or the function | old key stale, new key unclassified | yes, visible |
| Two sinks on one line | distinct keys when the arguments differ | yes |
| Identical key twice in one function | **fails** until one carries `//dsqllint:site <name>` (the key becomes `…:@name`). There are 0 cases today | yes |
| An existing ID moving to a different call | impossible without ordinals, which are not used | yes |

**Regeneration.** `go run ./cmd/dsqllint manifest -write` rewrites the manifest as follows:
- existing lines are kept verbatim;
- new sites get `unclassified <id> -- TODO`, which the parser rejects, so the gate stays red;
- stale lines are removed and printed with their old class;
- a stale/new pair in the same function and callee, or with the same hash in a renamed function, is printed as a *possible rename*. The class is never carried over.

### 2.4 Evidence records and origin

The corpus writes one JSONL record per statement:

```json
{"kind":"executed|rendered","sql":"...","engine":"aurora-dsql","mechanism":"pgxmock|sqlmock|wire-client","unit":"...","seq":3,"label":"file.go:Symbol","frames":[...]}
```

- An **executed** record comes from `Recorder.Match` (on the caller's goroutine) or from a client-side `net.Conn` write on `WireConn`. The server goroutine has no production frames.
- A **rendered** record comes from `corpus.Rendered(t, generator, sql)`.
- A rendered record **never** counts as execution evidence.

Origin confidence, reported per diagnostic and never upgraded:

| Level | Rule |
|---|---|
| `exec-site` | Executed record attributed to the innermost module frame inside a site's call span. Always present for executed records |
| `verified-builder` | The label is a direct builder in the site's SQL argument |
| `verified-match` | The executed SQL equals, byte for byte, a rendered record of a generator in the site's builder closure. This gives init-db per-generator origins |
| `verified-closure(d)` | The label is in the site's transitive closure at depth d ≤ 4, including typed struct-field links |
| `asserted` | The label is present but unverifiable (e.g. TxOptions arriving through a parameter) |

Failures:
- a rendered generator that links to no site;
- an executed record that maps to no site, or to more than one;
- a known finding keyed on an `asserted` origin.

## 3. Known findings and SQL policy

The file is `internal/dsqllint/known_findings.txt`, with lines of the form:

```
<site-id> <origin|-> <rule> <count> #<issue> -- <reason>
```

The gate compares exact counts per key. It fails on:
- more or fewer diagnostics than listed (a new or a disappeared instance);
- a rule that is new at a known site or origin;
- a diagnostic from an unlisted site;
- an entry whose site is not `dsql`, is stale, has no `#issue`, or uses a wildcard;
- a key whose origin is `asserted`.

The baseline, measured on PR #801 and to be re-keyed in #806, is 15 diagnostics, all #690:

| Generator | Rule | Count |
|---|---|---|
| `mainColumnIndexDDL` | `index_async` | 10 |
| `eavIndexDDL` | `index_async` | 2 |
| `keyRowIndexDDL` | `index_async` | 1 |
| `withTx` | `multi_ddl_transaction` | 1 |
| `Index.CreateSQL` | `index_concurrently` | 1 |

**Temporary keys in #740.** Until #806 there are no site IDs, so #740 keys known findings on *corpus origin label + rule + exact count*. Those labels are asserted by the corpus tests. The rule that an `asserted` origin cannot key a finding takes effect in #806, which re-keys every entry to *site + verified origin + rule + count*. Exact counting holds throughout.

Compatibility checking stays separate from discovery and runtime behaviour. dsql-lint 0.2.22 limitations (documented, measured):
- it does not flag advisory locks, `FOR UPDATE` or `NOWAIT`;
- it reports `DROP INDEX CONCURRENTLY` as a `parse_error`;
- it gives no emptiness signal;
- it reports two statements on one line at the same line.

## 4. Coverage dimensions reported every run

1. **Static:** sites discovered, classified per class, unsupported-form findings (must be 0).
2. **Rendered:** generators rendered; generators linked to a site.
3. **Executed:** `dsql` sites with executed evidence / `dsql` sites requiring it.
4. **Lint:** records linted / records; accepted and new diagnostics.

These are never combined into one percentage.

## 5. Corpus migration matrix

There are 106 statements from 64 generators today: 61 executed and 45 rendered.

| Source (generators) | Statements now (exec/rendered) | Dialect | Proposed | Origin after | Migrate | Risk | Validation |
|---|---|---|---|---|---|---|---|
| init-db table transaction: `withTx` BEGIN/COMMIT; `schemaRegistryDDL`, `entityMainDDL`, `eavTableDDL`, `changeLogDDL`, `eavIndexDDL`(2), `mainColumnIndexDDL`(10), uniquekey `keyTableDDL`/`keyRowIndexDDL`/`stateTableDDL`, rowguard/schemalease `tableDDL`, schemaledger `pointerDDL`/`generationDDL`/`attributeDDL` | 0/26 | DSQL | **executed** via `ensureTables` through `withTx`, with a ~10-line seam (`withTx` takes a `Begin(ctx)` interface); rendered DDL kept for origins | exec-site + verified-match | **yes** | high: #690 will restructure these transactions, and a test-assembled `corpus.Transaction` would keep asserting the old boundaries | T-33 |
| init-db registration: `loadRegisteredSchemaIDs`, `registerSchemas` | 2/0 | DSQL | executed | exec-site | no | low | T-32 |
| Declared indexes: `catalogQuery`, `tableSchemaQuery`, `Index.CreateSQL` | 0/3 | DSQL | **executed** via `declaredindex.Ensure` on pgxmock (its `Conn` interface already exists); `CreateSQL` also rendered | exec-site + verified-match | **yes** | medium: cheap, and shows autocommit `CONCURRENTLY` as actually sent | T-32 |
| Write transaction boundaries: `writeTxOptions` BEGIN ×4, `inWriteTx` COMMIT ×4 | 8/0 | DSQL | executed (wire-client) | exec-site; options `asserted` | no | low | T-16, T-17 |
| Write DML: `guardUpsertSQL`, `upsertChangeLog`, `nextRowVersion`, `deleteRecordInTx`, `batchDeleteInTx`, `insertEAVAttributes`, `replaceEAVAttributesScoped`, `buildRequeueMainStatement`, unique-key reads and writes (7), `lockRowInTx`, `checkRowGuard`, `txClock`, `buildInsertMainStatement` | 34/3 | DSQL | executed; the 3 rendered branch variants (three-row guard, live and deleted inserts for every typed family) stay rendered as **extra** variant coverage | exec-site / verified-builder | no | low | T-32 |
| **Update/merge transaction** (`MergePersistentRecord`, batch merge, `updateMainRow`) | 0/2 (`buildUpdateMainStatement` rendered only) | DSQL | **new executed unit** under `aurora-dsql`, added as #803's first step; rendered variants kept | exec-site / verified-builder | **yes (new)** | high: the critical write path has no evidence today | T-34 |
| CDC lease, mark and reads (`acquire`, `renew`, `deleteLease`, `MarkFlushedVersions`, flush and init reads ×7) | 11/0 | DSQL | executed (sqlmock) | exec-site | no | low | T-32 |
| CDC export snapshot: `stageInSnapshot` BEGIN/COMMIT | 4/0 | DSQL | executed (wire-client) | exec-site | no | low | T-17 |
| CDC export queries: `buildChangeLogQuery`, `buildMainEntityQuery`(2), `buildEAVQuery`(2) | 0/5 | DSQL | **rendered**, linked to the `appendStageRows` site through a typed struct-field link (`exportStage.query`; validated in planning). Executing them would need extended-protocol support in `WireConn` for little gain; runtime belongs to #739 | verified-closure | no | low–medium | T-22, T-32 |
| Federated dirty-ID reads (2) | 2/0 | DSQL | executed | exec-site | no | low | T-32 |
| widthaudit census builders (6) | 0/6 | depends on OD-2 | rendered, linked through the verbatim wrappers' callers; `uncovered #802` until OD-2 is decided | verified-builder | no | low (pure read-only SELECTs) | T-22 |

Totals: 61 statements stay executed, 29 move to executed, and 16 stay rendered and linked, which accounts for all 106. The new update unit adds statements on top of that. No current generator loses coverage (T-32).

Not every one of the 45 rendered statements is converted to execution:
- for pure builders whose variants are rendered, execution adds only the call site, which the static link already proves;
- execution matters where the *sequence or boundary* is produced by code (init-db, declared indexes) or where no evidence exists at all (update/merge).

## 6. Closing the conditional gates

**Gate C (wrappers, batch, transactions).**
- **Wrappers and interfaces.** Port the spike's verbatim fixpoint and `types.Implements` resolution, with no taint mode.
- **Begin and BeginTx.** Record the options expression.
- **Commit and Rollback.** These are the `boundary` sites.
- **SQL that pgx writes.** Captured client-side on `WireConn`.
- **Unsupported forms fail closed.** These are refused as findings:
  - sinks used as values;
  - calls through function values that return driver results;
  - interfaces with no implementer;
  - `Batch.Queue` and `SendBatch`;
  - unknown driver imports.
- **Tests:** T-03, T-05, T-06, T-10, T-16, T-17.
- **Exit:** all green, plus 0 findings on `main`.

**Gate D (attribution).**
- Origin levels as in §2.4.
- Typed field linking, which resolved 3 of the 4 unlinked generators in planning. The fourth (`writeTxOptions`) is pgx-written BEGIN options and stays `asserted`.
- Diagnostics print the level.
- **Tests:** T-19 to T-23.
- **Exit:** every PR #801 generator is verified, or explicitly `asserted` with a reason.

**Gate E (fail-closed validation).**
- **Rule.** Each record goes through the DuckDB empty-query check, via `database/sql` `PrepareContext` on an in-memory DuckDB. Empty fails. Anything else, including a DuckDB parser error, proceeds to dsql-lint.
- **Planning evidence.**
  - Correct on 38/38 probes: whitespace including `\v\f`, nested comments, `;;;`, quotes, dollar quoting, `E''` strings, DSQL syntax, transaction statements and multiple statements.
  - Parser errors on 3/106 real statements, all valid PostgreSQL/DSQL.
- **Zero meaningful SQL** also fails structurally: a `dsql` site with no record, zero units, or a missed positive control.
- **Tests:** T-25 to T-27.
- **Exit:** the probe set is a unit test.

**Gate F (maintenance).**
- **Tooling.** The regeneration command and rename hints (§2.3), and the catalogue completeness test against the pinned driver versions.
- **Scope checks.** The entry-point and main-package checks, and the build-tag check.
- **Toolchain.** Everything runs through `$(GOENV)`.
- **Test instrumentation.** Attribution was re-verified under `-cover`.
- **Exit:**
  1. CI's `DSQL lint gate` step is ≤ 60 s on a run after `make lint`. Measured in the PR; it is 13 s today.
  2. A replay of the regeneration over the last 50 commits on `main` reports how many keys changed. It is recorded in the PR; there is no threshold, it is input for OD-4.
  3. T-11, T-12 and T-15 are green.

## 7. Delivery

The issue map is in §0. Each issue is delivered by its own PR (or PRs) from `main`.

**Review contract**, stated in every PR description. A finding blocks only if it cites a failing T-test or a violated G1–G3. A shape outside the guarantees (ADR §6) goes into the non-guarantee documentation and an issue, not into the PR.

**#740, the lint baseline (M1).**
- **Port from PR #801**, adapting to typed records:
  - `internal/dsqllint/tool.go`;
  - `ParseLintOutput`, `FormatFinding`, `runLint` and `selfTest`;
  - `testdata/regression/*`;
  - `cmd/dsqllint/main.go`;
  - `corpus/corpus.go` (`Export`, `Recorder`) and `corpus/wire.go`;
  - all nine `*dsql_corpus*_test.go` files (106 statements, 64 generators);
  - the `tableTransactionDDL` refactor in `cmd/tools/init_db.go`;
  - the Makefile `lint-dsql` and `all` targets, the CI lint-job step, `dsql_lint_guard_test.go`;
  - `docs/dsql-lint.md`, which must state that there is no inventory yet.
- **New:**
  - a typed JSONL record sidecar (kind, SQL, engine, mechanism, unit, sequence, label), replacing origin comments and statement counting;
  - the DuckDB empty-statement check;
  - exact-count known findings on temporary label keys (§3);
  - a per-unit engine stamp.
- **Do not port:** `scan*.go`, `corpus/statements.go`, `inventory.*` or count-free `Judge`.
- **Exit:** T-25 to T-27, T-29 (label-keyed), T-32 (no loss of the 106 statements and 64 generators), T-35 to T-37.

**#803, engine seam.** See the issue for scope. Exit: its acceptance criteria. #740's corpus stays green, and every change to the known findings is listed.

**#804 to #807.** Each issue states its scope and exit tests. The spike's `sites`, `attr` and `evidence` packages are reimplemented to repository standards (≤500-line files, ≤100-line functions, wrapped errors). The mutation shapes become fixture packages under `internal/dsqllint/testdata/`.

**Definition of done for #740's original scope:**
- #807 is closed with T-01 to T-40 green in CI;
- the #805 audit is reviewed;
- #802 lists every `uncovered` site;
- the ADR is accepted.

**PR #801.** It is superseded by #740's baseline PR together with #803–#807. The owner closes it with links once #740's baseline PR is open.

## 8. Acceptance test matrix

"B" means blocking. AC numbers refer to #740's original acceptance criteria.

Tests by issue:
- **#740:** T-25 to T-27, T-29 (label-keyed), T-32 (no loss), T-35 to T-37.
- **#803:** keeps #740's tests green.
- **#804:** T-03 to T-12, T-39; T-01 and T-02 at the inventory level.
- **#805:** T-01, T-02 and T-13 to T-15 as gate failures.
- **#806:** T-16 to T-24, T-38; T-29 re-keyed to sites.
- **#807:** T-28 to T-34, T-40.

| ID | Input or mutation | Expected | Enforced by | B | AC |
|---|---|---|---|---|---|
| T-01 | New function with `tx.Exec(ctx, "<DDL>")` in a shipped package | unclassified site → fail | discovery, manifest | B | 4 |
| T-02 | Extra `ExecContext` inside a classified function (spike a9) | new key → fail | discovery, ID | B | 4 |
| T-03 | Verbatim helper with 2 callers; nested; cross-package; via a project interface; generic type parameter | each caller is a site; inner forwarding is folded | wrappers, interfaces | B | 4 |
| T-04 | Helper that modifies SQL: new helper → inner site; new caller of an existing modifying helper | first: fail (new site). Second: passes, asserted as a documented non-guarantee | discovery | B | 4 |
| T-05 | Method value; method expression; wrapper passed as a value; call through a function value returning `CommandTag`; interface without implementer | finding → fail | discovery | B | 4 |
| T-06 | Pinned pgx or `database/sql` handle gains an unlisted method; shipped code imports `lib/pq` | completeness or driver-import failure | catalogue test | B | 4 |
| T-07 | Project type with `Exec(ctx, string, ...any) (pgconn.CommandTag, error)` | no site | discovery | B | 4 |
| T-08 | New package with a sink imported by `cdc`; package outside S; new `cmd/x` main | site; not in scope; fail (unlisted main) | scope | B | 4 |
| T-09 | `//go:build !linux` file in S | fail | scope | B | 4 |
| T-10 | `Batch.Queue` + `SendBatch` | finding → fail; `SendBatch` never counted as evidence for `Queue` | discovery | B | 4 |
| T-11 | Identical key twice in one function; then with a directive; then a same-callee call inserted earlier | fail; distinct; no class transfer | ID | B | 4 |
| T-12 | Reformat, unrelated statement, move file; rename function or variable | 0 churn; stale+new pair with a rename hint | ID, regeneration | B | 4 |
| T-13 | Remove a manifest line; add a line for a missing site | unclassified / stale → fail | manifest | B | 4 |
| T-14 | Unknown class; missing reason; `uncovered` without `#N`; `boundary` on a non-`tx-end` site; `rendered` with no linked generator | fail at parse or join | manifest | B | 3, 4 |
| T-15 | `manifest -write` with a new site | new line `unclassified`; gate still red; no class assigned | regeneration | B | 4 |
| T-16 | Write path on `aurora-dsql` | BEGIN record (`begin`) attributed to the `inWriteTx` BeginTx site; a READ COMMITTED begin fixture fails with `transaction_isolation` | wire capture, lint | B | 1, 5 |
| T-17 | BEGIN/COMMIT/ROLLBACK via `WireConn` | client-write mechanism; correct sites (12/12 today) | attribution | B | 7 |
| T-18 | Corpus reaches a `postgres-only`, `duckdb` or `uncovered` site | fail (dialect contradicted) | join | B | 5 |
| T-19 | Two sinks on one line with non-literal arguments, both executed | ambiguous → fail | attribution | B | 7 |
| T-20 | Executed record with no module frame at a site | fail | join | B | 3 |
| T-21 | `dsql` site never executed while a sibling in the same function is | fail for that site | join | B | 3 |
| T-22 | Rendered record for an `executed` site; rendered generator linked to no site | not counted / fail | join | B | 3 |
| T-23 | Unverifiable label; known finding keyed on it | printed `asserted`; key rejected | origin | B | 7 |
| T-24 | Corpus unit built with the PostgreSQL engine | fail | sidecar engine stamp | B | 5 |
| T-25 | The 12 empty probes / the 26 non-empty probes (planning set) | fail / proceed (a DuckDB parser error still proceeds) | emptiness | B | 1 |
| T-26 | Zero units; zero records; tool exit 2; exit 1 with no diagnostics; exit 0 with diagnostics; bad JSON; missing file result | fail | lint infra (PR #801 tests) | B | 1 |
| T-27 | Positive fixture missing a rule; negative fixture with a diagnostic | fail every run | self-test | B | 5 |
| T-28 | `TEXT[]` or `SERIAL` added to a real DDL generator | new finding with site and origin | lint, origin | B | 5, 7 |
| T-29 | +1 index in `mainColumnIndexDDL`; −1; new rule at a known site; same rule from another site; entry without `#N` or with a wildcard | fail | known findings | B | 1 |
| T-30 | New generator at an exercised site with invalid SQL (spike g2); the same at an `uncovered` site | `parse_error` fail; passes and is documented | lint, docs | B | 4 |
| T-31 | New untaken branch at an existing site (spike a31) | static gate passes; report shows per-site record counts and makes no branch-coverage claim | report | B | 3, 4 |
| T-32 | Each of PR #801's 64 generators | rendered-and-linked or executed-and-matched; ≥106 statements | migration | B | 3 |
| T-33 | init-db | executed BEGIN, 24 DDL, COMMIT; per-generator `verified-match`; #690 counts exact | migration | B | 3, 5, 7 |
| T-34 | Update/merge transaction on `aurora-dsql` | executed unit present; `updateMainRow` has evidence | migration | B (OD-5) | 3 |
| T-35 | `ci.yml` lint job, Makefile `all` and `build-all` | same `make lint-dsql`; `build-all` excluded | guards | B | 2 |
| T-36 | Captured argv | never `--fix` | lint infra | B | 6 |
| T-37 | Two runs | byte-identical sidecar and report | determinism | B | 1 |
| T-38 | Fixture attribution under `-race`, `-cover`, `-gcflags=all=-l` | identical | attribution | B | 7 |
| T-39 | Each PR #801 failure shape (R1–R12: lowercase, comment prefix, concatenation, fragments, dynamic table, CTE columns, `SELECT(1)`, SET, CREATE modifiers, DO/CALL/VALUES/EXPLAIN/GRANT, `%w`, `BEGIN;…`, nested comments, `\v`) as (a) a generator feeding a new caller of an existing wrapper and (b) a new direct call | all caught as new sites, whatever the text | discovery | B | 4 |
| T-40 | Docs and summary | the four dimensions are printed separately; the "not proof" wording is present; no SQL-text scanner code remains | docs, report | B | 3, 8 |

## 9. Readiness review

1. **Does the invariant match the mechanism?** Yes. G1 is about sites; variants are covered only through G2/G3 and are stated as such.
2. **Is "shipped" explicit?** Yes (§2.1), including mains and build tags.
3. **Can an unknown sink bypass discovery?** Only an uncatalogued API, reflection or cgo. T-06 bounds the first; the rest are documented non-guarantees.
4. **Can a classification transfer silently?** No: there are no ordinals, a duplicate key fails, and regeneration never assigns a class.
5. **Can a variant at an existing site be missed?** Yes. ADR §6 and T-30/T-31 state it.
6. **Is the initial manifest self-validating?** No: the #805 audit is source-level, and evidence never sets a class.
7. **Can rendered SQL fake execution?** No (T-22).
8. **Can an asserted origin pass as verified?** No (T-23).
9. **Can a comment-only statement count?** No (T-25).
10. **Can a known diagnostic multiply?** No (T-29).
11. **Is existing coverage kept?** Yes (T-32, §5).
12. **Is the design understandable without the 12 rounds?** The ADR is meant to be.
