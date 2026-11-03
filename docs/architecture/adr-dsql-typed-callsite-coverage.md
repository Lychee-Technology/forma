# ADR: Typed SQL call-site coverage for the Aurora DSQL lint gate

- **Status:** Proposed. Approved in principle (CONDITIONAL GO); the open owner decisions are listed in the implementation plan.
- **Issue:** #740
- **Supersedes:** the inventory mechanism of PR #801
- **Evidence:**
  - recovery review: https://github.com/Lychee-Technology/forma/pull/801#issuecomment-6595036818
  - spike: https://github.com/Lychee-Technology/forma/pull/801#issuecomment-6596557050
  - spike branch: `spike/dsql-typed-callsite` at `ab3f1ea`
- **Plan:** `docs/plans/dsql-lint-typed-callsite-replacement.md`

## 1. Problem

Forma's `aurora-dsql` engine sends SQL that is mostly built in Go. #740 asks for a build-time gate. It lints that SQL with AWS's pinned dsql-lint and keeps an inventory, so that "newly introduced DSQL-reachable SQL cannot quietly evade the inventory" (criterion 4). The gate must name the generating source of each finding (criterion 7) and fail closed on extraction errors, zero files and zero statements.

## 2. Why PR #801 did not converge

PR #801's linting, tool pinning and corpus rendering are sound, and the reviews accepted them repeatedly. The inventory is where it failed.

- **What it tracks.** The inventory tracks *symbols whose text looks like SQL*, found by roughly 60 regular expressions over Go string literals and fragments. Executors are classified `passthrough`, so a new generator feeding one is found only if its text is recognised.
- **Why the reviews never ended.** Recognising SQL built by arbitrary Go code has no fixed point. Rounds R2–R12 each found another text shape, and each fix added patterns.
- **What it can't see by construction.** New SQL inside an already-classified symbol is invisible.
- **Statement counting.** The corpus is serialised to text and lexed again by three hand-written lexers that disagree about SQL whitespace. That disagreement is R1-F3, R11-F1 and R12-F1.
- **Requirement bound.** Criterion 4 was never given a bound, so every new shape counted as a blocking violation.

## 3. Spike evidence

All figures were measured at `9776c2b` + spike, on go1.26.0, linux/amd64, and reproduced from the pushed data. "Demonstrated" means a test exercised the behaviour. It does not mean the behaviour holds universally.

| Claim | Evidence | Status |
|---|---|---|
| Typed discovery finds the shipped execution sites | 126 sites (38 packages); 0 unresolved patterns in verbatim mode; 16 verbatim wrappers; 18 project interfaces resolved via `types.Implements` | demonstrated on current code |
| New execution sites cannot slip through | 38 synthetic mutations: 26 detected, 4 rejected as unsupported, 2 correctly ignored, 4 documented misses, 2 runtime-only, 0 silent | demonstrated for these 38 shapes only; not a proof that false negatives are absent |
| Stack attribution maps statements to sites | 61/61 real and 20/20 synthetic records, 0 ambiguous; unchanged under `-gcflags=all=-l`, `-race` and `-cover` (the last re-checked during planning) | demonstrated |
| Generator origin is statically linkable | 57/61 recorded labels; 60/64 PR #801 generators, and 63/64 with typed struct-field linking (a planning experiment, not committed) | demonstrated; the remaining one is pgx-written BEGIN options |
| Rendered SQL is not execution | 45/106 corpus statements come from direct generator calls; 11 generators link to sites that have no execution evidence | measured |
| An empty statement can be detected without a new grammar | DuckDB `PrepareContext` returned the empty-query error for all 12 empty probes and for none of the 26 non-empty probes (planning re-check: 38/38). It raised parser errors on valid DSQL (`ASYNC`, `IDENTITY (CACHE 1)`, `FOR UPDATE`, BEGIN options) | usable for emptiness only |
| Cost | Static pass 0.59–0.61 s warm, 17.9 s with an empty build cache; about 90 MB RSS; capture overhead within noise | measured locally; CI not measured |

Assumed rather than proven:
- the driver catalogue lists every API Forma uses;
- the generated sample manifest's classes are correct. Its 126/126 classification is circular, because "has evidence" defined `dsql`.

The following were recommended by the recovery review but not proven by the spike:
- Forma-specific lock rules;
- exact-count known findings (designed, not built);
- manifest churn over real history.

## 4. Decision

Replace text recognition with a **typed inventory of SQL execution sites**, joined with **runtime and rendered corpus evidence**, and keep PR #801's lint infrastructure.

1. **Discovery.** `go/packages` + `go/types`. A site is a call whose resolved `*types.Func` is in a finite driver catalogue, keyed by the package that declares the method. The catalogue covers pgx, pgxpool, pgconn and `database/sql`, including Begin, BeginTx, BeginFunc, Commit and Rollback, with the SQL parameter at a fixed index of string type. A call is also a site when it resolves to:
   - a project interface method with a catalogued implementer;
   - a project function that passes its own string parameter **verbatim** to a sink (followed to a fixpoint, with no general data flow).
2. **Unsupported forms fail closed.** These are refused, never ignored:
   - a sink used as a value (method value, method expression, function value);
   - a call through a function value that returns a driver result;
   - a project interface method with a driver method's name and no implementer;
   - `Batch.Queue` and `SendBatch`;
   - an import of a database driver package outside the catalogue.
3. **Identity.** `pkg.Func:callee:h8`, where `h8` hashes the SQL-argument expression (and the options expression for a begin). Two sites with the same key in one function fail the gate until one carries an explicit `//dsqllint:site <name>` directive. There is no ordinal fallback: the spike showed ordinals silently move a classification to a different call.
4. **Manifest.** A reviewed file assigns each site exactly one class: `dsql`, `postgres-only`, `duckdb`, `uncovered`, or `boundary`. Main packages and checked-in `.sql` files are listed as well. Regeneration never assigns a class.
5. **Evidence.** Each statement the corpus produces is a typed record (kind, SQL, engine, mechanism, stack, optional label) written to a structured sidecar. No metadata is reconstructed from SQL comments.
   - *Executed* records come from a recording driver: the pgxmock/sqlmock matcher, or a client-side capture on `WireConn`.
   - *Rendered* records come from calling a production generator directly.
6. **Attribution.** An executed record maps to the innermost module frame inside a site's call span. A rendered record maps through its generator to the site whose builder closure contains it. Origin confidence is reported, never upgraded: exec-site, verified-builder, verified-match, verified-closure(depth), or asserted.
7. **Lint.** Every record must be non-empty under the DuckDB emptiness check, and is linted by pinned dsql-lint 0.2.22. Known findings match on site, rule and **exact count**.

## 5. Guarantees

### Original criterion 4

> Newly introduced DSQL-reachable SQL cannot quietly evade the inventory.

### Operational interpretation (requires owner approval, plan OD-1)

**Scope S** is the non-test files of the module packages in the transitive import closure of the entry points `.`, `./factory`, `./cdc`, `./cmd/server`, `./cmd/tools` and `./cmd/lambda`. They are loaded in the default build context (CI's linux/amd64, `CGO_ENABLED=1`, no tags). Every `main` package in the module is an entry point or is listed `unshipped-main`. A file in S whose build constraint excludes it from that context fails the gate; there are none today.

After a green `make lint-dsql`, the following hold.

- **G1 (sites).** Every call in S to a catalogued driver entry point, whether direct, through a resolved project interface or through a verbatim-forwarding function, is in the inventory. Each site has exactly one reviewed class. The inventory and the manifest agree exactly, with no missing and no stale entries, and S contains none of the unsupported forms in §4.2. *A new execution site, including one inside an already-classified function, therefore fails until it is reviewed.*
- **G2 (evidence).** Every `dsql` site has corpus evidence of its declared kind, produced with the repository configured for `aurora-dsql`.
  - `executed` is the default. It is mandatory for transaction statements, and for sites whose SQL is not statically linked to a pure generator.
  - `rendered` is allowed only for statements of a generator that is statically linked to that site.
  - Every executed record maps to exactly one site, and that site is `dsql` or `boundary`. A record from a `postgres-only`, `duckdb` or `uncovered` site fails.
  - A rendered record never satisfies an executed requirement.
- **G3 (lint).** Every corpus record, executed or rendered, is non-empty. Each is linted lint-only by dsql-lint 0.2.22, whose positive and negative controls must behave in every run. The diagnostics equal the known findings exactly in site, rule and count, and each known finding names an issue.

**How this enforces criterion 4.** "DSQL-reachable SQL" is enforced through the place where SQL reaches the driver:
- a new place is caught by G1;
- new SQL at a `dsql` place is linted by G3 when the corpus represents it, which G2 requires for at least one form.

This is narrower than a literal reading of criterion 4, which no build-time mechanism can establish. It is stated here rather than implied.

## 6. Non-guarantees

- A **new SQL variant at an existing site** is checked only if the corpus executes or renders it. Examples: a new branch, SQL carried in a struct field or parameter, or a new caller of a helper that *modifies* the SQL it is given (verbatim following does not see this). The spike's a13b, a30, a31 and g2 cases show it. At an exercised `dsql` site, g2 was still caught by lint at run time.
- **Reflection, cgo, `unsafe`**, and driver APIs absent from the catalogue are outside the guarantee. A completeness test (plan T-06) keeps the catalogue in step with each pinned driver version.
- **`postgres-only` and `duckdb` labels** are refuted only when the DSQL corpus reaches the site.
- **SQL that DuckDB's postgres extension writes** (#691) is outside the guarantee.
- **SQL in packages outside S** is outside the guarantee: tests, harnesses, unshipped mains.
- **Live DSQL compatibility.** dsql-lint is an early-release static checker. A green gate does not prove that a cluster accepts the SQL or behaves like PostgreSQL. That is the job of #739 (MiniStack) and real-cluster runs (#633).

## 7. Rejected alternatives

- **SQL-text scanner (PR #801).** Open-ended, and blind to new SQL inside a classified symbol.
- **Full taint analysis.** It closes a13b, but it promoted 54 functions instead of 16 and added 69 sites, mostly over table-name parameters. It also cannot tell an identifier parameter from a SQL-fragment parameter, and it raised a real fail-closed case (`markFlushedFor` returns a sink as a value).
- **SSA or call-graph analysis.** Not needed: everything above works on AST, types and a local fixpoint.
- **A single blessed executor, enforced by lint.** Strongest, but it means refactoring about 100 call sites to suit a tool.
- **Runtime recording only.** It cannot see code that no test runs.
- **Ordinal or position IDs.** Position IDs churn on every edit, and ordinal IDs transfer classifications silently.
- **A custom statement lexer.** It reproduces PR #801's bug class.
- **A blanket `FOR UPDATE` rule.** AWS documents `SELECT … FOR UPDATE` and `FOR KEY SHARE` as supported, with optimistic adjudication at commit, and `FOR SHARE` and `NO KEY UPDATE` as unsupported. See [Concurrency control in Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html).

## 8. Compatibility with #740

| #740 criterion | How it is met |
|---|---|
| 1. Deterministic, pinned, no AWS or Docker, fails on diagnostics or extraction errors | PR #801's tool pin, plus G3 and exact counts |
| 2. CI runs the same entry point | `make lint-dsql` in the lint job and in `make all` (PR #801's wiring and guard tests) |
| 3. Generators linted; inventory says what is examined | G2 and G3; four coverage dimensions reported separately (§9) |
| 4. New SQL cannot quietly evade | G1 for sites; non-guarantees stated; owner approval OD-1 |
| 5. Invalid DSQL detected; PostgreSQL-only SQL kept out | regression fixtures; the `postgres-only` class fails when reached |
| 6. No rewriting | `--fix` is never passed; tested |
| 7. Diagnostics name SQL and generating source; gaps tracked | site plus origin confidence; `uncovered` requires an issue |
| 8. Not proof of compatibility | stated in the docs and in every gate summary |

## 9. Operational and maintenance costs

- **Reporting.** Four separate numbers are reported, never merged into one percentage:
  - static site classification (classified/discovered);
  - rendered coverage (generators rendered/linked);
  - execution evidence (`dsql` sites with executed evidence/requiring it);
  - lint coverage (records linted/records).
- **Runtime.** About 0.6 s warm for discovery. The cold cost in CI must be measured; the lint job's earlier `make lint` shares `.gocache`.
- **Churn.** Renaming a function or the SQL variable re-keys a site. The regenerate command lists stale/new pairs as *possible renames* and does not carry the class across.
- **Catalogue.** New driver APIs fail the completeness test until they are classified.
- **Dependency.** `golang.org/x/tools` moves from indirect to direct. It is used only by `internal/dsqllint`, and no shipped package imports it (guarded).

## 10. Acceptance criteria

Plan §8 (tests T-01 to T-40) is the acceptance suite. It covers every PR #801 failure class and every mutation shape from the spike.

## 11. Migration

One replacement PR from `main`, with milestone commits (plan §7). It recovers PR #801's infrastructure selectively and does not merge the spike. Of the 106 corpus statements, 61 stay executed, 29 move to execution (init-db tables, declared indexes), and 16 stay rendered and statically linked. A new executed update/merge transaction unit is added (plan §5).

## 12. Residual risks

- Corpus representativeness bounds the variant guarantee.
- Mock fidelity: zero-value rows, and pgxmock's double match on batches. Batches are refused.
- DuckDB and PostgreSQL may lex differently outside the 38 probes.
- The manifest audit relies on reviewers; it is not machine-checked beyond the evidence join.

## 13. Extension points

- Batch support: attribute `Queue` sites through the batch value's data flow.
- Taint following, opt-in per helper with a `//dsqllint:forwards-sql` directive.
- Field-sensitive builder linking (validated in planning; included in the plan).
- Forma rules backed by AWS documentation.
- Runtime confirmation of `postgres-only` labels from the dsql-emulated coordination suite.
- Running the same records against MiniStack (#739).
