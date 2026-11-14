# DSQL lint gate

`make lint-dsql` is a static check of SQL Forma produces against Amazon Aurora
DSQL's documented restrictions (#740). It collects a corpus of statements from
Forma's own Go code and lints them with the pinned release of AWS's
[`dsql-lint`](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint).
It needs no database, Docker or AWS credentials. On first use it downloads the
linter.

**`dsql-lint` is an early-release static checker. A clean result does not prove
that DSQL accepts the SQL or behaves like PostgreSQL.** For example, version
0.2.22 says nothing about advisory locks, `FOR UPDATE` or `NOWAIT`. The gate is
one layer of three:

| Layer | What it shows |
| --- | --- |
| `make lint-dsql` (this page) | The corpus passes DSQL's documented static restrictions: one DDL per transaction, `CREATE INDEX ASYNC`, no `SERIAL`, arrays, `TRUNCATE`, `SET TRANSACTION` and so on |
| [MiniStack strict-mode lane](dsql-ministack.md) (#739) | The SQL runs against a DSQL emulator |
| A real cluster ([Aurora DSQL](aurora-dsql.md#verification)) | It runs on DSQL |

**The gate has no inventory yet.** It lints the statements in the corpus and
knows nothing about any other SQL. New code that sends a statement no
`TestDSQLCorpus` test exports passes without being linted. Typed discovery of
SQL call sites and an audited manifest arrive with #804 and #805, and until
#806 a statement's origin label is what the corpus test asserts, not something
the gate has verified.

## Where it runs

CI's lint job runs `make lint-dsql`, and so does `make all`, so both use the
same entry point. `make build-all` does not run it. That target only compiles
and keeps working offline once modules are cached. The guards
`TestCILintJobRunsLintDSQL`, `TestMakeAllRunsLintDSQL` and
`TestBuildAllDoesNotRunLintDSQL` pin this wiring.

`make lint-dsql` runs `go run ./cmd/dsqllint`, which does this in order:

1. Lints the regression fixtures in `internal/dsqllint/testdata/regression/`.
   `invalid.sql` must trigger exactly the rules its `-- expect:` comments name
   (including the multi-DDL-transaction and `CREATE INDEX ASYNC` DDL
   restrictions), and `valid.sql` must trigger none. If a linter upgrade stops
   catching something, this step fails.
2. Runs `go test -run '^TestDSQLCorpus'` in every package with such a test,
   with `FORMA_DSQL_CORPUS_DIR` set, twice. Each test writes a JSON Lines file
   of typed records per unit. The two exports must match byte for byte.
3. Checks each record: its shape, that it was built for `aurora-dsql`, and that
   its SQL holds a statement.
4. Checks the corpus floor (`internal/dsqllint/corpus_floor.txt`).
5. Writes each unit's statements to a `.sql` file and lints them all in one
   `dsql-lint --format json` run. The build never passes `--fix`, so the gate
   never rewrites SQL.
6. Traces each diagnostic to its record and compares the result with
   `internal/dsqllint/known_findings.txt`, by exact count.

The gate fails closed. Any of these fails it:

- a `dsql-lint` exit other than 0 or 1, exit 1 with no diagnostics, or exit 0
  with some
- output that is not schema 1 JSON, a file the linter could not read, or a
  result missing for a file it was asked about
- a file result without its `file`, `error` or `diagnostics` array (a null
  `diagnostics` counts as none: only `[]` says a file is clean), or a diagnostic
  without its `rule`, `line` or `message`
- a diagnostic on a line no record holds
- a failing or missing corpus test, or two exports that differ
- zero units, or a unit with zero records
- a record that is malformed, built for another engine, or holds no statement
- a regression fixture that does not behave as it says
- a label below its corpus floor
- a known finding whose count is not exact, or a finding no entry lists

Use `-keep-corpus DIR` (`go run ./cmd/dsqllint -keep-corpus /tmp/corpus`) to
keep `records/` and the `sql/` linted from them for inspection.

## The corpus

A record is one statement with what is known about it:

```json
{"kind":"executed","sql":"SELECT ...","engine":"aurora-dsql","mechanism":"pgxmock","unit":"internal-federated-change-log-snapshot-hot-excluded","seq":2,"label":"internal/federated/change_log_snapshot.go:dirtySetQuery"}
```

| Field | Meaning |
| --- | --- |
| `kind` | `executed`: production code sent it to a connection. `rendered`: a test obtained it from a generator without sending it, so it says what the generator produces, not that anything sends it |
| `mechanism` | How it was captured. Executed: `pgxmock`, `sqlmock`, or `wire-client` for SQL real pgx writes. Rendered: `generator`, or `test-boundary` for a `BEGIN`/`COMMIT` the test spells around rendered statements |
| `engine` | The engine the unit was built for. The gate rejects anything but `aurora-dsql` |
| `unit`, `seq` | The statements sent together, and this one's place among them. Order matters: one-DDL-per-transaction is a property of the sequence |
| `label` | The corpus origin, `path/file.go:Symbol`, asserted by the test |

The baseline is PR #801's corpus: 106 statements from 64 generators in 16 units,
61 executed and 45 rendered.

- init-db's table transaction with its `BEGIN`/`COMMIT` (rendered, in the order
  `ensureTables` sends it), and its schema registration (executed)
- the declared-index catalog reads and build (rendered)
- whole write transactions of the DSQL engine, `BEGIN` to `COMMIT`, through the
  repository's entry points: a create, a delete, a two-row batch delete and a
  requeue (executed)
- the entity_main insert and update builders for a live and a deleted record
  setting a column of every typed family (rendered), and an update's scoped EAV
  replace (executed)
- the unique-key reservation, owner reads, undo and release, and the DSQL
  row-guard upsert, recheck, precondition probe and transaction clock (executed)
- the CDC schema lease, chunked flush mark, flush and init batch reads and the
  export snapshot boundaries (executed), and the export stage reads (rendered)
- the federated dirty-set reads (executed), and validate-schema-consistency's
  width census reads (rendered)

The update/merge transaction has no executed unit yet (#803), and
`buildUpdateMainStatement` is rendered only.

### Limits

- **A corpus is a sample.** Table names are the defaults, generators run with
  one or two sample rows, and a precondition fragment that stands in for the
  condition DSL's output is marked as such in its test. Runtime branches a
  sample does not take are not linted.
- **The corpus floor only stops the corpus shrinking.** It lists labels the
  corpus already has. It is not an inventory, and it cannot notice SQL the corpus
  never held. A label needs no line to be linted.
- **Placeholders stay as `$n`.** That is what Forma sends, and `dsql-lint` parses
  them.
- **Emptiness is DuckDB's judgement.** `dsql-lint` gives no signal for empty,
  whitespace-only or comment-only input: all exit 0 with no diagnostics. The gate
  asks DuckDB, already a Forma dependency, whether the text holds a statement, and
  only its empty-query answer counts, because its parser refuses valid DSQL such as
  `CREATE INDEX ASYNC`. A lexical difference between DuckDB and PostgreSQL on an
  exotic comment or whitespace form could slip through.

## When the gate fails

**A new finding.** The report names the corpus unit and line, the rule, the
origin label (marked asserted), the record's kind and mechanism, the linter's
suggestion, and the statement. Fix the code that produces it. If the finding is
real but its fix belongs to another issue, add a `known_findings.txt` line naming
the label, the rule, the exact count and the tracking issue. No entry can disable
a rule globally.

**A known finding's count changed.** One more means a new instance: the entry does
not absorb it, so fix it, or raise the count and say why in the PR. One fewer means
something was fixed: lower the count, remove the entry at zero, and update the issue
it names. The key is the label and the rule, so the same rule from another label is
a new finding.

The list is meant to change. It started at 15 diagnostics, all tracked by #690, but
no test pins that total or that issue: the gate's exact comparison is what holds the
list to the corpus, and every entry must name its own issue.

**A corpus problem.** A record holds no statement, is built for another engine, or
a label fell below its floor. If a corpus test was deleted, skipped or relabelled,
restore it or move the floor line with it, and say in the PR where the statement
went. `TestCheckedInBaselines` also refuses a floor below the baseline's 106
statements from 64 labels. If production code really dropped a generator, lower
`baselineStatements` and `baselineLabels` in the same PR and say why.

**A nondeterministic corpus.** Something in the named unit depends on map order or
the clock. Fix the test or the generator: known findings and reviews depend on the
same code producing the same bytes.

## Adding SQL to the corpus

Add it to one of the package's `TestDSQLCorpus*` tests, or add such a test; the
gate finds every package that has one. Call the production generator, or better
the entry point that sends it. If the code builds and sends the SQL inline, record
it from a mock built with `corpus.Recorder` (`rec.MatchPgx`, `rec.MatchSQL`). For
SQL that pgx writes itself, such as a transaction's `BEGIN`, use `corpus.WireConn`.
Never copy the SQL into the test. Where the code selects behaviour by engine, reach
it through the engine switch with `forma.EngineAuroraDSQL`.

## Investigating a false positive

1. Rerun with `-keep-corpus DIR` and lint the unit directly:
   `build/tools/dsql-lint-0.2.22/<os>-<arch>/dsql-lint DIR/sql/<unit>.sql`.
2. If the statement is valid DSQL, check the
   [dsql-lint issues](https://github.com/awslabs/aurora-dsql-tools/issues) and
   report it there.
3. Meanwhile, add a `known_findings.txt` line for that label and rule. The reason
   says it is a linter false positive, and the issue field names the Forma issue
   that tracks it.

## Upgrading dsql-lint

The pin is `ToolVersion` plus the `wheels` table in
`internal/dsqllint/tool.go`. The gate downloads the platform's PyPI wheel,
verifies its sha256, and extracts the prebuilt binary. It never runs pip and
never compiles Rust. `DSQL_LINT_BIN` points the gate at a preinstalled binary
instead, which must still report the pinned version. To upgrade:

1. Read the new release's changelog for rule and JSON-schema changes. The gate
   reads `schema_version` 1.
2. Take each wheel's URL and sha256 from
   `https://pypi.org/pypi/dsql-lint/<version>/json` (the `urls` list). Update
   `ToolVersion`, all four `wheels` entries, and the version in this page's
   commands together.
3. Run `make lint-dsql`. New rules surface as new findings. A rule that changed
   behaviour shows up in the regression fixtures or as a changed known-finding
   count. Decide each change on its merits: do not edit a fixture's expectations
   just to make an upgrade pass.

Version 0.2.22's limits that affect this gate: it does not flag advisory locks,
`FOR UPDATE` or `NOWAIT`, it reports `DROP INDEX CONCURRENTLY` as a `parse_error`,
and it gives no emptiness signal.
