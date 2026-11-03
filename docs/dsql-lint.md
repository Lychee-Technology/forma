# DSQL lint gate

`make lint-dsql` is a static check that the SQL Forma sends can run on Amazon
Aurora DSQL (#740). It renders SQL from Forma's own Go generators, lints it
with the pinned release of AWS's
[`dsql-lint`](https://github.com/awslabs/aurora-dsql-tools/tree/main/dsql-lint),
and keeps an inventory of every place in the code that holds SQL. It needs no
database, Docker or AWS credentials. On first use it downloads the linter.

**`dsql-lint` is an early-release static checker. A clean run does not prove
that DSQL accepts the SQL or behaves like PostgreSQL.** For example, version
0.2.22 says nothing about advisory locks, `FOR UPDATE`, or a statement it
misparses as valid SQL. The gate is one layer of three:

| Layer | What it shows |
| --- | --- |
| `make lint-dsql` (this page) | The SQL Forma renders passes DSQL's documented static restrictions: one DDL per transaction, `CREATE INDEX ASYNC`, no `SERIAL`, arrays, `TRUNCATE`, `SET TRANSACTION` and so on |
| MiniStack strict-mode lane (#739) | The SQL runs against a DSQL emulator |
| A real cluster ([Aurora DSQL](aurora-dsql.md#verification)) | It runs on DSQL |

## Where it runs

CI's lint job runs `make lint-dsql`, and so does `make all`, so both use the
same entry point. `make build-all` does not run it. That target only compiles
and keeps working offline once modules are cached. The guards
`TestCILintJobRunsLintDSQL` and `TestMakeAllRunsLintDSQL` pin this wiring.

`make lint-dsql` runs `go run ./cmd/dsqllint`, which does this in order:

1. Checks the inventory (`internal/dsqllint/inventory.txt`) against the code.
   This step is also `TestInventoryCoversRepository`, so `make test` enforces
   it on its own.
2. Lints the regression fixtures in `internal/dsqllint/testdata/regression/`.
   `invalid.sql` must trigger exactly the rules its `-- expect:` comments name
   (including the multi-DDL-transaction and `CREATE INDEX ASYNC` DDL
   restrictions), and `valid.sql` must trigger none. If a linter upgrade stops
   catching something, this step fails.
3. Runs `go test -run '^TestDSQLCorpus$'` in each package that has a
   rendered generator, with `FORMA_DSQL_CORPUS_DIR` set. Each test writes its
   statements to one `.sql` unit, with an `-- origin: file.go:Symbol` comment
   before every statement.
4. Lints every unit in one `dsql-lint --format json` run. The build never
   passes `--fix`, so the gate never rewrites SQL.
5. Traces each diagnostic to its generator through the origin comments and
   compares the result with `internal/dsqllint/known_findings.txt`.

The gate fails closed. Any of these fails it:

- a nonzero `dsql-lint` exit that the JSON output does not explain
- a file the linter could not read
- a render or export failure
- zero units or zero statements
- a rendered generator that is missing from the corpus
- a corpus statement whose generator is not classified `rendered`
- a new finding
- a known finding that no longer occurs

Use `-keep-corpus DIR` (`go run ./cmd/dsqllint -keep-corpus /tmp/corpus`) to
keep the rendered units for inspection.

## What is examined

`inventory.txt` lists every symbol whose source holds a string literal that
starts a SQL statement, and every checked-in `.sql` file. Each entry has one of
these classes and a note:

| Class | Meaning |
| --- | --- |
| `rendered` | A `TestDSQLCorpus` test renders it and the gate lints it |
| `uncovered` | Can reach DSQL but is not linted. The note says why |
| `postgres-only` | Runs only under the PostgreSQL engine (advisory locks). Must stay out of the corpus |
| `duckdb` | Sent to DuckDB, never to PostgreSQL or DSQL |
| `not-dsql` | Never sent to a DSQL database: e2e harnesses, the local sample importer, text that only looks like SQL |

The gate prints the counts and every uncovered entry on each run. At the time
of writing, 28 generators are rendered (40 statements):

- init-db's whole table transaction, exactly as `ensureTables` sends it, with
  `BEGIN`/`COMMIT`
- init-db's schema registration
- the declared-index catalog reads and build
- the DSQL row-guard upsert and recheck probes
- the precondition probe and transaction clock
- the CDC schema lease
- the chunked flush mark

62 DSQL-reachable locations are uncovered. Most are write-path, read-path,
CDC export and tool queries that a method builds inline per request from
schema metadata or the condition DSL. Rendering those needs a recording
harness per method, like the ones in `internal/dsql_corpus_test.go` and
`internal/cdc/dsql_corpus_test.go`, and is follow-up work.

Two uncovered items are linter limitations:

- `declaredindex.DropSQL`: `dsql-lint` 0.2.22 reports `DROP INDEX CONCURRENTLY`
  as a `parse_error`.
- `scripts/validate_schema_consistency.sql`: a psql script whose `\if`/`\set`
  meta-commands the linter cannot parse.

### Limits of the coverage

- **The scan is a keyword heuristic.** It finds upper-case literals that start
  a statement (`SELECT `, `INSERT INTO`, `CREATE TABLE`, and so on). SQL built
  only from fragments that do not start that way evades it. So does a
  `BEGIN ISOLATION LEVEL ...` that pgx sends from `pgx.TxOptions`, such as the
  CDC export's `REPEATABLE READ READ ONLY` read. Review still matters.
- **Rendering uses representative inputs.** Table names are the defaults,
  generators run with one or two sample rows, and there is one sample declared
  index. A precondition fragment that stands in for the condition DSL's output
  is marked as such in the test. Runtime branches that a sample does not take
  are not linted.
- **Placeholders stay as `$n`.** That is what Forma sends, and `dsql-lint`
  parses them.

## When the gate fails

**A new finding.** The report names the corpus unit and line, the rule, the
generator (`generated by:`), the linter's suggestion, and the statement. Fix
the generator. If the SQL is never sent to DSQL, reclassify its inventory
entry instead: `postgres-only` for SQL that runs only under the PostgreSQL
engine, with the reason in the note. If the finding is real but its fix
belongs to another issue, add a line to `known_findings.txt` naming the
generator, the rule and the tracking issue. An entry accepts only that rule
on that generator. No entry can disable a rule globally.

**A stale known finding.** The generator no longer produces it. Remove the
line, and update the issue it names.

**An unclassified SQL site.** New code holds SQL. Add an inventory entry. If
the SQL can reach DSQL, render it:

1. Add it to the package's `TestDSQLCorpus`. Call the production generator.
   If the code builds and sends the SQL inline, record it from a pgxmock or
   sqlmock connection built with `corpus.Recorder`. Never copy the SQL into
   the test.
2. Classify it `rendered`.

If you cannot render it, classify it `uncovered` with the reason.

**A corpus coverage problem.** A rendered generator is missing from the
corpus because its export was deleted, skipped or mislabelled. Or a corpus
origin is not a `rendered` entry.

## Investigating a false positive

1. Rerun with `-keep-corpus DIR` and lint the unit directly:
   `build/tools/dsql-lint-0.2.22/<os>-<arch>/dsql-lint DIR/<unit>.sql`.
2. If the statement is valid DSQL, check the
   [dsql-lint issues](https://github.com/awslabs/aurora-dsql-tools/issues) and
   report it there.
3. Meanwhile, add a `known_findings.txt` line for that generator and rule. The
   note says it is a linter false positive, and the issue field names the
   Forma issue that tracks it. Do not reclassify DSQL-reachable SQL to hide
   a finding.

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
3. Run `make lint-dsql`. New rules surface as new findings. A rule that
   changed behaviour shows up in the regression fixtures or as stale known
   findings. Decide each change on its merits: do not edit a fixture's
   expectations just to make an upgrade pass.
