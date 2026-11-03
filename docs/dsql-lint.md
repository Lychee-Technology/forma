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
3. Runs `go test -run '^TestDSQLCorpus'` in each package that has a
   rendered generator, with `FORMA_DSQL_CORPUS_DIR` set. Each test writes its
   statements to `.sql` units, with an `-- origin: file.go:Symbol` comment
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
- an origin comment with no executable SQL after it (only comments, blank
  lines or semicolons), or SQL before a unit's first origin comment. The
  statement count comes from the SQL, not from the labels, and every
  exported statement must hold exactly one statement
- a rendered generator that is missing from the corpus
- a corpus statement whose generator is not classified `rendered`
- a new finding
- a known finding that no longer occurs

Use `-keep-corpus DIR` (`go run ./cmd/dsqllint -keep-corpus /tmp/corpus`) to
keep the rendered units for inspection.

## What is examined

`inventory.txt` lists every site in the code and every checked-in `.sql`
file. A site is a symbol (function, method, package-level const, var or type)
that does any of these:

- holds SQL text. That is a string literal, or a constant expression folded
  from literals and the package's string constants, that starts a SQL
  statement in any letter case, after any leading SQL comments.
- assembles SQL text from fragments. Its string literals and the package's
  string constants, read in source order, start a SQL statement however they
  are put together (`strings.Join`, `fmt.Sprintf` arguments, a
  `strings.Builder`, `+`). A dynamic piece between them, such as a variable
  table name in `strings.Join([]string{"UPDATE ", table, " SET a = $1"}, "")`,
  is read both as a name and as absent. This finds the generator where it
  builds, even when it hands its SQL to an executor the inventory already
  classifies.
- calls a database method: `Exec`, `Query`, `QueryRow`, `Begin`, `BeginTx`,
  `SendBatch`, `CopyFrom`, `Prepare`, `Queue` and their `Context` forms.
  This catches SQL assembled from fragments that no pattern recognises,
  where it is sent.
- names pgx or `database/sql` transaction options. From these, pgx writes a
  `BEGIN ... ISOLATION LEVEL` statement the code never spells.

Each entry has one of these classes and a note:

| Class | Meaning |
| --- | --- |
| `rendered` | A `TestDSQLCorpus*` test renders it and the gate lints it |
| `uncovered` | Can reach DSQL but is not linted. The note starts with its gap group, `[name]`, and says why |
| `passthrough` | Sends, forwards or declares SQL that another entry builds: the executor of a builder, a wrapper, an interface. The note names the builder |
| `postgres-only` | Runs only under the PostgreSQL engine (advisory locks, `READ COMMITTED` begins). Must stay out of the corpus |
| `duckdb` | Sent to DuckDB, never to PostgreSQL or DSQL |
| `not-dsql` | Never sent to a DSQL database: e2e harnesses, the local sample importer, text that only looks like SQL, a method that only shares a database method's name |

The scan has no type information, so a method that only shares a database
method's name is a site too. Classify it `not-dsql` and say so in the note.

The gate prints the counts and every uncovered entry, grouped by gap, on each
run. At the time of writing it renders 64 generators (106 statements in 16
units):

- init-db's whole table transaction, exactly as `ensureTables` sends it, with
  `BEGIN`/`COMMIT`, and its schema registration
- the declared-index catalog reads and build
- whole write transactions of the DSQL engine, `BEGIN` to `COMMIT`, through
  the repository's entry points: a create, a delete, a two-row batch delete
  and a requeue
- the entity_main insert and update builders for a live and a deleted record
  setting a column of every typed family, and an update's scoped EAV replace
- the unique-key reservation, owner reads, undo and release
- the DSQL row-guard upsert and recheck probes, the precondition probe and
  the transaction clock
- the CDC schema lease, the chunked flush mark, the flush and init batch
  reads, and both export snapshots (delta and base): the `BEGIN ISOLATION
  LEVEL REPEATABLE READ READ ONLY`, the stage reads and the `COMMIT`
- the federated dirty-set reads
- validate-schema-consistency's width census reads

The `BEGIN` and `COMMIT` of a transaction are what pgx writes for the
transaction's options, not a copy. `corpus.WireConn` is a real pgx
connection to an in-process stand-in for a PostgreSQL server that records
each statement it receives. The statements of the body come from a pgxmock or
sqlmock connection built with `corpus.Recorder`, which records into the same
list in the order they are sent.

### Gaps

Each uncovered entry belongs to a gap: a `gap <name> <tracking> <note>` line of
`inventory.txt` that declares the group and what tracks closing it. The
tracking is an issue (`#691`) or `dsql-lint-coverage-follow-up`, the DSQL lint
coverage follow-up issue, which lists each such gap by name with its entries.
The gate refuses an uncovered entry without a declared gap, and a gap with no
entry left. The gaps are:

| Gap | Tracking | What is not linted |
| --- | --- | --- |
| `tool-statements` | follow-up | Statements that the `cmd/tools` subcommands, the schema ledger, manifest reconcile and the unique-key build send inline |
| `startup-reads` | follow-up | Catalog and registry reads at startup, and the health probe |
| `query-dsl` | follow-up | Read-path SQL assembled per query from the condition DSL and schema projections: the OLTP query template, the single-row read, the claim candidate read and the PostgreSQL predicate fragments. The row-guard probes that are rendered use a stand-in for the precondition fragment |
| `parser-limits` | follow-up | SQL the pinned linter cannot parse: `declaredindex.DropSQL` (`DROP INDEX CONCURRENTLY` is a `parse_error` in 0.2.22), and `scripts/validate_schema_consistency.sql` (psql `\if`/`\set` meta-commands) |
| `duckdb-postgres-scanner` | #691 | SQL that DuckDB's postgres extension writes for ATTACH-based reads. Forma never holds that SQL |

When you render an uncovered entry, reclassify it `rendered`. Remove its gap
once the gap has no entries, and update the tracking issue.

### Limits of the coverage

- **The scan is a heuristic without types.** It finds SQL text, database
  method calls and transaction options by name. Code that reaches the
  database only through a method with another name, such as an interface
  method declared elsewhere, is found at the implementation that calls the
  driver, which is itself an entry. That entry is usually `passthrough`, so
  its callers are not listed one by one. Review still matters.
- **Rendering uses representative inputs.** Table names are the defaults,
  generators run with one or two sample rows, and there is one sample declared
  index and one sample schema projection. A precondition fragment that stands
  in for the condition DSL's output is marked as such in the test. Runtime
  branches that a sample does not take are not linted.
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

**An unclassified site.** New code holds SQL, calls a database method or sets
transaction options. Add an inventory entry. If the SQL can reach DSQL, render
it:

1. Add it to one of the package's `TestDSQLCorpus*` tests. Call the
   production generator, or better the entry point that sends it. If the
   code builds and sends the SQL inline, record it from a pgxmock or sqlmock
   connection built with `corpus.Recorder`. For SQL that pgx writes itself,
   such as a transaction's `BEGIN`, use `corpus.WireConn`. Never copy the SQL
   into the test.
2. Classify it `rendered`.

If the symbol only sends or forwards SQL that another entry builds, classify
it `passthrough` and name that entry. If you cannot render it, classify it
`uncovered` under a gap, with the reason.

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
