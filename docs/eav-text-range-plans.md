# Text ranges on mixed EAV attributes: PostgreSQL plans (#815)

An assessment, written after the benchmark ran. It answers #815's remaining
question: with text and uuid attributes stored together in
`eav_data.value_text`, do selective ranges on PostgreSQL still plan well, and if
not, is the collation contract of #816 the cause?

## Background

#728 renders text ranges on the PostgreSQL route as
`<col> COLLATE "C" <op> $n`. #816 / PR #860 declares every Forma-owned text column
`COLLATE "C"`, so one ordinary index, `init-db`'s
`eav_data_text_idx (schema_id, attr_id, value_text, row_id)` included, can seek
those ranges ([Declared Indexes](schema-indexes.md#text-ranges),
[Text collation](text-collation.md)). PR #860 proved that by loading one
attribute at a time. With uuid `token`s and `t…` `note`s loaded together, a
`token gt` and a `note lt` were planned differently, and it was not known
whether the index, the statistics or the query shape was responsible.

## Conclusion

- **No collation regression.** In every load and statistics state the
  benchmark ran, the selective ranges are eligible to seek
  `eav_data_text_idx` with `value_text` in the Index Cond, and in practice they
  do. Moving `value_text` back to the database default and running the
  pre-#728 SQL, without `COLLATE "C"`, gives the same estimates and the same
  plans at the same cost.
- **There is a material planner cost, and it predates #728.** PostgreSQL
  estimates `attr_id = $a AND value_text > $b` as the product of two
  independent single-column selectivities. `value_text` has one histogram for
  every attribute, so a bound beyond another attribute's values looks
  unselective. Every uuid sorts below every note, so `token gt <last uuids>`
  is estimated at a quarter of `eav_data` (10,173 rows at 20,000 `e2e_wide`
  rows, 50,878 at 100,000) when it selects 5. In-range bounds (`token lt`,
  `note gt`) are estimated well.
- **The overestimate becomes cost in the query around the predicate, not in
  the predicate.** The predicate's own index-only scan reads 4 buffers either
  way. With the overestimate, the anchor's outer `eav_data t` (the extra scan
  #881 owns) is read whole: a sequential scan of every `eav_data` row of the
  schema, hash-joined to the predicate's 5 rows. While the tables are small,
  the planner also expects a full 1,000-row page, so it merge-joins the page
  against `entity_main`, walking its primary key, and hash-joins it against a
  second sequential scan of `eav_data`, for the page's attributes. At 100,000
  rows a 5-row `token gt` reads 1,923 buffers in about 26 ms, 1,872 of them in
  the outer scan; the one-attribute control reads 62 buffers in 0.13 ms. The
  outer scan grows with the schema's `eav_data` rows.
- **Statistics are not the remedy.** A statistics target of 10,000 on
  `value_text` plus an `mcv` extended statistic on `(attr_id, value_text)`
  leaves the estimate where it was (50,010 rows). PostgreSQL has no
  multi-column histogram, and an MCV list cannot hold 200,000 distinct values.
- **The query shape is.** Rewriting the anchor's single-leaf branch to read
  `eav_data x` alone, without the outer `t`, brings the 100,000-row `token gt`
  to 51 buffers and 0.39 ms under the same overestimate (50,477 rows), and
  cuts the half-attribute `token gt` from 98.3 to 65.5 ms. At 20,000 rows it
  removes only the outer scan, about 5 ms (20.7 to 15.5 ms; 20,021 to 19,804
  buffers, the latter on a reloaded table): the rest is the page's `entity_main` merge join
  and `eav_data` hash join, which the planner gives up for nested loops once
  the tables are larger, as at 100,000 rows.

**Disposition:** nothing in #816's one-index contract changes, and no index
or statistics object is added. The scale-dependent cost is #881's outer
`eav_data` scan; this benchmark is a reproduction of it on current `main` and
measures what removing it is worth for one EAV leaf. Whether the rewrite is
equivalent for every condition (negations, several leaves, rows without EAV
values) is #881's to establish. The page joins on small tables are bounded by
the table sizes and need no separate fix until #881's change is measured with
this benchmark.

## How it was measured

`TestMixedEAVTextRangePlans`
(`internal/e2e_harness/production/eav_mixed_plan_e2e_test.go`) runs on the
production harness. CI runs it at 20,000 rows; the recorded run used
`FORMA_EAV_PLAN_ROWS=20000,100000` and wrote its record to files:

```bash
FORMA_EAV_PLAN_EVIDENCE=<dir> FORMA_EAV_PLAN_ROWS=20000,100000 GOTOOLCHAIN=go1.26.0+auto \
  go test ./internal/e2e_harness/production/ -tags=e2e \
  -run TestMixedEAVTextRangePlans -v -count=1 -timeout=20m
```

For each size it records the server version, the database and column
collations, the attribute ids, the index DDL, the full result table and, for
the cases listed in `textPlanCases`, the SQL the repository sent, its
arguments and PostgreSQL's `EXPLAIN (ANALYZE, BUFFERS)` text. It logs the same
record when `FORMA_EAV_PLAN_EVIDENCE` is unset.

**Recorded run.** Revision `9fa2824`, which adds only test code to `main` at
`bfeead9`: the SQL generation and `init-db` are `main`'s. PostgreSQL 16.15
(the harness's `postgres:16` image), database collation `en_US.utf8`,
`eav_data.value_text` under `pg_catalog."C"`. All reads were shared-buffer
hits. Its records are committed unedited, below a short header, as
[N = 20,000](eav-text-range-plans/rows-20000.md) and
[N = 100,000](eav-text-range-plans/rows-100000.md). Times are
`EXPLAIN ANALYZE`'s, from one run on a shared machine: across earlier runs the
same 100,000-row `token gt` plan measured between 26 and 57 ms. Read buffers
and estimates as the evidence, milliseconds as indicative.

**Fixture.** Row *i* of `e2e_wide` (1..N) has `title` `t%06d` and `rank`
*i* mod 1000, and a random uuid as its row id. `note` holds the title;
`token` holds the uuid `00000000-0000-4000-8000-%012x` of *i*. Both sort as
*i* does. The loads are `note + token` (both on every row), `note only`,
`token only`, and `note + token on 1 row in 20` (all notes, a token on every
20th row: per-attribute cardinality skew). `init-db`'s `eav_data` text index
and a declared `(rank, title)` index are built. `eav_data`'s autovacuum is
off, so its statistics change only when the test says.

**Statistics states.** `missing`: loaded, never analyzed or vacuumed, as
after a bulk load before autovacuum runs. `fresh`: `VACUUM (ANALYZE)`, as
autovacuum leaves it. `stale`: analyzed with notes only, then tokens added.

**Probes.** For each attribute: `equals`, `in` of two values, the four
ranges with a bound that selects 5 rows at each end of the attribute's
values, and a `gt` and an `lt` that select half of it. The composite control
ranges `title` under `rank = 7`. Each runs through
`DBPersistentRecordRepository.QueryPersistentRecords`, the hot-tier
PostgreSQL route, with a page of 1,000. The test explains the SQL the
repository sent, with its arguments.

**Metrics.** "est" and "actual" are the predicate node's rows per loop.
"Scan output rows" are the rows the plan's scans of a relation returned
after their filters (Actual Rows × Loops). "Rows filtered by scans" are the
rows those scans read and then discarded by a Filter or an index recheck
(Rows Removed by Filter or by Index Recheck × Loops). Neither counts the
index entries a btree passes over inside an Index Cond, so neither is a
scan's whole work: the buffers are.

**Asserted:** every probe's rows and total against a bytewise comparison in
Go; eligibility of the index for every selective probe (its plan, or its plan
with sequential scans off, seeks the index with the column in the Index Cond);
and pre-#728 estimates within a factor of two of the compliant table's
wherever either exceeds 1% of `eav_data`. **Reported only:** the plan choice,
estimates, buffers and times. A scan is the right plan for half of an
attribute, and the plan an overestimate yields depends on costs the test does
not pin. Where that plan merge-joins `entity_main`, how far the walk goes
depends on the random row ids, so its buffers vary from run to run.

**Not covered.** No row is unflushed in `change_log`, so the anchor's hot
branch is empty. The federated hot leg renders the same condition, but only
over the dirty window (#858). Generic plans of a prepared statement were not
measured; the repository's arguments reach the planner as custom-plan values.

## Results

The full tables, and the plan trees cited below, are in the
[recorded run's files](#plan-evidence); the test prints them on every run.
Below, "control" is the one-attribute load, and "mixed" is `note + token`,
`fresh` unless stated otherwise.

### A selective range beyond another attribute

`token gt` selecting the last 5 tokens (`note lt` selecting the first 5
notes is its mirror and measured the same):

| N | eav_data | est | actual | sequential scans | scan output rows (eav_data / entity_main) | buffers | ms |
| ---: | --- | ---: | ---: | --- | --- | ---: | ---: |
| 20,000 | control | 2 | 5 | none | 15 / 5 | 57 | 0.12 |
| 20,000 | mixed | 10,173 | 5 | eav_data ×2 | 80,005 / 19,251 | 20,021 | 20.74 |
| 20,000 | mixed, pre-#728 SQL, default collation | 10,182 | 5 | eav_data ×2 | 80,005 / 19,251 | 20,337 | 20.49 |
| 20,000 | mixed, target 10,000 + mcv | 10,002 | 5 | eav_data ×2 | 80,005 / 19,251 | 20,337 | 20.44 |
| 20,000 | mixed, anchor without outer `t` | 10,182 | 5 | eav_data ×1 | 40,005 / 19,251 | 19,804 | 15.53 |
| 100,000 | control | 127 | 5 | none | 15 / 5 | 62 | 0.13 |
| 100,000 | mixed | 50,878 | 5 | eav_data ×1 | 200,015 / 5 | 1,923 | 25.70 |
| 100,000 | mixed, pre-#728 SQL, default collation | 50,918 | 5 | eav_data ×1 | 200,015 / 5 | 2,715 | 26.70 |
| 100,000 | mixed, target 10,000 + mcv | 50,010 | 5 | eav_data ×1 | 200,015 / 5 | 2,715 | 26.26 |
| 100,000 | mixed, anchor without outer `t` | 50,477 | 5 | none | 15 / 5 | 51 | 0.39 |
| 100,000 | token on 1 row in 20 | 4,926 | 5 | eav_data ×1 | 105,015 / 5 | 846 | 12.88 |

No scan in these rows filters anything out; the sequential scans' output is
what they read. The predicate is an Index Only Scan of `eav_data_text_idx`
with `value_text` in its Index Cond in every row. The last three passes run
on a reloaded `eav_data`, whose earlier, deleted loads still take up pages,
and the pre-#728 pass also on a table its `ALTER` rewrote; that is why their
buffer counts at the same plan differ from the mixed row's.

In-range bounds are not affected: the first 5 tokens of the mixed load
(`token lt`) read 62 buffers at 20,000 rows and 67 at 100,000, like the
control. A bound in the histogram's last bucket can be overestimated within
one attribute too: the last 5 notes were estimated at 200 rows at 20,000,
which put a sequential scan of `entity_main` into the note-only control's
plan (749 buffers) and the merge-join walk into the mixed one's (19,662).

### Plan evidence

The recorded run's files hold the generated SQL, arguments and full
`EXPLAIN (ANALYZE, BUFFERS)` tree of these cases at both sizes: mixed
`token gt 5` and `note lt 5`, the token-only control, mixed `token gt half`,
the stale `token in`, and `token gt 5` under the anchor rewrite, the richer
statistics and the pre-#728 SQL. What the trees show:

- **Mixed `token gt 5`, 100,000 rows.** Under the anchor's `HashAggregate`,
  a `Hash Semi Join` takes `Seq Scan on eav_data t` (200,000 rows, 1,872
  buffers, 12 ms; its Filter is only `schema_id`) as its outer side and
  hashes the predicate's `Index Only Scan using eav_data_text_idx on
  eav_data x` (estimated 50,878 rows, actual 5, 4 buffers). The page then
  reads `entity_main` and `eav_data` by primary key, 5 loops each.
- **The control** puts the same index-only scan, estimated at 127 rows,
  under a `Nested Loop` that probes `eav_data_pkey` for `t` five times; no
  sequential scan runs.
- **The rewrite** keeps the overestimate (50,477 rows) but has no `t` to
  join: the `HashAggregate` reads the index-only scan directly, and the plan
  is the control's.
- **Mixed `token gt 5`, 20,000 rows.** The anchor's outer scan reads 40,000
  rows in 375 buffers. The page's estimated 1,000 rows bring a `Merge Join`
  whose `Index Scan using entity_main_pkey` walks 19,251 rows (19,265
  buffers), and a `Hash Join` whose `Seq Scan on eav_data e` hashes all
  40,000 `eav_data` rows (375 buffers, about 10 ms) for the page's
  attributes. Under the rewrite only the outer scan is gone.
- **Pre-#728 SQL and richer statistics** show the 100,000-row mixed tree
  with the same nodes and estimates within 2%; their extra buffers are the
  rewritten and reloaded table's pages.
- **Stale `token in`, 100,000 rows.** Estimated at 1 row, the `in` list is a
  Filter on an index-only scan of the attribute's `(schema_id, attr_id)`
  prefix: 2 rows out, 99,998 removed by the filter, once in the anchor's
  flushed branch and once in its `change_log` branch.

### Statistics states

| N | probe | missing | fresh | stale (note only) |
| ---: | --- | ---: | ---: | ---: |
| 20,000 | `token gt` 5 rows | 108 buf, 0.16 ms | 20,021 buf, 20.74 ms | 102 buf, 0.17 ms |
| 20,000 | `token gt` half (10,000) | 128,444 buf, 72.02 ms | 2,813 buf, 37.09 ms | 108,448 buf, 70.51 ms |
| 20,000 | `token in` (2) | 918 buf, 8.01 ms | 32 buf, 0.10 ms | 982 buf, 8.38 ms |
| 100,000 | `token gt` 5 rows | 113 buf, 0.21 ms | 1,923 buf, 25.70 ms | 107 buf, 0.17 ms |
| 100,000 | `token gt` half (50,000) | 611,184 buf, 525.61 ms | 11,398 buf, 98.33 ms | 511,188 buf, 347.81 ms |
| 100,000 | `token in` (2) | 56 buf, 0.15 ms | 34 buf, 0.10 ms | 4,560 buf, 40.84 ms |

Without statistics, or with statistics that have not seen the attribute, the
planner estimates one row for every token predicate. Selective ranges then
plan well, by luck, and half-attribute ranges take nested loops that cost 2
to 5 times the fresh plan's time. `in` can filter the attribute's whole
`(schema_id, attr_id)` prefix instead of seeking its values: its scans return
16 rows but filter out 39,996 at 20,000 rows (missing and stale) and 199,996
at 100,000 (stale). These are the ordinary costs of planning before
`ANALYZE`, not effects of mixing or of the collation: run `ANALYZE` after a
bulk load, as the
[migration](text-collation.md#migrating-a-table-created-before-the-rule)
already does.

### Controls

- **Half-attribute ranges, fresh statistics, attribute a large share of
  `value_text`:** the one-attribute loads, `note + token` (and its passes
  with the rewrite, the richer statistics and the pre-#728 SQL), and the
  notes of the skewed load. Their estimates are within a factor of two (for
  example 14,965 against 10,000 for mixed `token gt` half at 20,000 rows,
  74,820 against 50,000 at 100,000). They plan as a sequential scan of
  `eav_data` for the predicate, or as an index-only scan of
  `eav_data_text_idx` hashed against the anchor's outer sequential scan. No
  recorded plan uses a bitmap read. For half of an attribute, those reads
  are justified.
- **Half-attribute ranges on the skewed load's tokens, fresh statistics.**
  The tokens are 1,000 of `value_text`'s 21,000 values at 20,000 rows (5,000
  of 105,000 at 100,000), all below the notes. The independence product
  described in the [Conclusion](#conclusion) therefore applies within their
  own range: the column's share beyond the bound times the attribute's share.
  `token gt` half is estimated at 979 against 500 (5,008 against 2,500 at
  100,000), within a factor of two. `token lt` half is estimated at 21
  against 500 (q-error 24) and at 123 against 2,500 (q-error 20). Its
  predicate is an index-only scan of `eav_data_text_idx` with no sequential
  scan in the plan, reading 5,708 buffers in 6.83 ms at 20,000 rows and
  17,530 buffers in 18.14 ms at 100,000. The overestimated `gt` half read
  20,527 and 10,841. This underestimate mirrors the selective `token gt`'s
  overestimate and has the same cause, not the collation. The test asserts
  neither plan.
- **Half-attribute ranges, missing statistics, and the tokens under stale
  statistics.** On `note + token`, the four half ranges without statistics
  and the two token half ranges under stale statistics are estimated at 1 row
  against 10,000 (50,000 at 100,000). Each plans as an index-only scan of
  `eav_data_text_idx` with no sequential scan.
  At 20,000 rows these plans read 108,448 to 128,444 buffers in 70 to 76 ms;
  fresh, the same ranges read 2,769 to 2,900. At 100,000 rows they read
  511,188 to 611,186 buffers in 348 to 529 ms; fresh, 11,179 to 11,399 (see
  [Statistics states](#statistics-states)). Under stale statistics the notes,
  which the `ANALYZE` saw, are estimated within one row and plan as
  sequential scans.
- The composite `(rank, title)` index seeks both ranges under `rank = 7` with
  estimates within one row (10 rows at 20,000, 50 at 100,000; 85 and 456
  buffers), as `eav_data`'s statistics cannot affect it.
- `equals` and `in` seek the index under fresh statistics in every load.
