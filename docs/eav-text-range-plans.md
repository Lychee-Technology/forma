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
  is estimated at a quarter of `eav_data` (10,003 rows at 20,000 `e2e_wide`
  rows, 50,629 at 100,000) when it selects 5. In-range bounds (`token lt`,
  `note gt`) are estimated well.
- **The overestimate becomes cost in the anchor query, not in the predicate.**
  The predicate's own index-only scan reads 4 buffers either way. With the
  overestimate, the plan reads every `eav_data` row of the schema through
  the anchor's outer `eav_data t` (the extra scan #881 owns), and, while
  `entity_main` is small enough, walks it in a merge join.
  At 100,000 rows a 5-row `token gt` reads 1,923 buffers in about 26 ms; the
  one-attribute control reads 62 buffers in 0.14 ms. The outer scan grows
  with the schema's `eav_data` rows.
- **Statistics are not the remedy.** A statistics target of 10,000 on
  `value_text` plus an `mcv` extended statistic on `(attr_id, value_text)`
  leaves the estimate where it was (50,010 rows). PostgreSQL has no
  multi-column histogram, and an MCV list cannot hold 200,000 distinct values.
- **The query shape is.** Rewriting the anchor's single-leaf branch to read
  `eav_data x` alone, without the outer `t`, brings the 100,000-row `token gt`
  to 51 buffers and 0.44 ms under the same overestimate, and cuts the time of
  the half-attribute ranges by about a third (105.7 to 68.7 ms). At 20,000 rows it does not
  help much (18,992 to 18,775 buffers): the rest of that cost is the
  `entity_main` merge-join walk, which the planner gives up for nested loops
  once the table is larger, as at 100,000 rows.

**Disposition:** nothing in #816's one-index contract changes, and no index
or statistics object is added. The scale-dependent cost is #881's outer
`eav_data` scan; this benchmark is a reproduction of it on current `main` and
measures what removing it is worth for one EAV leaf. Whether the rewrite is
equivalent for every condition (negations, several leaves, rows without EAV
values) is #881's to establish. The bounded merge-join walk on small tables
needs no separate fix until #881's change is measured with this benchmark.

## How it was measured

`TestMixedEAVTextRangePlans`
(`internal/e2e_harness/production/eav_mixed_plan_e2e_test.go`) runs on the
production harness. CI runs it at 20,000 rows; the recorded run used
`FORMA_EAV_PLAN_ROWS=20000,100000`:

```bash
FORMA_EAV_PLAN_ROWS=20000,100000 GOTOOLCHAIN=go1.26.0+auto \
  go test ./internal/e2e_harness/production/ -tags=e2e \
  -run TestMixedEAVTextRangePlans -v -count=1 -timeout=20m
```

It logs the server version, the database collation, the index DDL, each
size's full result table in Markdown, and the generated SQL with its
`EXPLAIN (ANALYZE, BUFFERS)` text for `token gt` on the mixed and the
token-only loads.

**Recorded run.** `main` at `bfeead9`, PostgreSQL 16.15 (the harness's
`postgres:16` image), database collation `en_US.utf8`, `eav_data.value_text`
under `pg_catalog."C"`. All reads were shared-buffer hits. Times are
`EXPLAIN ANALYZE`'s, from one run on a shared machine: across three runs the
same 100,000-row `token gt` plan measured between 26 and 57 ms. Read buffers and estimates as the evidence,
milliseconds as indicative.

**Fixture.** Row *i* of `e2e_wide` (1..N) has `title` `t%06d` and `rank`
*i* mod 1000. `note` holds the title; `token` holds the uuid
`00000000-0000-4000-8000-%012x` of *i*. Both sort as *i* does. The loads are
`note + token` (both on every row), `note only`, `token only`, and
`note + token on 1 row in 20` (all notes, a token on every 20th row: per-attribute
cardinality skew). `init-db`'s `eav_data` text index and a declared
`(rank, title)` index are built. `eav_data`'s autovacuum is off, so its
statistics change only when the test says.

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

**Asserted:** every probe's rows and total against a bytewise comparison in
Go; eligibility of the index for every selective probe (its plan, or its plan
with sequential scans off, seeks the index with the column in the Index Cond);
and pre-#728 estimates within a factor of two of the compliant table's
wherever either exceeds 1% of `eav_data`. **Reported only:** the plan choice,
estimates, buffers and times. A scan is the right plan for half of an
attribute, and the plan an overestimate yields depends on costs the test does
not pin.

**Not covered.** No row is unflushed in `change_log`, so the anchor's hot
branch is empty. The federated hot leg renders the same condition, but only
over the dirty window (#858). Generic plans of a prepared statement were not
measured; the repository's arguments reach the planner as custom-plan values.

## Results

The full tables of the recorded run are in the
[appendix](#appendix-the-recorded-runs-full-tables); the test prints them on
every run. Below, "control" is the one-attribute load, and
"mixed" is `note + token`, `fresh` unless stated otherwise.

### A selective range beyond another attribute

`token gt` selecting the last 5 tokens (`note lt` selecting the first 5
notes is its mirror and measured the same):

| N | eav_data | est | actual | sequential scans | rows read (eav_data / entity_main) | buffers | ms |
| ---: | --- | ---: | ---: | --- | --- | ---: | ---: |
| 20,000 | control | 2 | 5 | none | 15 / 5 | 57 | 0.12 |
| 20,000 | mixed | 10,003 | 5 | eav_data ×2 | 80,005 / 18,209 | 18,992 | 22.78 |
| 20,000 | mixed, pre-#728 SQL, default collation | 10,025 | 5 | eav_data ×2 | 80,005 / 18,209 | 19,308 | 20.52 |
| 20,000 | mixed, target 10,000 + mcv | 10,002 | 5 | eav_data ×2 | 80,005 / 18,209 | 19,308 | 20.58 |
| 20,000 | mixed, anchor without outer `t` | 10,165 | 5 | eav_data ×1 | 40,005 / 18,209 | 18,775 | 16.83 |
| 100,000 | control | 112 | 5 | none | 15 / 5 | 62 | 0.14 |
| 100,000 | mixed | 50,629 | 5 | eav_data ×1 | 200,015 / 5 | 1,923 | 26.30 |
| 100,000 | mixed, pre-#728 SQL, default collation | 50,735 | 5 | eav_data ×1 | 200,015 / 5 | 2,715 | 27.87 |
| 100,000 | mixed, target 10,000 + mcv | 50,010 | 5 | eav_data ×1 | 200,015 / 5 | 2,715 | 28.41 |
| 100,000 | mixed, anchor without outer `t` | 50,762 | 5 | none | 15 / 5 | 51 | 0.44 |
| 100,000 | token on 1 row in 20 | 4,848 | 5 | eav_data ×2 | 210,005 / 5 | 1,614 | 47.10 |

The predicate is an Index Only Scan of `eav_data_text_idx` with `value_text` in
its Index Cond in every row. The last three passes run on a reloaded
`eav_data`, whose earlier, deleted loads still take up pages, and the pre-#728
pass also on a table its `ALTER` rewrote; that is why their buffer counts at
the same plan differ from the mixed row's.

In-range bounds are not affected: the first 5 tokens of the mixed load
(`token lt`) read 62 buffers at 20,000 rows and 67 at 100,000, like the
control. A bound in the histogram's last bucket can be overestimated within
one attribute too: the last 5 notes were estimated at 200 rows at 20,000,
which put a merge-join walk of `entity_main` into the plan (749 buffers in the
note-only control, 18,633 mixed).

### Statistics states

| N | probe | missing | fresh | stale (note only) |
| ---: | --- | ---: | ---: | ---: |
| 20,000 | `token gt` 5 rows | 108 buf, 0.17 ms | 18,992 buf, 22.78 ms | 102 buf, 0.16 ms |
| 20,000 | `token gt` half (10,000) | 128,444 buf, 74.41 ms | 2,850 buf, 39.57 ms | 108,448 buf, 71.35 ms |
| 20,000 | `token in` (2) | 918 buf, 7.47 ms | 32 buf, 0.13 ms | 982 buf, 8.17 ms |
| 100,000 | `token gt` 5 rows | 113 buf, 0.19 ms | 1,923 buf, 26.30 ms | 107 buf, 0.18 ms |
| 100,000 | `token gt` half (50,000) | 611,184 buf, 566.83 ms | 11,398 buf, 105.70 ms | 511,188 buf, 355.11 ms |
| 100,000 | `token in` (2) | 56 buf, 0.16 ms | 34 buf, 0.11 ms | 4,560 buf, 41.68 ms |

Without statistics, or with statistics that have not seen the attribute, the
planner estimates one row for every token predicate. Selective ranges then
plan well, by luck, and half-attribute ranges take nested loops that cost 2
to 5 times the fresh plan's time. `in` filters the attribute's whole
`(schema_id, attr_id)` prefix instead of seeking its values. These are the
ordinary costs of planning before `ANALYZE`, not effects of mixing or of the
collation: run `ANALYZE` after a bulk load, as the
[migration](text-collation.md#migrating-a-table-created-before-the-rule)
already does.

### Controls

- Half-attribute ranges plan as scans or bitmap reads under every load, with
  estimates within a factor of two (for example 15,007 against 10,000 for
  mixed `token gt` half at 20,000 rows). Those scans are justified.
- The composite `(rank, title)` index seeks both ranges under `rank = 7` with
  estimates within one row (10 rows at 20,000, 50 at 100,000; 85 and 456
  buffers), as `eav_data`'s statistics cannot affect it.
- `equals` and `in` seek the index under fresh statistics in every load.

## Appendix: the recorded run's full tables

`mixed` is note and token on every row; `skew (token 1/20)` is a token on 1 row
in 20; `no outer t` is the anchor rewrite; `pre-#728` is a default-collation
column queried without `COLLATE "C"`. "seeks" means the plan's Index Cond
names the column. "rows read" sums the plan's `eav_data` / `entity_main` scans.
`IOS` is an Index Only Scan.

<details><summary>N = 20,000</summary>

| eav_data | stats | probe | match | predicate | seeks | est | act | seq scans | rows read eav/main | buf | ms |
|---|---|---|--:|---|---|--:|--:|---|---|--:|--:|
| mixed | missing | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 28 | 0.18 |
| mixed | missing | note in | 2 | IOS text_idx | no | 1 | 2 |  | 16/2 | 582 | 7.78 |
| mixed | missing | note gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.24 |
| mixed | missing | note gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.23 |
| mixed | missing | note lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.22 |
| mixed | missing | note lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.25 |
| mixed | missing | note gt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 128276 | 94.12 |
| mixed | missing | note lt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 128274 | 75.18 |
| mixed | missing | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 28 | 0.12 |
| mixed | missing | token in | 2 | IOS text_idx | no | 1 | 2 |  | 16/2 | 918 | 7.47 |
| mixed | missing | token gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.17 |
| mixed | missing | token gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.16 |
| mixed | missing | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.16 |
| mixed | missing | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 108 | 0.16 |
| mixed | missing | token gt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 128444 | 74.41 |
| mixed | missing | token lt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 128442 | 73.92 |
| mixed | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 26 | 0.09 |
| mixed | fresh | note in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 32 | 0.10 |
| mixed | fresh | note gt 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×1 | 40015/18209 | 18633 | 16.80 |
| mixed | fresh | note gte 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×1 | 40015/18209 | 18633 | 16.73 |
| mixed | fresh | note lt 5 | 5 | IOS text_idx | yes | 10197 | 5 | eav ×2 | 80005/18947 | 19731 | 22.52 |
| mixed | fresh | note lte 5 | 5 | IOS text_idx | yes | 10197 | 5 | eav ×2 | 80005/18947 | 19731 | 21.57 |
| mixed | fresh | note gt half | 10000 | IOS text_idx | yes | 4799 | 10000 | eav ×2 | 90000/1984 | 2806 | 36.08 |
| mixed | fresh | note lt half | 10000 | IOS text_idx | yes | 15195 | 10000 | eav ×2 | 90000/2020 | 2843 | 37.78 |
| mixed | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 26 | 0.15 |
| mixed | fresh | token in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 32 | 0.13 |
| mixed | fresh | token gt 5 | 5 | IOS text_idx | yes | 10003 | 5 | eav ×2 | 80005/18209 | 18992 | 22.78 |
| mixed | fresh | token gte 5 | 5 | IOS text_idx | yes | 10003 | 5 | eav ×2 | 80005/18209 | 18992 | 22.32 |
| mixed | fresh | token lt 5 | 5 | IOS text_idx | yes | 4 | 5 |  | 25/5 | 62 | 0.17 |
| mixed | fresh | token lte 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 62 | 0.16 |
| mixed | fresh | token gt half | 10000 | IOS text_idx | yes | 15007 | 10000 | eav ×2 | 90000/1984 | 2850 | 39.57 |
| mixed | fresh | token lt half | 10000 | IOS text_idx | yes | 4998 | 10000 | eav ×2 | 90000/2020 | 2887 | 37.25 |
| mixed | fresh | rank=7, title gt half | 10 | IOS (rank, title) | yes | 10 | 10 |  | 20/20 | 85 | 0.17 |
| mixed | fresh | rank=7, title lt half | 10 | IOS (rank, title) | yes | 10 | 10 |  | 20/20 | 85 | 0.18 |
| note only | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 25 | 0.13 |
| note only | fresh | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 30 | 0.21 |
| note only | fresh | note gt 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×1, main ×1 | 20010/20000 | 749 | 7.65 |
| note only | fresh | note gte 5 | 5 | IOS text_idx | yes | 201 | 5 | eav ×1, main ×1 | 20010/20000 | 749 | 7.29 |
| note only | fresh | note lt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.12 |
| note only | fresh | note lte 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.11 |
| note only | fresh | note gt half | 10000 | Seq eav_data | no | 10000 | 10000 | eav ×3 | 50000/1984 | 3552 | 26.10 |
| note only | fresh | note lt half | 10000 | Seq eav_data | no | 9999 | 10000 | eav ×3 | 50000/2020 | 3589 | 25.89 |
| mixed | stale | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 32 | 0.24 |
| mixed | stale | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 10/2 | 38 | 0.15 |
| mixed | stale | note gt 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×1, main ×1 | 40015/20000 | 754 | 12.86 |
| mixed | stale | note gte 5 | 5 | IOS text_idx | yes | 201 | 5 | eav ×1, main ×1 | 40015/20000 | 754 | 13.09 |
| mixed | stale | note lt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 25/5 | 77 | 0.17 |
| mixed | stale | note lte 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 25/5 | 77 | 0.17 |
| mixed | stale | note gt half | 10000 | Seq eav_data | no | 10000 | 10000 | eav ×3 | 90000/1984 | 3552 | 39.75 |
| mixed | stale | note lt half | 10000 | Seq eav_data | no | 9999 | 10000 | eav ×3 | 90000/2020 | 3589 | 39.32 |
| mixed | stale | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 32 | 0.10 |
| mixed | stale | token in | 2 | IOS text_idx | no | 1 | 2 |  | 16/2 | 982 | 8.17 |
| mixed | stale | token gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 102 | 0.16 |
| mixed | stale | token gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 102 | 0.16 |
| mixed | stale | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 112 | 0.16 |
| mixed | stale | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 112 | 0.16 |
| mixed | stale | token gt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 108448 | 71.35 |
| mixed | stale | token lt half | 10000 | IOS text_idx | yes | 1 | 10000 |  | 62000/1000 | 108666 | 68.99 |
| token only | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 25 | 0.09 |
| token only | fresh | token in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 30 | 0.10 |
| token only | fresh | token gt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.12 |
| token only | fresh | token gte 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.13 |
| token only | fresh | token lt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.12 |
| token only | fresh | token lte 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 15/5 | 57 | 0.12 |
| token only | fresh | token gt half | 10000 | Seq eav_data | no | 10000 | 10000 | eav ×3 | 50000/1984 | 3792 | 25.53 |
| token only | fresh | token lt half | 10000 | Seq eav_data | no | 9999 | 10000 | eav ×3 | 50000/2020 | 3829 | 26.04 |
| skew (token 1/20) | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 25 | 0.09 |
| skew (token 1/20) | fresh | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 30 | 0.10 |
| skew (token 1/20) | fresh | note gt 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×2, main ×1 | 42005/20000 | 531 | 10.38 |
| skew (token 1/20) | fresh | note gte 5 | 5 | IOS text_idx | yes | 201 | 5 | eav ×2, main ×1 | 42005/20000 | 531 | 10.41 |
| skew (token 1/20) | fresh | note lt 5 | 5 | IOS text_idx | yes | 999 | 5 | eav ×2 | 42005/18947 | 19299 | 13.16 |
| skew (token 1/20) | fresh | note lte 5 | 5 | IOS text_idx | yes | 1000 | 5 | eav ×2 | 42005/18947 | 19299 | 13.10 |
| skew (token 1/20) | fresh | note gt half | 10000 | Seq eav_data | no | 9400 | 10000 | eav ×3 | 52000/1984 | 2469 | 25.91 |
| skew (token 1/20) | fresh | note lt half | 10000 | Seq eav_data | no | 10599 | 10000 | eav ×3 | 52000/2020 | 2506 | 25.48 |
| skew (token 1/20) | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 26 | 0.13 |
| skew (token 1/20) | fresh | token in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 32 | 0.12 |
| skew (token 1/20) | fresh | token gt 5 | 5 | IOS text_idx | yes | 960 | 5 | eav ×2 | 42005/19500 | 19857 | 13.40 |
| skew (token 1/20) | fresh | token gte 5 | 5 | IOS text_idx | yes | 960 | 5 | eav ×2 | 42005/19500 | 19857 | 13.40 |
| skew (token 1/20) | fresh | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 94 | 0.17 |
| skew (token 1/20) | fresh | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 92 | 0.16 |
| skew (token 1/20) | fresh | token gt half | 500 | IOS text_idx | yes | 979 | 500 | eav ×2 | 42500/19991 | 20555 | 18.16 |
| skew (token 1/20) | fresh | token lt half | 500 | IOS text_idx | yes | 21 | 500 |  | 2500/500 | 5708 | 6.35 |
| mixed, no outer t | fresh | note gt 5 | 5 | IOS text_idx | yes | 201 | 5 | eav ×1, main ×1 | 40005/20000 | 746 | 12.34 |
| mixed, no outer t | fresh | note gte 5 | 5 | IOS text_idx | yes | 201 | 5 | eav ×1, main ×1 | 40005/20000 | 746 | 12.25 |
| mixed, no outer t | fresh | note lt 5 | 5 | IOS text_idx | yes | 10034 | 5 | eav ×1 | 40005/18947 | 19514 | 17.09 |
| mixed, no outer t | fresh | note lte 5 | 5 | IOS text_idx | yes | 10035 | 5 | eav ×1 | 40005/18947 | 19514 | 17.53 |
| mixed, no outer t | fresh | note gt half | 10000 | IOS text_idx | yes | 5017 | 10000 | eav ×1 | 50000/1984 | 2637 | 28.03 |
| mixed, no outer t | fresh | note lt half | 10000 | Seq eav_data | no | 15051 | 10000 | eav ×2 | 50000/2020 | 3095 | 30.93 |
| mixed, no outer t | fresh | token gt 5 | 5 | IOS text_idx | yes | 10165 | 5 | eav ×1 | 40005/18209 | 18775 | 16.83 |
| mixed, no outer t | fresh | token gte 5 | 5 | IOS text_idx | yes | 10165 | 5 | eav ×1 | 40005/18209 | 18775 | 17.15 |
| mixed, no outer t | fresh | token lt 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 15/5 | 46 | 0.16 |
| mixed, no outer t | fresh | token lte 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 15/5 | 46 | 0.12 |
| mixed, no outer t | fresh | token gt half | 10000 | Seq eav_data | no | 14950 | 10000 | eav ×2 | 50000/1984 | 3058 | 30.88 |
| mixed, no outer t | fresh | token lt half | 10000 | IOS text_idx | yes | 4980 | 10000 | eav ×1 | 50000/2020 | 2767 | 28.33 |
| mixed | target 10k + mcv | note gt 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 62 | 0.25 |
| mixed | target 10k + mcv | note gte 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 62 | 0.16 |
| mixed | target 10k + mcv | note lt 5 | 5 | IOS text_idx | yes | 10005 | 5 | eav ×2 | 80005/18947 | 20047 | 22.91 |
| mixed | target 10k + mcv | note lte 5 | 5 | IOS text_idx | yes | 10005 | 5 | eav ×2 | 80005/18947 | 20047 | 29.44 |
| mixed | target 10k + mcv | note gt half | 10000 | IOS text_idx | yes | 5000 | 10000 | eav ×2 | 90000/1984 | 3170 | 35.32 |
| mixed | target 10k + mcv | note lt half | 10000 | Seq eav_data | no | 20000 | 10000 | eav ×3 | 90000/2020 | 3628 | 38.15 |
| mixed | target 10k + mcv | token gt 5 | 5 | IOS text_idx | yes | 10002 | 5 | eav ×2 | 80005/18209 | 19308 | 20.58 |
| mixed | target 10k + mcv | token gte 5 | 5 | IOS text_idx | yes | 10002 | 5 | eav ×2 | 80005/18209 | 19308 | 20.71 |
| mixed | target 10k + mcv | token lt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 25/5 | 62 | 0.16 |
| mixed | target 10k + mcv | token lte 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 62 | 0.15 |
| mixed | target 10k + mcv | token gt half | 10000 | Seq eav_data | no | 15000 | 10000 | eav ×3 | 90000/1984 | 3591 | 37.46 |
| mixed | target 10k + mcv | token lt half | 10000 | IOS text_idx | yes | 5000 | 10000 | eav ×2 | 90000/2020 | 3300 | 35.09 |
| mixed, pre-#728 | fresh | note gt 5 | 5 | IOS text_idx | yes | 199 | 5 | eav ×1 | 40015/18209 | 18791 | 15.27 |
| mixed, pre-#728 | fresh | note gte 5 | 5 | IOS text_idx | yes | 200 | 5 | eav ×1 | 40015/18209 | 18791 | 15.03 |
| mixed, pre-#728 | fresh | note lt 5 | 5 | IOS text_idx | yes | 10174 | 5 | eav ×2 | 80005/18947 | 20047 | 20.38 |
| mixed, pre-#728 | fresh | note lte 5 | 5 | IOS text_idx | yes | 10175 | 5 | eav ×2 | 80005/18947 | 20047 | 20.16 |
| mixed, pre-#728 | fresh | note gt half | 10000 | IOS text_idx | yes | 4788 | 10000 | eav ×2 | 90000/1984 | 3122 | 35.10 |
| mixed, pre-#728 | fresh | note lt half | 10000 | IOS text_idx | yes | 15162 | 10000 | eav ×2 | 90000/2020 | 3159 | 36.17 |
| mixed, pre-#728 | fresh | token gt 5 | 5 | IOS text_idx | yes | 10025 | 5 | eav ×2 | 80005/18209 | 19308 | 20.52 |
| mixed, pre-#728 | fresh | token gte 5 | 5 | IOS text_idx | yes | 10025 | 5 | eav ×2 | 80005/18209 | 19308 | 20.78 |
| mixed, pre-#728 | fresh | token lt 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 25/5 | 62 | 0.15 |
| mixed, pre-#728 | fresh | token lte 5 | 5 | IOS text_idx | yes | 2 | 5 |  | 25/5 | 62 | 0.15 |
| mixed, pre-#728 | fresh | token gt half | 10000 | IOS text_idx | yes | 14948 | 10000 | eav ×2 | 90000/1984 | 3166 | 39.32 |
| mixed, pre-#728 | fresh | token lt half | 10000 | IOS text_idx | yes | 5101 | 10000 | eav ×2 | 90000/2020 | 3203 | 40.00 |

</details>


<details><summary>N = 100,000</summary>

| eav_data | stats | probe | match | predicate | seeks | est | act | seq scans | rows read eav/main | buf | ms |
|---|---|---|--:|---|---|--:|--:|---|---|--:|--:|
| mixed | missing | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 29 | 0.19 |
| mixed | missing | note in | 2 | IOS text_idx | yes | 1 | 2 |  | 16/2 | 56 | 0.19 |
| mixed | missing | note gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.29 |
| mixed | missing | note gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.27 |
| mixed | missing | note lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.26 |
| mixed | missing | note lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.25 |
| mixed | missing | note gt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 610346 | 541.59 |
| mixed | missing | note lt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 610344 | 524.70 |
| mixed | missing | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 29 | 0.10 |
| mixed | missing | token in | 2 | IOS text_idx | yes | 1 | 2 |  | 16/2 | 56 | 0.16 |
| mixed | missing | token gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.19 |
| mixed | missing | token gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.19 |
| mixed | missing | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.25 |
| mixed | missing | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 113 | 0.19 |
| mixed | missing | token gt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 611184 | 566.83 |
| mixed | missing | token lt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 611186 | 522.62 |
| mixed | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 27 | 0.15 |
| mixed | fresh | note in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 34 | 0.12 |
| mixed | fresh | note gt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.15 |
| mixed | fresh | note gte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.14 |
| mixed | fresh | note lt 5 | 5 | IOS text_idx | yes | 50363 | 5 | eav ×1 | 200015/5 | 1923 | 27.01 |
| mixed | fresh | note lte 5 | 5 | IOS text_idx | yes | 50363 | 5 | eav ×1 | 200015/5 | 1923 | 26.68 |
| mixed | fresh | note gt half | 50000 | IOS text_idx | yes | 25183 | 50000 | eav ×1 | 252000/1000 | 11179 | 103.64 |
| mixed | fresh | note lt half | 50000 | IOS text_idx | yes | 75543 | 50000 | eav ×1 | 252000/1000 | 11179 | 98.18 |
| mixed | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 27 | 0.22 |
| mixed | fresh | token in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 34 | 0.11 |
| mixed | fresh | token gt 5 | 5 | IOS text_idx | yes | 50629 | 5 | eav ×1 | 200015/5 | 1923 | 26.30 |
| mixed | fresh | token gte 5 | 5 | IOS text_idx | yes | 50630 | 5 | eav ×1 | 200015/5 | 1923 | 26.81 |
| mixed | fresh | token lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.14 |
| mixed | fresh | token lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.14 |
| mixed | fresh | token gt half | 50000 | IOS text_idx | yes | 74473 | 50000 | eav ×1 | 252000/1000 | 11398 | 105.70 |
| mixed | fresh | token lt half | 50000 | IOS text_idx | yes | 24800 | 50000 | eav ×1 | 252000/1000 | 11399 | 107.57 |
| mixed | fresh | rank=7, title gt half | 50 | IOS (rank, title) | yes | 49 | 50 |  | 100/100 | 456 | 0.94 |
| mixed | fresh | rank=7, title lt half | 50 | IOS (rank, title) | yes | 51 | 50 |  | 100/100 | 456 | 0.64 |
| note only | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 26 | 0.09 |
| note only | fresh | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 32 | 0.10 |
| note only | fresh | note gt 5 | 5 | IOS text_idx | yes | 1000 | 5 |  | 15/5 | 62 | 0.18 |
| note only | fresh | note gte 5 | 5 | IOS text_idx | yes | 1001 | 5 |  | 15/5 | 62 | 0.13 |
| note only | fresh | note lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 62 | 0.12 |
| note only | fresh | note lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 62 | 0.14 |
| note only | fresh | note gt half | 50000 | Seq eav_data | no | 50000 | 50000 | eav ×2 | 151000/1000 | 13200 | 83.75 |
| note only | fresh | note lt half | 50000 | Seq eav_data | no | 49999 | 50000 | eav ×2 | 151000/1000 | 13200 | 80.96 |
| mixed | stale | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 33 | 0.10 |
| mixed | stale | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 10/2 | 40 | 0.11 |
| mixed | stale | note gt 5 | 5 | IOS text_idx | yes | 1000 | 5 |  | 25/5 | 72 | 0.16 |
| mixed | stale | note gte 5 | 5 | IOS text_idx | yes | 1001 | 5 |  | 25/5 | 72 | 0.16 |
| mixed | stale | note lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 82 | 0.14 |
| mixed | stale | note lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 82 | 0.14 |
| mixed | stale | note gt half | 50000 | Seq eav_data | no | 50000 | 50000 | eav ×2 | 252000/1000 | 14200 | 120.39 |
| mixed | stale | note lt half | 50000 | Seq eav_data | no | 49999 | 50000 | eav ×2 | 252000/1000 | 14200 | 118.75 |
| mixed | stale | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 33 | 0.10 |
| mixed | stale | token in | 2 | IOS text_idx | no | 1 | 2 |  | 16/2 | 4560 | 41.68 |
| mixed | stale | token gt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 107 | 0.18 |
| mixed | stale | token gte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 107 | 0.17 |
| mixed | stale | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 117 | 0.17 |
| mixed | stale | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 117 | 0.17 |
| mixed | stale | token gt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 511188 | 355.11 |
| mixed | stale | token lt half | 50000 | IOS text_idx | yes | 1 | 50000 |  | 302000/1000 | 512144 | 372.40 |
| token only | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 26 | 0.10 |
| token only | fresh | token in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 32 | 0.11 |
| token only | fresh | token gt 5 | 5 | IOS text_idx | yes | 112 | 5 |  | 15/5 | 62 | 0.14 |
| token only | fresh | token gte 5 | 5 | IOS text_idx | yes | 113 | 5 |  | 15/5 | 62 | 0.13 |
| token only | fresh | token lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 62 | 0.14 |
| token only | fresh | token lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 62 | 0.13 |
| token only | fresh | token gt half | 50000 | Seq eav_data | no | 49986 | 50000 | eav ×2 | 151000/1000 | 14002 | 85.07 |
| token only | fresh | token lt half | 50000 | Seq eav_data | no | 50013 | 50000 | eav ×2 | 151000/1000 | 14002 | 107.01 |
| skew (token 1/20) | fresh | note equals | 1 | IOS text_idx | yes | 1 | 1 |  | 5/1 | 26 | 0.10 |
| skew (token 1/20) | fresh | note in | 2 | IOS text_idx | yes | 2 | 2 |  | 6/2 | 32 | 0.10 |
| skew (token 1/20) | fresh | note gt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 63 | 0.14 |
| skew (token 1/20) | fresh | note gte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 63 | 0.17 |
| skew (token 1/20) | fresh | note lt 5 | 5 | IOS text_idx | yes | 4997 | 5 | eav ×2 | 210005/5 | 1612 | 46.18 |
| skew (token 1/20) | fresh | note lte 5 | 5 | IOS text_idx | yes | 4997 | 5 | eav ×2 | 210005/5 | 1612 | 47.37 |
| skew (token 1/20) | fresh | note gt half | 50000 | Seq eav_data | no | 46978 | 50000 | eav ×3 | 260000/1000 | 6381 | 120.60 |
| skew (token 1/20) | fresh | note lt half | 50000 | Seq eav_data | no | 52971 | 50000 | eav ×3 | 260000/1000 | 6381 | 117.84 |
| skew (token 1/20) | fresh | token equals | 1 | IOS text_idx | yes | 1 | 1 |  | 8/1 | 27 | 0.17 |
| skew (token 1/20) | fresh | token in | 2 | IOS text_idx | yes | 1 | 2 |  | 10/2 | 34 | 0.11 |
| skew (token 1/20) | fresh | token gt 5 | 5 | IOS text_idx | yes | 4848 | 5 | eav ×2 | 210005/5 | 1614 | 47.10 |
| skew (token 1/20) | fresh | token gte 5 | 5 | IOS text_idx | yes | 4849 | 5 | eav ×2 | 210005/5 | 1614 | 49.10 |
| skew (token 1/20) | fresh | token lt 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 99 | 0.33 |
| skew (token 1/20) | fresh | token lte 5 | 5 | IOS text_idx | yes | 1 | 5 |  | 40/5 | 97 | 0.18 |
| skew (token 1/20) | fresh | token gt half | 2500 | IOS text_idx | yes | 4901 | 2500 | eav ×2 | 212500/1000 | 6634 | 59.90 |
| skew (token 1/20) | fresh | token lt half | 2500 | IOS text_idx | yes | 149 | 2500 |  | 9500/1000 | 17530 | 19.23 |
| mixed, no outer t | fresh | note gt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 51 | 0.14 |
| mixed, no outer t | fresh | note gte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 51 | 0.13 |
| mixed, no outer t | fresh | note lt 5 | 5 | IOS text_idx | yes | 50233 | 5 |  | 15/5 | 51 | 0.48 |
| mixed, no outer t | fresh | note lte 5 | 5 | IOS text_idx | yes | 50233 | 5 |  | 15/5 | 51 | 0.74 |
| mixed, no outer t | fresh | note gt half | 50000 | IOS text_idx | yes | 25118 | 50000 |  | 52000/1000 | 9549 | 100.32 |
| mixed, no outer t | fresh | note lt half | 50000 | Seq eav_data | no | 75348 | 50000 | eav ×1 | 52000/1000 | 11666 | 63.81 |
| mixed, no outer t | fresh | token gt 5 | 5 | IOS text_idx | yes | 50762 | 5 |  | 15/5 | 51 | 0.44 |
| mixed, no outer t | fresh | token gte 5 | 5 | IOS text_idx | yes | 50762 | 5 |  | 15/5 | 51 | 0.42 |
| mixed, no outer t | fresh | token lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 51 | 0.18 |
| mixed, no outer t | fresh | token lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 15/5 | 51 | 0.13 |
| mixed, no outer t | fresh | token gt half | 50000 | Seq eav_data | no | 73786 | 50000 | eav ×1 | 52000/1000 | 11666 | 68.71 |
| mixed, no outer t | fresh | token lt half | 50000 | IOS text_idx | yes | 25746 | 50000 |  | 52000/1000 | 10029 | 56.89 |
| mixed | target 10k + mcv | note gt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.23 |
| mixed | target 10k + mcv | note gte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.19 |
| mixed | target 10k + mcv | note lt 5 | 5 | IOS text_idx | yes | 50012 | 5 | eav ×1 | 200015/5 | 2715 | 28.96 |
| mixed | target 10k + mcv | note lte 5 | 5 | IOS text_idx | yes | 50012 | 5 | eav ×1 | 200015/5 | 2715 | 28.26 |
| mixed | target 10k + mcv | note gt half | 50000 | IOS text_idx | yes | 25000 | 50000 | eav ×1 | 252000/1000 | 12213 | 116.77 |
| mixed | target 10k + mcv | note lt half | 50000 | Seq eav_data | no | 80000 | 50000 | eav ×2 | 252000/1000 | 14330 | 119.95 |
| mixed | target 10k + mcv | token gt 5 | 5 | IOS text_idx | yes | 50010 | 5 | eav ×1 | 200015/5 | 2715 | 28.41 |
| mixed | target 10k + mcv | token gte 5 | 5 | IOS text_idx | yes | 50010 | 5 | eav ×1 | 200015/5 | 2715 | 28.55 |
| mixed | target 10k + mcv | token lt 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 67 | 0.18 |
| mixed | target 10k + mcv | token lte 5 | 5 | IOS text_idx | yes | 3 | 5 |  | 25/5 | 67 | 0.19 |
| mixed | target 10k + mcv | token gt half | 50000 | Seq eav_data | no | 75000 | 50000 | eav ×2 | 252000/1000 | 14330 | 119.47 |
| mixed | target 10k + mcv | token lt half | 50000 | IOS text_idx | yes | 25000 | 50000 | eav ×1 | 252000/1000 | 12693 | 109.07 |
| mixed, pre-#728 | fresh | note gt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.17 |
| mixed, pre-#728 | fresh | note gte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.28 |
| mixed, pre-#728 | fresh | note lt 5 | 5 | IOS text_idx | yes | 50259 | 5 | eav ×1 | 200015/5 | 2715 | 26.41 |
| mixed, pre-#728 | fresh | note lte 5 | 5 | IOS text_idx | yes | 50260 | 5 | eav ×1 | 200015/5 | 2715 | 28.60 |
| mixed, pre-#728 | fresh | note gt half | 50000 | IOS text_idx | yes | 25130 | 50000 | eav ×1 | 252000/1000 | 11971 | 116.77 |
| mixed, pre-#728 | fresh | note lt half | 50000 | IOS text_idx | yes | 75389 | 50000 | eav ×1 | 252000/1000 | 11971 | 109.58 |
| mixed, pre-#728 | fresh | token gt 5 | 5 | IOS text_idx | yes | 50735 | 5 | eav ×1 | 200015/5 | 2715 | 27.87 |
| mixed, pre-#728 | fresh | token gte 5 | 5 | IOS text_idx | yes | 50735 | 5 | eav ×1 | 200015/5 | 2715 | 27.83 |
| mixed, pre-#728 | fresh | token lt 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.17 |
| mixed, pre-#728 | fresh | token lte 5 | 5 | IOS text_idx | yes | 10 | 5 |  | 25/5 | 67 | 0.16 |
| mixed, pre-#728 | fresh | token gt half | 50000 | IOS text_idx | yes | 74485 | 50000 | eav ×1 | 252000/1000 | 12191 | 133.21 |
| mixed, pre-#728 | fresh | token lt half | 50000 | IOS text_idx | yes | 24995 | 50000 | eav ×1 | 252000/1000 | 12191 | 133.64 |

</details>
