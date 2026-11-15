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

The full tables of the recorded run are in #815's pull request; the test
prints them on every run. Below, "control" is the one-attribute load, and
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
