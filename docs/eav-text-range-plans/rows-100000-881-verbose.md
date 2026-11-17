# Recorded run of the #815 benchmark after #881 under EXPLAIN VERBOSE, N = 100,000

`TestMixedEAVTextRangePlans` wrote everything below the line, unedited, with
`FORMA_EAV_PLAN_EVIDENCE` set. It is a second run of the
[#881 record](rows-100000-881.md), at the revision whose recorder writes the plan
trees under `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)`, which #881's investigation
asked for: each node also names its schema-qualified relation and alias and
its output columns, which is what tells the anchor's `eav_data t` from the
predicate's `x`, the `change_log` branch's `t_1` and the page's `e`.
[The anchor without its outer scan (#881)](../eav-text-range-plans.md#the-anchor-without-its-outer-scan-881)
reads the earlier record's figures; this one shows the same plan shapes.

- Revision: `675a9f50afab2f7513475942516d6b1521be3bab`, which changes only
  the recorder's `EXPLAIN` options on top of
  `4c151cb595e5367df998faba6c1ab29aad8aa74e` (#881's change and its record):
  the SQL generation is the one the #881 record measured.
- Command: `FORMA_EAV_PLAN_EVIDENCE=<dir> FORMA_EAV_PLAN_ROWS=20000,100000
  go test ./internal/e2e_harness/production/ -tags=e2e
  -run TestMixedEAVTextRangePlans -count=1 -timeout=20m`, Go toolchain
  go1.26.0.
- The result table is the JSON measurement, which VERBOSE does not touch; the
  plan sections are the VERBOSE text, the SQL as sent and its arguments.
- `ANALYZE` samples rows at random, so estimates differ from the #881
  record's by a few percent, and with them the length of the 20,000-row
  page's merge-join walk over `entity_main_pkey`. Times are indicative only
  (a shared machine); read estimates and buffers as the evidence.

---

## N = 100000

- Server: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Database collation: `en_US.utf8`; `eav_data.value_text`: `pg_catalog."C"`
- `e2e_wide` rows: 100000; `attr_id` note = 7, token = 17

Indexes:

```sql
CREATE UNIQUE INDEX eav_data_pkey ON public.eav_data USING btree (schema_id, row_id, attr_id, array_indices);
CREATE INDEX eav_data_text_idx ON public.eav_data USING btree (schema_id, attr_id, value_text, row_id) WHERE (value_text IS NOT NULL);
CREATE INDEX entity_main_cx_3160dfc8f228db96 ON public.entity_main USING btree (ltbase_schema_id, text_01, bigint_01, ltbase_row_id);
CREATE INDEX entity_main_cx_4e10412b55d4133b ON public.entity_main USING btree (ltbase_schema_id, text_02, bigint_03, ltbase_row_id);
CREATE INDEX entity_main_cx_6a11ab76e5b5dcf2 ON public.entity_main USING btree (ltbase_schema_id, smallint_01, text_01, ltbase_row_id);
CREATE INDEX entity_main_cx_9d2fd9305aa866a4 ON public.entity_main USING btree (ltbase_schema_id, text_01, bigint_02, ltbase_row_id);
CREATE UNIQUE INDEX entity_main_pkey ON public.entity_main USING btree (ltbase_schema_id, ltbase_row_id);
```

## Results

| eav_data | statistics | probe | matches | predicate access | seeks | est | actual | q-error | seq scans | scan output rows | rows filtered by scans | buffers | ms |
| --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: |
| note + token | missing | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.15 |
| note + token | missing | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 27 | 0.18 |
| note + token | missing | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.18 |
| note + token | missing | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.18 |
| note + token | missing | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.18 |
| note + token | missing | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.18 |
| note + token | missing | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 9675 | 63.00 |
| note + token | missing | note lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 9674 | 60.84 |
| note + token | missing | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.08 |
| note + token | missing | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 27 | 0.10 |
| note + token | missing | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | missing | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.17 |
| note + token | missing | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | missing | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | missing | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 10094 | 61.34 |
| note + token | missing | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 10095 | 82.83 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 116 | 0.22 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.20 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.33 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.33 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 302000, entity_main 1000 |  | 610346 | 525.49 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 302000, entity_main 1000 |  | 610344 | 526.79 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.32 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.20 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.20 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 113 | 0.20 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 302000, entity_main 1000 |  | 611184 | 523.99 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 302000, entity_main 1000 |  | 611186 | 531.60 |
| note + token | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.08 |
| note + token | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 27 | 0.10 |
| note + token | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1001 | 5 | 200 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1001 | 5 | 200 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50056 | 5 | 10011 |  | eav_data 15, entity_main 5 |  | 51 | 0.41 |
| note + token | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50057 | 5 | 10011 |  | eav_data 15, entity_main 5 |  | 51 | 0.36 |
| note + token | fresh | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 25028 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9307 | 56.61 |
| note + token | fresh | note lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 75084 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9307 | 53.63 |
| note + token | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.07 |
| note + token | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 27 | 0.09 |
| note + token | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50942 | 5 | 10188 |  | eav_data 15, entity_main 5 |  | 51 | 1.46 |
| note + token | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50943 | 5 | 10189 |  | eav_data 15, entity_main 5 |  | 51 | 0.41 |
| note + token | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.12 |
| note + token | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.12 |
| note + token | fresh | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 74915 | 50000 | 1 |  | eav_data 52000, entity_main 1000 |  | 9526 | 56.20 |
| note + token | fresh | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 24971 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9527 | 55.40 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1001 | 5 | 200 |  | eav_data 25, entity_main 5 |  | 67 | 0.17 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1001 | 5 | 200 |  | eav_data 25, entity_main 5 |  | 67 | 0.16 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50056 | 5 | 10011 | eav_data ×1 | eav_data 200015, entity_main 5 |  | 1923 | 26.52 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50057 | 5 | 10011 | eav_data ×1 | eav_data 200015, entity_main 5 |  | 1923 | 26.54 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 25028 | 50000 | 2 | eav_data ×1 | eav_data 252000, entity_main 1000 |  | 11179 | 103.30 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 75084 | 50000 | 2 | eav_data ×1 | eav_data 252000, entity_main 1000 |  | 11179 | 100.34 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50942 | 5 | 10188 | eav_data ×1 | eav_data 200015, entity_main 5 |  | 1923 | 29.23 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50943 | 5 | 10189 | eav_data ×1 | eav_data 200015, entity_main 5 |  | 1923 | 27.11 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 25, entity_main 5 |  | 67 | 0.20 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 25, entity_main 5 |  | 67 | 0.14 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 74915 | 50000 | 1 | eav_data ×1 | eav_data 252000, entity_main 1000 |  | 11398 | 103.59 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 24971 | 50000 | 2 | eav_data ×1 | eav_data 252000, entity_main 1000 |  | 11399 | 102.37 |
| note + token | fresh | rank=7, title gt half | 50 | Index Only Scan on entity_main_cx_6a11ab76e5b5dcf2 | yes | 49 | 50 | 1 |  | eav_data 100, entity_main 100 |  | 456 | 0.75 |
| note + token | fresh | rank=7, title lt half | 50 | Index Only Scan on entity_main_cx_6a11ab76e5b5dcf2 | yes | 51 | 50 | 1 |  | eav_data 100, entity_main 100 |  | 456 | 0.73 |
| note only | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 14 | 0.07 |
| note only | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 25 | 0.08 |
| note only | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| note only | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| note only | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| note only | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| note only | fresh | note gt half | 50000 | Seq Scan on eav_data | no | 49002 | 50000 | 1 | eav_data ×1 | eav_data 51000, entity_main 1000 | eav_data 50000 | 10601 | 56.77 |
| note only | fresh | note lt half | 50000 | Seq Scan on eav_data | no | 50997 | 50000 | 1 | eav_data ×1 | eav_data 51000, entity_main 1000 | eav_data 50000 | 10601 | 55.35 |
| note + token | stale (note only) | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 16 | 0.07 |
| note + token | stale (note only) | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 6, entity_main 2 |  | 29 | 0.09 |
| note + token | stale (note only) | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token | stale (note only) | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.12 |
| note + token | stale (note only) | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 56 | 0.12 |
| note + token | stale (note only) | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 56 | 0.12 |
| note + token | stale (note only) | note gt half | 50000 | Seq Scan on eav_data | no | 49002 | 50000 | 1 | eav_data ×1 | eav_data 52000, entity_main 1000 | eav_data 150000 | 11601 | 64.70 |
| note + token | stale (note only) | note lt half | 50000 | Seq Scan on eav_data | no | 50997 | 50000 | 1 | eav_data ×1 | eav_data 52000, entity_main 1000 | eav_data 150000 | 11601 | 65.17 |
| note + token | stale (note only) | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 16 | 0.07 |
| note + token | stale (note only) | token in | 2 | Index Only Scan on eav_data_text_idx | no | 1 | 2 | 2 |  | eav_data 6, entity_main 2 | eav_data 99998 | 2278 | 19.56 |
| note + token | stale (note only) | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 52 | 0.12 |
| note + token | stale (note only) | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 52 | 0.12 |
| note + token | stale (note only) | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 52 | 0.12 |
| note + token | stale (note only) | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 52 | 0.12 |
| note + token | stale (note only) | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 10095 | 61.62 |
| note + token | stale (note only) | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 1 | 50000 | 50000 |  | eav_data 52000, entity_main 1000 |  | 10172 | 61.95 |
| token only | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 14 | 0.07 |
| token only | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 25 | 0.09 |
| token only | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| token only | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| token only | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| token only | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 46 | 0.11 |
| token only | fresh | token gt half | 50000 | Seq Scan on eav_data | no | 50170 | 50000 | 1 | eav_data ×1 | eav_data 51000, entity_main 1000 | eav_data 50000 | 11002 | 57.21 |
| token only | fresh | token lt half | 50000 | Seq Scan on eav_data | no | 49829 | 50000 | 1 | eav_data ×1 | eav_data 51000, entity_main 1000 | eav_data 50000 | 11002 | 56.33 |
| note + token on 1 row in 20 | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 14 | 0.07 |
| note + token on 1 row in 20 | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 25 | 0.09 |
| note + token on 1 row in 20 | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 47 | 0.11 |
| note + token on 1 row in 20 | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 47 | 0.11 |
| note + token on 1 row in 20 | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 5007 | 5 | 1001 | eav_data ×1 | eav_data 105005, entity_main 5 |  | 819 | 30.92 |
| note + token on 1 row in 20 | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 5008 | 5 | 1002 | eav_data ×1 | eav_data 105005, entity_main 5 |  | 819 | 30.79 |
| note + token on 1 row in 20 | fresh | note gt half | 50000 | Seq Scan on eav_data | no | 47073 | 50000 | 1 | eav_data ×2 | eav_data 155000, entity_main 1000 | eav_data 55000 | 5588 | 85.64 |
| note + token on 1 row in 20 | fresh | note lt half | 50000 | Seq Scan on eav_data | no | 53078 | 50000 | 1 | eav_data ×2 | eav_data 155000, entity_main 1000 | eav_data 55000 | 5588 | 85.62 |
| note + token on 1 row in 20 | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.12 |
| note + token on 1 row in 20 | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 27 | 0.09 |
| note + token on 1 row in 20 | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 4654 | 5 | 931 | eav_data ×1 | eav_data 105005, entity_main 5 |  | 821 | 30.89 |
| note + token on 1 row in 20 | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 4654 | 5 | 931 | eav_data ×1 | eav_data 105005, entity_main 5 |  | 821 | 30.18 |
| note + token on 1 row in 20 | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 53 | 0.12 |
| note + token on 1 row in 20 | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 52 | 0.17 |
| note + token on 1 row in 20 | fresh | token gt half | 2500 | Index Only Scan on eav_data_text_idx | yes | 4732 | 2500 | 2 | eav_data ×1 | eav_data 107500, entity_main 1000 |  | 5841 | 41.57 |
| note + token on 1 row in 20 | fresh | token lt half | 2500 | Index Only Scan on eav_data_text_idx | yes | 116 | 2500 | 22 |  | eav_data 4500, entity_main 1000 |  | 10029 | 12.98 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.17 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.16 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50012 | 5 | 10002 |  | eav_data 15, entity_main 5 |  | 51 | 0.54 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50012 | 5 | 10002 |  | eav_data 15, entity_main 5 |  | 51 | 0.41 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 25000 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9549 | 55.37 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lt half | 50000 | Seq Scan on eav_data | no | 80000 | 50000 | 2 | eav_data ×1 | eav_data 52000, entity_main 1000 | eav_data 150000 | 11666 | 63.00 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50010 | 5 | 10002 |  | eav_data 15, entity_main 5 |  | 51 | 0.41 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50010 | 5 | 10002 |  | eav_data 15, entity_main 5 |  | 51 | 0.41 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.14 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.14 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gt half | 50000 | Seq Scan on eav_data | no | 75000 | 50000 | 2 | eav_data ×1 | eav_data 52000, entity_main 1000 | eav_data 150000 | 11666 | 62.45 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 25000 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 10029 | 51.77 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.15 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50393 | 5 | 10079 |  | eav_data 15, entity_main 5 |  | 51 | 0.38 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50393 | 5 | 10079 |  | eav_data 15, entity_main 5 |  | 51 | 0.38 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 25201 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9307 | 56.08 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 75585 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9307 | 55.08 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50599 | 5 | 10120 |  | eav_data 15, entity_main 5 |  | 51 | 0.38 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 50599 | 5 | 10120 |  | eav_data 15, entity_main 5 |  | 51 | 0.39 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.14 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 74410 | 50000 | 1 |  | eav_data 52000, entity_main 1000 |  | 9527 | 76.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lt half | 50000 | Index Only Scan on eav_data_text_idx | yes | 24803 | 50000 | 2 |  | eav_data 52000, entity_main 1000 |  | 9527 | 76.22 |

## Plans

### `token gt half`: note + token, statistics missing

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000000c350"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=74.55..74.79 rows=2 width=608) (actual time=57.236..62.434 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=10094
  CTE ordered
    ->  Limit  (cost=25.20..25.20 rows=2 width=24) (actual time=51.240..51.330 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=1094
          ->  Sort  (cost=25.20..25.20 rows=2 width=24) (actual time=51.238..51.277 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 119kB
                Buffers: shared hit=1094
                ->  WindowAgg  (cost=25.15..25.19 rows=2 width=24) (actual time=42.922..47.115 rows=50000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=1094
                      ->  Unique  (cost=25.15..25.16 rows=2 width=16) (actual time=30.353..37.784 rows=50000 loops=1)
                            Output: t.row_id
                            Buffers: shared hit=1094
                            ->  Sort  (cost=25.15..25.16 rows=2 width=16) (actual time=30.352..32.110 rows=50000 loops=1)
                                  Output: t.row_id
                                  Sort Key: t.row_id
                                  Sort Method: quicksort  Memory: 2708kB
                                  Buffers: shared hit=1094
                                  ->  Append  (cost=8.45..25.14 rows=2 width=16) (actual time=17.482..26.824 rows=50000 loops=1)
                                        Buffers: shared hit=1094
                                        ->  Unique  (cost=8.45..8.46 rows=1 width=16) (actual time=17.481..24.693 rows=50000 loops=1)
                                              Output: t.row_id
                                              Buffers: shared hit=1092
                                              ->  Sort  (cost=8.45..8.46 rows=1 width=16) (actual time=17.480..19.233 rows=50000 loops=1)
                                                    Output: t.row_id
                                                    Sort Key: t.row_id
                                                    Sort Method: quicksort  Memory: 2708kB
                                                    Buffers: shared hit=1092
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..8.44 rows=1 width=16) (actual time=0.011..9.885 rows=50000 loops=1)
                                                          Output: t.row_id
                                                          Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000000c350'::text))
                                                          Heap Fetches: 50000
                                                          Buffers: shared hit=1092
                                        ->  Nested Loop Semi Join  (cost=0.57..16.68 rows=1 width=16) (actual time=0.004..0.004 rows=0 loops=1)
                                              Output: cl.row_id
                                              Join Filter: (cl.row_id = t_1.row_id)
                                              Buffers: shared hit=2
                                              ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.004..0.004 rows=0 loops=1)
                                                    Output: cl.schema_id, cl.row_id, cl.flushed_at
                                                    Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=2
                                              ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                                    Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint) AND (t_1.value_text > '00000000-0000-4000-8000-00000000c350'::text))
                                                    Heap Fetches: 0
  ->  Sort  (cost=16.93..16.93 rows=2 width=540) (actual time=53.310..53.343 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 118kB
        Buffers: shared hit=5094
        ->  Nested Loop  (cost=0.42..16.92 rows=2 width=540) (actual time=51.250..53.035 rows=1000 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=5094
              ->  CTE Scan on ordered o  (cost=0.00..0.04 rows=2 width=24) (actual time=51.240..51.451 rows=1000 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=1094
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..8.44 rows=1 width=532) (actual time=0.001..0.001 rows=1 loops=1000)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=4000
  ->  GroupAggregate  (cost=32.42..32.56 rows=5 width=48) (actual time=3.922..8.545 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=5000
        ->  Sort  (cost=32.42..32.44 rows=5 width=116) (actual time=3.907..3.973 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=5000
              ->  Nested Loop  (cost=4.45..32.37 rows=5 width=116) (actual time=0.014..3.589 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=5000
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.04 rows=2 width=16) (actual time=0.000..0.063 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Bitmap Heap Scan on public.eav_data e  (cost=4.45..16.13 rows=3 width=116) (actual time=0.002..0.003 rows=2 loops=1000)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Recheck Cond: ((e.schema_id = '21'::smallint) AND (o_1.row_id = e.row_id))
                          Heap Blocks: exact=2000
                          Buffers: shared hit=5000
                          ->  Bitmap Index Scan on eav_data_pkey  (cost=0.00..4.45 rows=3 width=0) (actual time=0.001..0.001 rows=2 loops=1000)
                                Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                                Buffers: shared hit=3000
Planning:
  Buffers: shared hit=1
Planning Time: 0.443 ms
Execution Time: 62.510 ms
```

### `token gt half`: note + token, pre-#881 anchor with the outer eav_data scan, statistics missing

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000000c350"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (EXISTS (SELECT 1 FROM "eav_data" x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 AND x.value_text COLLATE "C" > $3))
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (EXISTS (SELECT 1 FROM "eav_data" x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 AND x.value_text COLLATE "C" > $3))
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=149.52..149.98 rows=4 width=608) (actual time=517.204..522.165 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=611184, temp read=245 written=246
  CTE ordered
    ->  Limit  (cost=50.74..50.75 rows=4 width=24) (actual time=511.482..511.570 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=602184, temp read=245 written=246
          ->  Sort  (cost=50.74..50.75 rows=4 width=24) (actual time=511.481..511.525 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 167kB
                Buffers: shared hit=602184, temp read=245 written=246
                ->  WindowAgg  (cost=50.61..50.70 rows=4 width=24) (actual time=502.712..506.707 rows=50000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=602184, temp read=245 written=246
                      ->  HashAggregate  (cost=50.61..50.65 rows=4 width=16) (actual time=492.509..496.266 rows=50000 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 3625kB
                            Buffers: shared hit=602184, temp read=245 written=246
                            ->  Append  (cost=24.64..50.60 rows=4 width=16) (actual time=223.073..483.779 rows=50000 loops=1)
                                  Buffers: shared hit=602184, temp read=245 written=246
                                  ->  Unique  (cost=24.64..24.66 rows=3 width=16) (actual time=223.073..235.626 rows=50000 loops=1)
                                        Output: t.row_id
                                        Buffers: shared hit=251092, temp read=245 written=246
                                        ->  Sort  (cost=24.64..24.65 rows=3 width=16) (actual time=223.071..228.874 rows=100000 loops=1)
                                              Output: t.row_id
                                              Sort Key: t.row_id
                                              Sort Method: external merge  Disk: 1960kB
                                              Buffers: shared hit=251092, temp read=245 written=246
                                              ->  Nested Loop  (cost=12.89..24.62 rows=3 width=16) (actual time=19.998..202.471 rows=100000 loops=1)
                                                    Output: t.row_id
                                                    Buffers: shared hit=251092
                                                    ->  HashAggregate  (cost=8.45..8.46 rows=1 width=18) (actual time=19.987..28.132 rows=50000 loops=1)
                                                          Output: x.schema_id, x.row_id
                                                          Group Key: x.schema_id, x.row_id
                                                          Batches: 1  Memory Usage: 5161kB
                                                          Buffers: shared hit=1092
                                                          ->  Index Only Scan using eav_data_text_idx on public.eav_data x  (cost=0.42..8.44 rows=1 width=18) (actual time=0.014..9.840 rows=50000 loops=1)
                                                                Output: x.schema_id, x.attr_id, x.value_text, x.row_id
                                                                Index Cond: ((x.schema_id = '21'::smallint) AND (x.attr_id = '17'::smallint) AND (x.value_text > '00000000-0000-4000-8000-00000000c350'::text))
                                                                Heap Fetches: 50000
                                                                Buffers: shared hit=1092
                                                    ->  Bitmap Heap Scan on public.eav_data t  (cost=4.45..16.13 rows=3 width=18) (actual time=0.002..0.003 rows=2 loops=50000)
                                                          Output: t.schema_id, t.row_id, t.attr_id, t.array_indices, t.value_text, t.value_numeric
                                                          Recheck Cond: ((t.schema_id = '21'::smallint) AND (t.row_id = x.row_id))
                                                          Heap Blocks: exact=100000
                                                          Buffers: shared hit=250000
                                                          ->  Bitmap Index Scan on eav_data_pkey  (cost=0.00..4.45 rows=3 width=0) (actual time=0.002..0.002 rows=2 loops=50000)
                                                                Index Cond: ((t.schema_id = '21'::smallint) AND (t.row_id = x.row_id))
                                                                Buffers: shared hit=150000
                                  ->  Nested Loop  (cost=24.78..25.93 rows=1 width=16) (actual time=246.134..246.135 rows=0 loops=1)
                                        Output: cl.row_id
                                        Inner Unique: true
                                        Join Filter: (cl.row_id = x_1.row_id)
                                        Buffers: shared hit=351092
                                        ->  HashAggregate  (cost=24.63..24.66 rows=3 width=32) (actual time=219.680..224.206 rows=50000 loops=1)
                                              Output: t_1.row_id, x_1.row_id
                                              Group Key: t_1.row_id
                                              Batches: 1  Memory Usage: 5161kB
                                              Buffers: shared hit=251092
                                              ->  Nested Loop  (cost=12.89..24.62 rows=3 width=32) (actual time=18.031..201.296 rows=100000 loops=1)
                                                    Output: t_1.row_id, x_1.row_id
                                                    Buffers: shared hit=251092
                                                    ->  HashAggregate  (cost=8.45..8.46 rows=1 width=18) (actual time=18.019..25.732 rows=50000 loops=1)
                                                          Output: x_1.schema_id, x_1.row_id
                                                          Group Key: x_1.schema_id, x_1.row_id
                                                          Batches: 1  Memory Usage: 5161kB
                                                          Buffers: shared hit=1092
                                                          ->  Index Only Scan using eav_data_text_idx on public.eav_data x_1  (cost=0.42..8.44 rows=1 width=18) (actual time=0.013..9.018 rows=50000 loops=1)
                                                                Output: x_1.schema_id, x_1.attr_id, x_1.value_text, x_1.row_id
                                                                Index Cond: ((x_1.schema_id = '21'::smallint) AND (x_1.attr_id = '17'::smallint) AND (x_1.value_text > '00000000-0000-4000-8000-00000000c350'::text))
                                                                Heap Fetches: 50000
                                                                Buffers: shared hit=1092
                                                    ->  Bitmap Heap Scan on public.eav_data t_1  (cost=4.45..16.13 rows=3 width=18) (actual time=0.002..0.003 rows=2 loops=50000)
                                                          Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                                          Recheck Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = x_1.row_id))
                                                          Heap Blocks: exact=100000
                                                          Buffers: shared hit=250000
                                                          ->  Bitmap Index Scan on eav_data_pkey  (cost=0.00..4.45 rows=3 width=0) (actual time=0.002..0.002 rows=2 loops=50000)
                                                                Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = x_1.row_id))
                                                                Buffers: shared hit=150000
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..0.41 rows=1 width=16) (actual time=0.000..0.000 rows=0 loops=50000)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.row_id = t_1.row_id) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=100000
  ->  Sort  (cost=33.87..33.88 rows=4 width=540) (actual time=513.463..513.497 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 118kB
        Buffers: shared hit=606184, temp read=245 written=246
        ->  Nested Loop  (cost=0.42..33.83 rows=4 width=540) (actual time=511.494..513.209 rows=1000 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=606184, temp read=245 written=246
              ->  CTE Scan on ordered o  (cost=0.00..0.08 rows=4 width=24) (actual time=511.483..511.685 rows=1000 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=602184, temp read=245 written=246
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..8.44 rows=1 width=532) (actual time=0.001..0.001 rows=1 loops=1000)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=4000
  ->  GroupAggregate  (cost=64.90..65.17 rows=10 width=48) (actual time=3.735..8.150 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=5000
        ->  Sort  (cost=64.90..64.92 rows=10 width=116) (actual time=3.721..3.786 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=5000
              ->  Nested Loop  (cost=4.45..64.73 rows=10 width=116) (actual time=0.010..3.409 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=5000
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.08 rows=4 width=16) (actual time=0.000..0.066 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Bitmap Heap Scan on public.eav_data e  (cost=4.45..16.13 rows=3 width=116) (actual time=0.002..0.002 rows=2 loops=1000)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Recheck Cond: ((e.schema_id = '21'::smallint) AND (o_1.row_id = e.row_id))
                          Heap Blocks: exact=2000
                          Buffers: shared hit=5000
                          ->  Bitmap Index Scan on eav_data_pkey  (cost=0.00..4.45 rows=3 width=0) (actual time=0.001..0.001 rows=2 loops=1000)
                                Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                                Buffers: shared hit=3000
Planning:
  Buffers: shared hit=1
Planning Time: 0.523 ms
Execution Time: 522.398 ms
```

### `note lt 5`: note + token, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `7`
- `$3` = `"t000006"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" < $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" < $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=20041.78..20134.68 rows=1000 width=608) (actual time=0.249..0.274 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=51
  CTE ordered
    ->  Limit  (cost=7090.49..7092.99 rows=1000 width=24) (actual time=0.201..0.202 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=7090.49..7199.89 rows=43763 width=24) (actual time=0.200..0.201 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=3706.34..4691.01 rows=43763 width=24) (actual time=0.198..0.199 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=3706.34..4143.97 rows=43763 width=16) (actual time=0.121..0.195 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 1561kB
                            Buffers: shared hit=6
                            ->  Append  (cost=2915.82..3596.93 rows=43763 width=16) (actual time=0.025..0.102 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=2915.82..3353.44 rows=43762 width=16) (actual time=0.025..0.100 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 1561kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..2790.68 rows=50056 width=16) (actual time=0.005..0.005 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '7'::smallint) AND (t.value_text < 't000006'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.68 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '7'::smallint))
                                              Filter: (t_1.value_text < 't000006'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=0.220..0.220 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=26
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=0.206..0.217 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=26
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.201..0.202 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=6
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=7525.47..7580.61 rows=2005 width=48) (actual time=0.027..0.047 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=25
        ->  Sort  (cost=7525.47..7530.48 rows=2005 width=75) (actual time=0.018..0.018 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=25
              ->  Nested Loop  (cost=0.42..7415.50 rows=2005 width=75) (actual time=0.003..0.015 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=25
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.38 rows=2 width=75) (actual time=0.002..0.002 rows=2 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=25
Planning:
  Buffers: shared hit=1
Planning Time: 0.375 ms
Execution Time: 0.373 ms
```

### `token gt 5`: note + token, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001869b"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=20154.54..20247.44 rows=1000 width=608) (actual time=0.255..0.279 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=51
  CTE ordered
    ->  Limit  (cost=7203.25..7205.75 rows=1000 width=24) (actual time=0.197..0.199 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=7203.25..7314.31 rows=44424 width=24) (actual time=0.197..0.198 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=3767.99..4767.53 rows=44424 width=24) (actual time=0.194..0.196 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=3767.99..4212.23 rows=44424 width=16) (actual time=0.110..0.192 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 1561kB
                            Buffers: shared hit=6
                            ->  Append  (cost=2965.97..3656.93 rows=44424 width=16) (actual time=0.016..0.100 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=2965.97..3410.20 rows=44423 width=16) (actual time=0.016..0.098 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 1561kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..2838.62 rows=50942 width=16) (actual time=0.005..0.005 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.61 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-00000001869b'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=0.215..0.215 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=26
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=0.203..0.212 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=26
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.198..0.199 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=6
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=7525.47..7580.61 rows=2005 width=48) (actual time=0.037..0.056 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=25
        ->  Sort  (cost=7525.47..7530.48 rows=2005 width=75) (actual time=0.028..0.029 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=25
              ->  Nested Loop  (cost=0.42..7415.50 rows=2005 width=75) (actual time=0.006..0.016 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=25
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.38 rows=2 width=75) (actual time=0.002..0.003 rows=2 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=25
Planning:
  Buffers: shared hit=1
Planning Time: 0.404 ms
Execution Time: 0.370 ms
```

### `token gt half`: note + token, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000000c350"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=23103.25..23196.15 rows=1000 width=608) (actual time=51.699..56.979 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=9526
  CTE ordered
    ->  Limit  (cost=10151.96..10154.46 rows=1000 width=24) (actual time=47.059..47.152 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=526
          ->  Sort  (cost=10151.96..10304.01 rows=60821 width=24) (actual time=47.058..47.103 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 167kB
                Buffers: shared hit=526
                ->  WindowAgg  (cost=5448.73..6817.21 rows=60821 width=24) (actual time=37.571..41.925 rows=50000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=526
                      ->  HashAggregate  (cost=5448.73..6056.94 rows=60821 width=16) (actual time=27.192..30.648 rows=50000 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 3601kB
                            Buffers: shared hit=526
                            ->  Append  (cost=4361.30..5296.68 rows=60821 width=16) (actual time=14.601..20.141 rows=50000 loops=1)
                                  Buffers: shared hit=526
                                  ->  HashAggregate  (cost=4361.30..4969.49 rows=60820 width=16) (actual time=14.601..17.947 rows=50000 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 3601kB
                                        Buffers: shared hit=524
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..4174.01 rows=74915 width=16) (actual time=0.016..6.303 rows=50000 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000000c350'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=524
                                  ->  Nested Loop Semi Join  (cost=0.57..23.08 rows=1 width=16) (actual time=0.008..0.008 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.007..0.008 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-00000000c350'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=49.193..49.229 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 118kB
        Buffers: shared hit=4526
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=47.071..48.921 rows=1000 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=4526
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=47.060..47.276 rows=1000 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=526
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.001..0.001 rows=1 loops=1000)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=4000
  ->  GroupAggregate  (cost=7525.47..7580.61 rows=2005 width=48) (actual time=2.501..7.197 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=5000
        ->  Sort  (cost=7525.47..7530.48 rows=2005 width=75) (actual time=2.486..2.557 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=5000
              ->  Nested Loop  (cost=0.42..7415.50 rows=2005 width=75) (actual time=0.006..2.152 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=5000
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.060 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.38 rows=2 width=75) (actual time=0.001..0.002 rows=2 loops=1000)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=5000
Planning:
  Buffers: shared hit=1
Planning Time: 0.756 ms
Execution Time: 57.175 ms
```

### `token gt 5`: note + token, pre-#881 anchor with the outer eav_data scan, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001869b"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (EXISTS (SELECT 1 FROM "eav_data" x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 AND x.value_text COLLATE "C" > $3))
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (EXISTS (SELECT 1 FROM "eav_data" x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 AND x.value_text COLLATE "C" > $3))
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=32314.90..32407.80 rows=1000 width=608) (actual time=27.351..27.402 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=1923
  CTE ordered
    ->  Limit  (cost=19363.61..19366.11 rows=1000 width=24) (actual time=27.221..27.248 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=1878
          ->  Sort  (cost=19363.61..19612.95 rows=99737 width=24) (actual time=27.220..27.247 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=1878
                ->  WindowAgg  (cost=11651.05..13895.13 rows=99737 width=24) (actual time=27.212..27.241 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=1878
                      ->  HashAggregate  (cost=11651.05..12648.42 rows=99737 width=16) (actual time=26.992..27.228 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 1561kB
                            Buffers: shared hit=1878
                            ->  Append  (cost=9891.93..11401.71 rows=99737 width=16) (actual time=26.739..26.997 rows=5 loops=1)
                                  Buffers: shared hit=1878
                                  ->  HashAggregate  (cost=9891.93..10889.29 rows=99736 width=16) (actual time=26.738..26.984 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 1561kB
                                        Buffers: shared hit=1876
                                        ->  Hash Semi Join  (cost=3475.39..9636.55 rows=102154 width=16) (actual time=13.278..26.734 rows=10 loops=1)
                                              Output: t.row_id
                                              Hash Cond: (t.row_id = x.row_id)
                                              Buffers: shared hit=1876
                                              ->  Seq Scan on public.eav_data t  (cost=0.00..4372.00 rows=200000 width=18) (actual time=0.004..13.144 rows=200000 loops=1)
                                                    Output: t.schema_id, t.row_id, t.attr_id, t.array_indices, t.value_text, t.value_numeric
                                                    Filter: (t.schema_id = '21'::smallint)
                                                    Buffers: shared hit=1872
                                              ->  Hash  (cost=2838.62..2838.62 rows=50942 width=18) (actual time=0.014..0.015 rows=5 loops=1)
                                                    Output: x.schema_id, x.row_id
                                                    Buckets: 65536  Batches: 1  Memory Usage: 513kB
                                                    Buffers: shared hit=4
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data x  (cost=0.42..2838.62 rows=50942 width=18) (actual time=0.010..0.011 rows=5 loops=1)
                                                          Output: x.schema_id, x.row_id
                                                          Index Cond: ((x.schema_id = '21'::smallint) AND (x.attr_id = '17'::smallint) AND (x.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                                          Heap Fetches: 0
                                                          Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.99..13.73 rows=1 width=16) (actual time=0.011..0.012 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.011..0.011 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Nested Loop Semi Join  (cost=0.84..5.50 rows=1 width=32) (never executed)
                                              Output: t_1.row_id, x_1.row_id
                                              Join Filter: (cl.row_id = x_1.row_id)
                                              ->  Index Only Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..4.46 rows=2 width=18) (never executed)
                                                    Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id))
                                                    Heap Fetches: 0
                                              ->  Index Scan using eav_data_pkey on public.eav_data x_1  (cost=0.42..0.51 rows=1 width=18) (never executed)
                                                    Output: x_1.schema_id, x_1.row_id, x_1.attr_id, x_1.array_indices, x_1.value_text, x_1.value_numeric
                                                    Index Cond: ((x_1.schema_id = '21'::smallint) AND (x_1.row_id = t_1.row_id) AND (x_1.attr_id = '17'::smallint))
                                                    Filter: (x_1.value_text > '00000000-0000-4000-8000-00000001869b'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=27.262..27.263 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=1898
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=27.239..27.255 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=1898
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=27.221..27.223 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=1878
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.005..0.005 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=7525.47..7580.61 rows=2005 width=48) (actual time=0.084..0.104 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=25
        ->  Sort  (cost=7525.47..7530.48 rows=2005 width=75) (actual time=0.058..0.058 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=25
              ->  Nested Loop  (cost=0.42..7415.50 rows=2005 width=75) (actual time=0.034..0.049 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=25
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.38 rows=2 width=75) (actual time=0.004..0.004 rows=2 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=25
Planning:
  Buffers: shared hit=1
Planning Time: 0.529 ms
Execution Time: 27.594 ms
```

### `token in`: note + token, statistics stale (note only)

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `[]string{"00000000-0000-4000-8000-00000000002b", "00000000-0000-4000-8000-00000000002c"}`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text = ANY($3))
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text = ANY($3))
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=51.07..51.19 rows=2 width=608) (actual time=20.259..20.268 rows=2 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=2278
  CTE ordered
    ->  Limit  (cost=17.20..17.20 rows=2 width=24) (actual time=20.225..20.226 rows=2 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=2260
          ->  Sort  (cost=17.20..17.20 rows=2 width=24) (actual time=20.224..20.226 rows=2 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=2260
                ->  WindowAgg  (cost=17.15..17.19 rows=2 width=24) (actual time=20.223..20.224 rows=2 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=2260
                      ->  Unique  (cost=17.15..17.16 rows=2 width=16) (actual time=20.220..20.221 rows=2 loops=1)
                            Output: t.row_id
                            Buffers: shared hit=2260
                            ->  Sort  (cost=17.15..17.16 rows=2 width=16) (actual time=20.220..20.221 rows=2 loops=1)
                                  Output: t.row_id
                                  Sort Key: t.row_id
                                  Sort Method: quicksort  Memory: 25kB
                                  Buffers: shared hit=2260
                                  ->  Append  (cost=4.45..17.14 rows=2 width=16) (actual time=20.215..20.219 rows=2 loops=1)
                                        Buffers: shared hit=2260
                                        ->  Unique  (cost=4.45..4.46 rows=1 width=16) (actual time=20.215..20.216 rows=2 loops=1)
                                              Output: t.row_id
                                              Buffers: shared hit=2258
                                              ->  Sort  (cost=4.45..4.45 rows=1 width=16) (actual time=20.214..20.215 rows=2 loops=1)
                                                    Output: t.row_id
                                                    Sort Key: t.row_id
                                                    Sort Method: quicksort  Memory: 25kB
                                                    Buffers: shared hit=2258
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..4.44 rows=1 width=16) (actual time=0.014..20.213 rows=2 loops=1)
                                                          Output: t.row_id
                                                          Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint))
                                                          Filter: (t.value_text = ANY ('{00000000-0000-4000-8000-00000000002b,00000000-0000-4000-8000-00000000002c}'::text[]))
                                                          Rows Removed by Filter: 99998
                                                          Heap Fetches: 100000
                                                          Buffers: shared hit=2258
                                        ->  Nested Loop Semi Join  (cost=0.57..12.67 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                              Output: cl.row_id
                                              Join Filter: (cl.row_id = t_1.row_id)
                                              Buffers: shared hit=2
                                              ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                                    Output: cl.schema_id, cl.row_id, cl.flushed_at
                                                    Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=2
                                              ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.42..4.44 rows=1 width=16) (never executed)
                                                    Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint))
                                                    Filter: (t_1.value_text = ANY ('{00000000-0000-4000-8000-00000000002b,00000000-0000-4000-8000-00000000002c}'::text[]))
                                                    Heap Fetches: 0
  ->  Sort  (cost=16.93..16.93 rows=2 width=540) (actual time=20.236..20.236 rows=2 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=2268
        ->  Nested Loop  (cost=0.42..16.92 rows=2 width=540) (actual time=20.230..20.234 rows=2 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=2268
              ->  CTE Scan on ordered o  (cost=0.00..0.04 rows=2 width=24) (actual time=20.225..20.226 rows=2 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=2260
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..8.44 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=2)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=8
  ->  GroupAggregate  (cost=16.95..17.00 rows=2 width=48) (actual time=0.020..0.026 rows=2 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=10
        ->  Sort  (cost=16.95..16.95 rows=2 width=61) (actual time=0.010..0.011 rows=4 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=10
              ->  Nested Loop  (cost=0.42..16.93 rows=2 width=61) (actual time=0.004..0.008 rows=4 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=10
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.04 rows=2 width=16) (actual time=0.000..0.001 rows=2 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..8.44 rows=1 width=61) (actual time=0.002..0.003 rows=2 loops=2)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=10
Planning:
  Buffers: shared hit=1
Planning Time: 0.375 ms
Execution Time: 20.303 ms
```

### `token gt 5`: token only, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001869b"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=204.86..205.45 rows=11 width=608) (actual time=0.059..0.076 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=46
  CTE ordered
    ->  Limit  (cost=18.28..18.31 rows=11 width=24) (actual time=0.017..0.018 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=18.28..18.31 rows=11 width=24) (actual time=0.017..0.018 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=17.84..18.09 rows=11 width=24) (actual time=0.014..0.016 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=17.84..17.95 rows=11 width=16) (actual time=0.011..0.012 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 24kB
                            Buffers: shared hit=6
                            ->  Append  (cost=4.67..17.81 rows=11 width=16) (actual time=0.008..0.011 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=4.67..4.77 rows=10 width=16) (actual time=0.008..0.009 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 24kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..4.64 rows=10 width=16) (actual time=0.005..0.006 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..12.99 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Join Filter: (cl.row_id = t_1.row_id)
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.42..4.64 rows=10 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint) AND (t_1.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                              Heap Fetches: 0
  ->  Sort  (cost=93.22..93.25 rows=11 width=540) (actual time=0.035..0.036 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=26
        ->  Nested Loop  (cost=0.42..93.03 rows=11 width=540) (actual time=0.022..0.032 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=26
              ->  CTE Scan on ordered o  (cost=0.00..0.22 rows=11 width=24) (actual time=0.017..0.019 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=6
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..8.44 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=93.33..93.64 rows=11 width=48) (actual time=0.021..0.033 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=20
        ->  Sort  (cost=93.33..93.36 rows=11 width=90) (actual time=0.014..0.015 rows=5 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=20
              ->  Nested Loop  (cost=0.42..93.14 rows=11 width=90) (actual time=0.003..0.012 rows=5 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=20
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.22 rows=11 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..8.44 rows=1 width=90) (actual time=0.002..0.002 rows=1 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=20
Planning:
  Buffers: shared hit=1
Planning Time: 0.386 ms
Execution Time: 0.115 ms
```

### `token gt 5`: note + token on 1 row in 20, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001863c"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=12560.44..12615.97 rows=1000 width=608) (actual time=30.558..30.585 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=821, temp read=325 written=325
  CTE ordered
    ->  Limit  (cost=1362.12..1364.62 rows=1000 width=24) (actual time=0.045..0.048 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=8
          ->  Sort  (cost=1362.12..1373.73 rows=4647 width=24) (actual time=0.044..0.046 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=8
                ->  WindowAgg  (cost=1002.77..1107.33 rows=4647 width=24) (actual time=0.042..0.044 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=8
                      ->  HashAggregate  (cost=1002.77..1049.24 rows=4647 width=16) (actual time=0.028..0.040 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 217kB
                            Buffers: shared hit=8
                            ->  Append  (cost=896.77..991.15 rows=4647 width=16) (actual time=0.013..0.027 rows=5 loops=1)
                                  Buffers: shared hit=8
                                  ->  HashAggregate  (cost=896.77..943.23 rows=4646 width=16) (actual time=0.013..0.024 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 217kB
                                        Buffers: shared hit=6
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..885.13 rows=4654 width=16) (actual time=0.008..0.010 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000001863c'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=6
                                  ->  Nested Loop Semi Join  (cost=0.57..24.69 rows=1 width=16) (actual time=0.001..0.002 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-00000001863c'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=0.070..0.070 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=28
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=0.052..0.066 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=28
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.045..0.047 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=8
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.003..0.003 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=5772.49..5802.00 rows=1073 width=48) (actual time=30.484..30.504 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=793, temp read=325 written=325
        ->  Sort  (cost=5772.49..5775.17 rows=1073 width=62) (actual time=30.469..30.470 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=793, temp read=325 written=325
              ->  Hash Join  (cost=4546.00..5718.48 rows=1073 width=62) (actual time=27.785..30.464 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=793, temp read=325 written=325
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id
                    ->  Hash  (cost=2105.50..2105.50 rows=105000 width=62) (actual time=27.761..27.761 rows=105000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 131072  Batches: 2  Memory Usage: 4215kB
                          Buffers: shared hit=793, temp written=323
                          ->  Seq Scan on public.eav_data e  (cost=0.00..2105.50 rows=105000 width=62) (actual time=0.003..10.702 rows=105000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=793
Planning:
  Buffers: shared hit=1
Planning Time: 0.435 ms
Execution Time: 30.647 ms
```

### `token gt 5`: note + token, statistics fresh, target 10000 + mcv(attr_id, value_text)

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001869b"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text COLLATE "C" > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=23153.11..23245.81 rows=1000 width=608) (actual time=0.268..0.293 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=51
  CTE ordered
    ->  Limit  (cost=9820.88..9823.38 rows=1000 width=24) (actual time=0.207..0.209 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=9820.88..9930.27 rows=43758 width=24) (actual time=0.207..0.208 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=6437.12..7421.67 rows=43758 width=24) (actual time=0.203..0.205 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=6437.12..6874.70 rows=43758 width=16) (actual time=0.117..0.201 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 1561kB
                            Buffers: shared hit=6
                            ->  Append  (cost=5646.67..6327.72 rows=43758 width=16) (actual time=0.020..0.107 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=5646.67..6084.24 rows=43757 width=16) (actual time=0.020..0.104 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 1561kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..5521.65 rows=50010 width=16) (actual time=0.008..0.009 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.69 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-00000001869b'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=0.232..0.232 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=26
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=0.216..0.228 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=26
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.208..0.209 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=6
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.003..0.003 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=7906.41..7961.41 rows=2000 width=48) (actual time=0.033..0.053 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=25
        ->  Sort  (cost=7906.41..7911.41 rows=2000 width=75) (actual time=0.023..0.023 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=25
              ->  Nested Loop  (cost=0.42..7796.75 rows=2000 width=75) (actual time=0.005..0.019 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=25
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.76 rows=2 width=75) (actual time=0.003..0.003 rows=2 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=25
Planning:
  Buffers: shared hit=1
Planning Time: 4.102 ms
Execution Time: 0.420 ms
```

### `token gt 5`: note + token, pre-#728 SQL on a default-collation column, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-00000001869b"`
- `$4` = `1000`
- `$5` = `0`

```sql
WITH anchor AS (
	            SELECT DISTINCT t.row_id
	            FROM "eav_data" t
	            WHERE t.schema_id = $1 AND (t.attr_id = $2 AND t.value_text > $3)
	            UNION
	            -- Include real-time buffer rows from change_log (flushed_at = 0)
	            -- while preserving the same anchor filter semantics.
	            SELECT cl.row_id
	            FROM "change_log" cl
	            WHERE cl.schema_id = $1
	                AND cl.flushed_at = 0
	                AND EXISTS (
	                    SELECT 1
	                    FROM "eav_data" t
	                    WHERE t.schema_id = $1
	                        AND t.row_id = cl.row_id
	                        AND (t.attr_id = $2 AND t.value_text > $3)
	                )
	        ),
        keys AS (
            SELECT
                a.row_id,
                COUNT(*) OVER() AS total
            FROM anchor a
        ),
        ordered AS (
            SELECT
                row_id,
                total
            FROM keys
            ORDER BY
                row_id
            LIMIT $4 OFFSET $5
        ),
        main_data AS (
            SELECT
                ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
                o.total
            FROM ordered o
            INNER JOIN "entity_main" m
                ON m.ltbase_schema_id = $1
                AND m.ltbase_row_id = o.row_id
        ),
        eav_aggregated AS (
            SELECT
                e.row_id,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'schema_id', e.schema_id,
                        'row_id', e.row_id,
                        'attr_id', e.attr_id,
                        'array_indices', e.array_indices,
                        'value_text', e.value_text,
                        'value_numeric', e.value_numeric
                    ) ORDER BY e.attr_id, e.array_indices
                )::TEXT AS attributes_json
            FROM ordered o
            INNER JOIN "eav_data" e
                ON e.schema_id = $1
                AND e.row_id = o.row_id
            GROUP BY e.row_id
        )
        SELECT
            ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02,
            COALESCE(e.attributes_json, '[]') AS attributes_json,
            m.total AS total_records,
            CEIL(m.total::numeric / NULLIF($4::numeric, 0)) AS total_pages,
            (FLOOR($5::numeric / NULLIF($4::numeric, 0)) + 1)::int AS current_page
        FROM main_data m
        LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
        ORDER BY
            m.ltbase_row_id;
```

```text
Merge Left Join  (cost=20482.50..20576.44 rows=1000 width=608) (actual time=0.281..0.305 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=50 read=1
  CTE ordered
    ->  Limit  (cost=7148.34..7150.84 rows=1000 width=24) (actual time=0.223..0.225 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=5 read=1
          ->  Sort  (cost=7148.34..7258.39 rows=44020 width=24) (actual time=0.222..0.224 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=5 read=1
                ->  WindowAgg  (cost=3744.32..4734.77 rows=44020 width=24) (actual time=0.220..0.221 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=5 read=1
                      ->  HashAggregate  (cost=3744.32..4184.52 rows=44020 width=16) (actual time=0.135..0.217 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 1561kB
                            Buffers: shared hit=5 read=1
                            ->  Append  (cost=2949.39..3634.27 rows=44020 width=16) (actual time=0.039..0.126 rows=5 loops=1)
                                  Buffers: shared hit=5 read=1
                                  ->  HashAggregate  (cost=2949.39..3389.59 rows=44019 width=16) (actual time=0.039..0.123 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 1561kB
                                        Buffers: shared hit=3 read=1
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.42..2822.90 rows=50599 width=16) (actual time=0.027..0.027 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-00000001869b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=3 read=1
                                  ->  Nested Loop Semi Join  (cost=0.57..24.58 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.42..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-00000001869b'::text)
  ->  Sort  (cost=5423.33..5425.83 rows=1000 width=540) (actual time=0.247..0.247 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=25 read=1
        ->  Nested Loop  (cost=0.42..5373.50 rows=1000 width=540) (actual time=0.231..0.243 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=25 read=1
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.223..0.225 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=5 read=1
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.42..5.35 rows=1 width=532) (actual time=0.003..0.003 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=20
  ->  GroupAggregate  (cost=7908.33..7964.19 rows=2031 width=48) (actual time=0.031..0.051 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=25
        ->  Sort  (cost=7908.33..7913.41 rows=2031 width=75) (actual time=0.022..0.022 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=25
              ->  Nested Loop  (cost=0.42..7796.75 rows=2031 width=75) (actual time=0.005..0.018 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=25
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.42..7.76 rows=2 width=75) (actual time=0.003..0.003 rows=2 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=25
Planning:
  Buffers: shared hit=1
Planning Time: 0.440 ms
Execution Time: 0.437 ms
```
