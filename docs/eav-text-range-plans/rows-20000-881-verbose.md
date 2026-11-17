# Recorded run of the #815 benchmark after #881 under EXPLAIN VERBOSE, N = 20,000

`TestMixedEAVTextRangePlans` wrote everything below the line, unedited, with
`FORMA_EAV_PLAN_EVIDENCE` set. It is a second run of the
[#881 record](rows-20000-881.md), at the revision whose recorder writes the plan
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

## N = 20000

- Server: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Database collation: `en_US.utf8`; `eav_data.value_text`: `pg_catalog."C"`
- `e2e_wide` rows: 20000; `attr_id` note = 7, token = 17

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
| note + token | missing | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 14 | 0.15 |
| note + token | missing | note in | 2 | Index Only Scan on eav_data_text_idx | no | 1 | 2 | 2 |  | eav_data 6, entity_main 2 | eav_data 19998 | 289 | 4.17 |
| note + token | missing | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.17 |
| note + token | missing | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.16 |
| note + token | missing | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.17 |
| note + token | missing | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.17 |
| note + token | missing | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8140 | 19.56 |
| note + token | missing | note lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8139 | 19.71 |
| note + token | missing | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 14 | 0.08 |
| note + token | missing | token in | 2 | Index Only Scan on eav_data_text_idx | no | 1 | 2 | 2 |  | eav_data 6, entity_main 2 | eav_data 19998 | 457 | 4.02 |
| note + token | missing | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.12 |
| note + token | missing | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.12 |
| note + token | missing | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.12 |
| note + token | missing | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 46 | 0.12 |
| note + token | missing | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8224 | 19.18 |
| note + token | missing | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8223 | 19.46 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 111 | 0.19 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.17 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.17 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.16 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 62000, entity_main 1000 |  | 128276 | 75.21 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | note lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 62000, entity_main 1000 |  | 128274 | 71.25 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.30 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.16 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.16 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 40, entity_main 5 |  | 108 | 0.16 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 62000, entity_main 1000 |  | 128444 | 73.31 |
| note + token, pre-#881 anchor with the outer eav_data scan | missing | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 62000, entity_main 1000 |  | 128442 | 73.88 |
| note + token | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 14 | 0.08 |
| note + token | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 25 | 0.09 |
| note + token | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 588 | 12.67 |
| note + token | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 588 | 12.60 |
| note + token | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10198 | 5 | 2040 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17394 | 16.08 |
| note + token | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10199 | 5 | 2040 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17394 | 15.98 |
| note + token | fresh | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4799 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 1958 |  | 2408 | 27.63 |
| note + token | fresh | note lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 15197 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 2034 |  | 2484 | 27.36 |
| note + token | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 14 | 0.07 |
| note + token | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 25 | 0.09 |
| note + token | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10001 | 5 | 2000 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18762 | 16.99 |
| note + token | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10002 | 5 | 2000 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18762 | 16.93 |
| note + token | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.16 |
| note + token | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.13 |
| note + token | fresh | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 15004 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 1958 |  | 2452 | 28.70 |
| note + token | fresh | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4998 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 2034 |  | 2528 | 29.37 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1 | eav_data 40015, entity_main 18337 |  | 18778 | 15.76 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1 | eav_data 40015, entity_main 18337 |  | 18778 | 15.82 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10198 | 5 | 2040 | eav_data ×2 | eav_data 80005, entity_main 16975 |  | 17769 | 22.13 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10199 | 5 | 2040 | eav_data ×2 | eav_data 80005, entity_main 16975 |  | 17769 | 21.98 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4799 | 10000 | 2 | eav_data ×2 | eav_data 90000, entity_main 1958 |  | 2783 | 35.77 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | note lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 15197 | 10000 | 2 | eav_data ×2 | eav_data 90000, entity_main 2034 |  | 2859 | 37.76 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10001 | 5 | 2000 | eav_data ×2 | eav_data 80005, entity_main 18337 |  | 19137 | 22.69 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10002 | 5 | 2000 | eav_data ×2 | eav_data 80005, entity_main 18337 |  | 19137 | 22.71 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 25, entity_main 5 |  | 62 | 0.19 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 25, entity_main 5 |  | 62 | 0.14 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 15004 | 10000 | 2 | eav_data ×2 | eav_data 90000, entity_main 1958 |  | 2827 | 36.84 |
| note + token, pre-#881 anchor with the outer eav_data scan | fresh | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4998 | 10000 | 2 | eav_data ×2 | eav_data 90000, entity_main 2034 |  | 2903 | 37.12 |
| note + token | fresh | rank=7, title gt half | 10 | Index Only Scan on entity_main_cx_6a11ab76e5b5dcf2 | yes | 10 | 10 | 1 |  | eav_data 20, entity_main 20 |  | 85 | 0.17 |
| note + token | fresh | rank=7, title lt half | 10 | Index Only Scan on entity_main_cx_6a11ab76e5b5dcf2 | yes | 10 | 10 | 1 |  | eav_data 20, entity_main 20 |  | 85 | 0.18 |
| note only | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 13 | 0.08 |
| note only | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 23 | 0.09 |
| note only | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 20005, entity_main 20000 |  | 733 | 7.57 |
| note only | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 201 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 20005, entity_main 20000 |  | 733 | 7.76 |
| note only | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.13 |
| note only | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.10 |
| note only | fresh | note gt half | 10000 | Seq Scan on eav_data | no | 10000 | 10000 | 1 | eav_data ×2 | eav_data 30000, entity_main 1958 | eav_data 10000 | 3009 | 20.95 |
| note only | fresh | note lt half | 10000 | Seq Scan on eav_data | no | 9999 | 10000 | 1 | eav_data ×2 | eav_data 30000, entity_main 2034 | eav_data 10000 | 3085 | 20.93 |
| note + token | stale (note only) | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.12 |
| note + token | stale (note only) | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 6, entity_main 2 |  | 27 | 0.16 |
| note + token | stale (note only) | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 733 | 14.21 |
| note + token | stale (note only) | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 201 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 733 | 14.01 |
| note + token | stale (note only) | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.15 |
| note + token | stale (note only) | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 51 | 0.14 |
| note + token | stale (note only) | note gt half | 10000 | Seq Scan on eav_data | no | 10000 | 10000 | 1 | eav_data ×2 | eav_data 50000, entity_main 1958 | eav_data 30000 | 3009 | 30.17 |
| note + token | stale (note only) | note lt half | 10000 | Seq Scan on eav_data | no | 9999 | 10000 | 1 | eav_data ×2 | eav_data 50000, entity_main 2034 | eav_data 30000 | 3085 | 30.90 |
| note + token | stale (note only) | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 15 | 0.07 |
| note + token | stale (note only) | token in | 2 | Index Only Scan on eav_data_text_idx | no | 1 | 2 | 2 |  | eav_data 6, entity_main 2 | eav_data 19998 | 488 | 4.19 |
| note + token | stale (note only) | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 47 | 0.12 |
| note + token | stale (note only) | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 47 | 0.12 |
| note + token | stale (note only) | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 47 | 0.12 |
| note + token | stale (note only) | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 47 | 0.12 |
| note + token | stale (note only) | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8225 | 19.43 |
| note + token | stale (note only) | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 1 | 10000 | 10000 |  | eav_data 12000, entity_main 1000 |  | 8254 | 19.42 |
| token only | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 13 | 0.07 |
| token only | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 23 | 0.08 |
| token only | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.11 |
| token only | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.10 |
| token only | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.10 |
| token only | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 10, entity_main 5 |  | 41 | 0.17 |
| token only | fresh | token gt half | 10000 | Seq Scan on eav_data | no | 10000 | 10000 | 1 | eav_data ×2 | eav_data 30000, entity_main 1958 | eav_data 10000 | 3169 | 21.43 |
| token only | fresh | token lt half | 10000 | Seq Scan on eav_data | no | 9999 | 10000 | 1 | eav_data ×2 | eav_data 30000, entity_main 2034 | eav_data 10000 | 3245 | 21.42 |
| note + token on 1 row in 20 | fresh | note equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 2, entity_main 1 |  | 13 | 0.07 |
| note + token on 1 row in 20 | fresh | note in | 2 | Index Only Scan on eav_data_text_idx | yes | 2 | 2 | 1 |  | eav_data 4, entity_main 2 |  | 23 | 0.08 |
| note + token on 1 row in 20 | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 21005, entity_main 20000 |  | 372 | 7.88 |
| note + token on 1 row in 20 | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 201 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 21005, entity_main 20000 |  | 372 | 7.53 |
| note + token on 1 row in 20 | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 999 | 5 | 200 | eav_data ×1 | eav_data 21005, entity_main 16975 |  | 17178 | 9.47 |
| note + token on 1 row in 20 | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1000 | 5 | 200 | eav_data ×1 | eav_data 21005, entity_main 16975 |  | 17178 | 9.53 |
| note + token on 1 row in 20 | fresh | note gt half | 10000 | Seq Scan on eav_data | no | 9400 | 10000 | 1 | eav_data ×2 | eav_data 31000, entity_main 1958 | eav_data 11000 | 2287 | 21.24 |
| note + token on 1 row in 20 | fresh | note lt half | 10000 | Seq Scan on eav_data | no | 10599 | 10000 | 1 | eav_data ×2 | eav_data 31000, entity_main 2034 | eav_data 11000 | 2363 | 21.28 |
| note + token on 1 row in 20 | fresh | token equals | 1 | Index Only Scan on eav_data_text_idx | yes | 1 | 1 | 1 |  | eav_data 3, entity_main 1 |  | 14 | 0.07 |
| note + token on 1 row in 20 | fresh | token in | 2 | Index Only Scan on eav_data_text_idx | yes | 1 | 2 | 2 |  | eav_data 6, entity_main 2 |  | 25 | 0.09 |
| note + token on 1 row in 20 | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 960 | 5 | 192 | eav_data ×1 | eav_data 21005, entity_main 16571 |  | 16775 | 9.80 |
| note + token on 1 row in 20 | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 960 | 5 | 192 | eav_data ×1 | eav_data 21005, entity_main 16571 |  | 16775 | 9.87 |
| note + token on 1 row in 20 | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 48 | 0.12 |
| note + token on 1 row in 20 | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 1 | 5 | 5 |  | eav_data 15, entity_main 5 |  | 47 | 0.12 |
| note + token on 1 row in 20 | fresh | token gt half | 500 | Index Only Scan on eav_data_text_idx | yes | 979 | 500 | 2 | eav_data ×1 | eav_data 21500, entity_main 19947 |  | 20365 | 14.83 |
| note + token on 1 row in 20 | fresh | token lt half | 500 | Index Only Scan on eav_data_text_idx | yes | 21 | 500 | 24 |  | eav_data 1500, entity_main 500 |  | 4207 | 5.42 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.18 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.13 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10005 | 5 | 2001 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17552 | 16.49 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10005 | 5 | 2001 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17552 | 14.92 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 5000 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 1958 |  | 2614 | 27.43 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | note lt half | 10000 | Seq Scan on eav_data | no | 20000 | 10000 | 2 | eav_data ×2 | eav_data 50000, entity_main 2034 | eav_data 30000 | 3111 | 29.09 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10002 | 5 | 2000 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18920 | 15.09 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10002 | 5 | 2000 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18920 | 15.13 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.14 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 3 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.13 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token gt half | 10000 | Seq Scan on eav_data | no | 15000 | 10000 | 2 | eav_data ×2 | eav_data 50000, entity_main 1958 | eav_data 30000 | 3035 | 27.39 |
| note + token | fresh, target 10000 + mcv(attr_id, value_text) | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 5000 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 2034 |  | 2783 | 26.32 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 200 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 746 | 12.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 201 | 5 | 40 | eav_data ×1, entity_main ×1 | eav_data 40005, entity_main 20000 |  | 746 | 11.84 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10010 | 5 | 2002 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17552 | 14.21 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10011 | 5 | 2002 | eav_data ×1 | eav_data 40005, entity_main 16975 |  | 17552 | 14.48 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4805 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 1958 |  | 2566 | 26.94 |
| note + token, pre-#728 SQL on a default-collation column | fresh | note lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 15216 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 2034 |  | 2642 | 26.67 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10189 | 5 | 2038 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18920 | 15.25 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 10190 | 5 | 2038 | eav_data ×1 | eav_data 40005, entity_main 18337 |  | 18920 | 15.08 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lt 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lte 5 | 5 | Index Only Scan on eav_data_text_idx | yes | 2 | 5 | 2 |  | eav_data 15, entity_main 5 |  | 46 | 0.13 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token gt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 14983 | 10000 | 1 | eav_data ×1 | eav_data 50000, entity_main 1958 |  | 2610 | 30.30 |
| note + token, pre-#728 SQL on a default-collation column | fresh | token lt half | 10000 | Index Only Scan on eav_data_text_idx | yes | 4995 | 10000 | 2 | eav_data ×1 | eav_data 50000, entity_main 2034 |  | 2686 | 30.68 |

## Plans

### `token gt half`: note + token, statistics missing

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000002710"`
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
Merge Left Join  (cost=58.79..58.87 rows=2 width=608) (actual time=14.273..19.590 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=8224
  CTE ordered
    ->  Limit  (cost=25.19..25.19 rows=2 width=24) (actual time=10.153..10.250 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=224
          ->  Sort  (cost=25.19..25.19 rows=2 width=24) (actual time=10.152..10.191 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 119kB
                Buffers: shared hit=224
                ->  WindowAgg  (cost=25.14..25.18 rows=2 width=24) (actual time=8.251..9.100 rows=10000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=224
                      ->  Unique  (cost=25.14..25.15 rows=2 width=16) (actual time=5.837..7.323 rows=10000 loops=1)
                            Output: t.row_id
                            Buffers: shared hit=224
                            ->  Sort  (cost=25.14..25.15 rows=2 width=16) (actual time=5.837..6.181 rows=10000 loops=1)
                                  Output: t.row_id
                                  Sort Key: t.row_id
                                  Sort Method: quicksort  Memory: 619kB
                                  Buffers: shared hit=224
                                  ->  Append  (cost=8.44..25.13 rows=2 width=16) (actual time=3.317..5.243 rows=10000 loops=1)
                                        Buffers: shared hit=224
                                        ->  Unique  (cost=8.44..8.45 rows=1 width=16) (actual time=3.317..4.811 rows=10000 loops=1)
                                              Output: t.row_id
                                              Buffers: shared hit=222
                                              ->  Sort  (cost=8.44..8.45 rows=1 width=16) (actual time=3.316..3.672 rows=10000 loops=1)
                                                    Output: t.row_id
                                                    Sort Key: t.row_id
                                                    Sort Method: quicksort  Memory: 619kB
                                                    Buffers: shared hit=222
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..8.43 rows=1 width=16) (actual time=0.011..2.084 rows=10000 loops=1)
                                                          Output: t.row_id
                                                          Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000002710'::text))
                                                          Heap Fetches: 10000
                                                          Buffers: shared hit=222
                                        ->  Nested Loop Semi Join  (cost=0.56..16.67 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                              Output: cl.row_id
                                              Join Filter: (cl.row_id = t_1.row_id)
                                              Buffers: shared hit=2
                                              ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                                    Output: cl.schema_id, cl.row_id, cl.flushed_at
                                                    Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=2
                                              ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.41..8.43 rows=1 width=16) (never executed)
                                                    Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint) AND (t_1.value_text > '00000000-0000-4000-8000-000000002710'::text))
                                                    Heap Fetches: 0
  ->  Sort  (cost=16.67..16.67 rows=2 width=540) (actual time=11.949..11.985 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 118kB
        Buffers: shared hit=3224
        ->  Nested Loop  (cost=0.29..16.66 rows=2 width=540) (actual time=10.161..11.682 rows=1000 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=3224
              ->  CTE Scan on ordered o  (cost=0.00..0.04 rows=2 width=24) (actual time=10.154..10.341 rows=1000 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=224
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..8.31 rows=1 width=532) (actual time=0.001..0.001 rows=1 loops=1000)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=3000
  ->  GroupAggregate  (cost=16.94..16.97 rows=1 width=48) (actual time=2.320..7.050 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=5000
        ->  Sort  (cost=16.94..16.94 rows=1 width=116) (actual time=2.310..2.375 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=5000
              ->  Nested Loop  (cost=0.41..16.93 rows=1 width=116) (actual time=0.007..1.926 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=5000
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.04 rows=2 width=16) (actual time=0.000..0.059 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.41..8.43 rows=1 width=116) (actual time=0.001..0.002 rows=2 loops=1000)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=5000
Planning:
  Buffers: shared hit=1
Planning Time: 0.400 ms
Execution Time: 19.665 ms
```

### `token gt half`: note + token, pre-#881 anchor with the outer eav_data scan, statistics missing

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000002710"`
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
Merge Left Join  (cost=68.21..68.29 rows=2 width=608) (actual time=67.044..72.109 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=128444
  CTE ordered
    ->  Limit  (cost=34.60..34.61 rows=2 width=24) (actual time=63.181..63.273 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=120444
          ->  Sort  (cost=34.60..34.61 rows=2 width=24) (actual time=63.180..63.218 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 119kB
                Buffers: shared hit=120444
                ->  WindowAgg  (cost=34.56..34.59 rows=2 width=24) (actual time=61.367..62.182 rows=10000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=120444
                      ->  Unique  (cost=34.56..34.57 rows=2 width=16) (actual time=58.942..60.385 rows=10000 loops=1)
                            Output: t.row_id
                            Buffers: shared hit=120444
                            ->  Sort  (cost=34.56..34.56 rows=2 width=16) (actual time=58.941..59.276 rows=10000 loops=1)
                                  Output: t.row_id
                                  Sort Key: t.row_id
                                  Sort Method: quicksort  Memory: 619kB
                                  Buffers: shared hit=120444
                                  ->  Append  (cost=16.90..34.55 rows=2 width=16) (actual time=24.802..58.291 rows=10000 loops=1)
                                        Buffers: shared hit=120444
                                        ->  Unique  (cost=16.90..16.91 rows=1 width=16) (actual time=24.802..26.981 rows=10000 loops=1)
                                              Output: t.row_id
                                              Buffers: shared hit=50222
                                              ->  Sort  (cost=16.90..16.91 rows=1 width=16) (actual time=24.801..25.446 rows=20000 loops=1)
                                                    Output: t.row_id
                                                    Sort Key: t.row_id
                                                    Sort Method: quicksort  Memory: 1237kB
                                                    Buffers: shared hit=50222
                                                    ->  Nested Loop  (cost=8.85..16.89 rows=1 width=16) (actual time=3.626..22.103 rows=20000 loops=1)
                                                          Output: t.row_id
                                                          Buffers: shared hit=50222
                                                          ->  HashAggregate  (cost=8.44..8.45 rows=1 width=18) (actual time=3.620..4.549 rows=10000 loops=1)
                                                                Output: x.schema_id, x.row_id
                                                                Group Key: x.schema_id, x.row_id
                                                                Batches: 1  Memory Usage: 1193kB
                                                                Buffers: shared hit=222
                                                                ->  Index Only Scan using eav_data_text_idx on public.eav_data x  (cost=0.41..8.43 rows=1 width=18) (actual time=0.012..1.810 rows=10000 loops=1)
                                                                      Output: x.schema_id, x.attr_id, x.value_text, x.row_id
                                                                      Index Cond: ((x.schema_id = '21'::smallint) AND (x.attr_id = '17'::smallint) AND (x.value_text > '00000000-0000-4000-8000-000000002710'::text))
                                                                      Heap Fetches: 10000
                                                                      Buffers: shared hit=222
                                                          ->  Index Only Scan using eav_data_pkey on public.eav_data t  (cost=0.41..8.43 rows=1 width=18) (actual time=0.001..0.002 rows=2 loops=10000)
                                                                Output: t.schema_id, t.row_id, t.attr_id, t.array_indices
                                                                Index Cond: ((t.schema_id = '21'::smallint) AND (t.row_id = x.row_id))
                                                                Heap Fetches: 20000
                                                                Buffers: shared hit=50000
                                        ->  Nested Loop  (cost=17.05..17.63 rows=1 width=16) (actual time=30.857..30.858 rows=0 loops=1)
                                              Output: cl.row_id
                                              Inner Unique: true
                                              Join Filter: (cl.row_id = x_1.row_id)
                                              Buffers: shared hit=70222
                                              ->  HashAggregate  (cost=16.89..16.91 rows=1 width=32) (actual time=25.598..26.340 rows=10000 loops=1)
                                                    Output: t_1.row_id, x_1.row_id
                                                    Group Key: t_1.row_id
                                                    Batches: 1  Memory Usage: 1193kB
                                                    Buffers: shared hit=50222
                                                    ->  Nested Loop  (cost=8.85..16.89 rows=1 width=32) (actual time=3.714..22.284 rows=20000 loops=1)
                                                          Output: t_1.row_id, x_1.row_id
                                                          Buffers: shared hit=50222
                                                          ->  HashAggregate  (cost=8.44..8.45 rows=1 width=18) (actual time=3.710..4.634 rows=10000 loops=1)
                                                                Output: x_1.schema_id, x_1.row_id
                                                                Group Key: x_1.schema_id, x_1.row_id
                                                                Batches: 1  Memory Usage: 1193kB
                                                                Buffers: shared hit=222
                                                                ->  Index Only Scan using eav_data_text_idx on public.eav_data x_1  (cost=0.41..8.43 rows=1 width=18) (actual time=0.008..1.752 rows=10000 loops=1)
                                                                      Output: x_1.schema_id, x_1.attr_id, x_1.value_text, x_1.row_id
                                                                      Index Cond: ((x_1.schema_id = '21'::smallint) AND (x_1.attr_id = '17'::smallint) AND (x_1.value_text > '00000000-0000-4000-8000-000000002710'::text))
                                                                      Heap Fetches: 10000
                                                                      Buffers: shared hit=222
                                                          ->  Index Only Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.43 rows=1 width=18) (actual time=0.001..0.002 rows=2 loops=10000)
                                                                Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices
                                                                Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = x_1.row_id))
                                                                Heap Fetches: 20000
                                                                Buffers: shared hit=50000
                                              ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..0.71 rows=1 width=16) (actual time=0.000..0.000 rows=0 loops=10000)
                                                    Output: cl.schema_id, cl.row_id, cl.flushed_at
                                                    Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.row_id = t_1.row_id) AND (cl.flushed_at = 0))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=20000
  ->  Sort  (cost=16.67..16.67 rows=2 width=540) (actual time=64.941..64.975 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 118kB
        Buffers: shared hit=123444
        ->  Nested Loop  (cost=0.29..16.66 rows=2 width=540) (actual time=63.191..64.676 rows=1000 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=123444
              ->  CTE Scan on ordered o  (cost=0.00..0.04 rows=2 width=24) (actual time=63.182..63.388 rows=1000 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=120444
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..8.31 rows=1 width=532) (actual time=0.001..0.001 rows=1 loops=1000)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=3000
  ->  GroupAggregate  (cost=16.94..16.97 rows=1 width=48) (actual time=2.099..6.602 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=5000
        ->  Sort  (cost=16.94..16.94 rows=1 width=116) (actual time=2.085..2.149 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=5000
              ->  Nested Loop  (cost=0.41..16.93 rows=1 width=116) (actual time=0.004..1.708 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=5000
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.04 rows=2 width=16) (actual time=0.000..0.056 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.41..8.43 rows=1 width=116) (actual time=0.001..0.001 rows=2 loops=1000)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=5000
Planning:
  Buffers: shared hit=1
Planning Time: 0.578 ms
Execution Time: 72.217 ms
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
Merge Left Join  (cost=3070.19..4636.45 rows=1000 width=608) (actual time=11.390..15.830 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=17394
  CTE ordered
    ->  Limit  (cost=1468.14..1470.64 rows=1000 width=24) (actual time=0.077..0.079 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=1468.14..1490.39 rows=8902 width=24) (actual time=0.076..0.078 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=779.75..980.05 rows=8902 width=24) (actual time=0.072..0.074 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=779.75..868.77 rows=8902 width=16) (actual time=0.055..0.069 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 409kB
                            Buffers: shared hit=6
                            ->  Append  (cost=599.37..757.50 rows=8902 width=16) (actual time=0.022..0.044 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=599.37..688.38 rows=8901 width=16) (actual time=0.022..0.035 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 409kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..573.87 rows=10198 width=16) (actual time=0.008..0.009 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '7'::smallint) AND (t.value_text < 't000006'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.61 rows=1 width=16) (actual time=0.006..0.007 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.006..0.006 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '7'::smallint))
                                              Filter: (t_1.value_text < 't000006'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=0.398..4.809 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=17019
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.012..3.779 rows=16975 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=17013
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=0.085..0.086 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=6
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.078..0.079 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=6
  ->  GroupAggregate  (cost=1529.44..1584.36 rows=1997 width=48) (actual time=10.988..11.011 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=375
        ->  Sort  (cost=1529.44..1534.43 rows=1997 width=75) (actual time=10.978..10.979 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=375
              ->  Hash Join  (cost=1375.00..1419.97 rows=1997 width=75) (actual time=10.967..10.970 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=375
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=875.00..875.00 rows=40000 width=75) (actual time=10.773..10.774 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=375
                          ->  Seq Scan on public.eav_data e  (cost=0.00..875.00 rows=40000 width=75) (actual time=0.003..4.121 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=375
Planning:
  Buffers: shared hit=1
Planning Time: 0.467 ms
Execution Time: 16.192 ms
```

### `token gt 5`: note + token, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004e1b"`
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
Merge Left Join  (cost=3043.40..4609.66 rows=1000 width=608) (actual time=12.113..16.614 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=18762
  CTE ordered
    ->  Limit  (cost=1441.34..1443.84 rows=1000 width=24) (actual time=0.075..0.077 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=1441.34..1463.23 rows=8755 width=24) (actual time=0.074..0.076 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=764.33..961.32 rows=8755 width=24) (actual time=0.071..0.073 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=764.33..851.88 rows=8755 width=16) (actual time=0.044..0.068 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 409kB
                            Buffers: shared hit=6
                            ->  Append  (cost=586.44..742.44 rows=8755 width=16) (actual time=0.014..0.042 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=586.44..673.98 rows=8754 width=16) (actual time=0.013..0.036 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 409kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..561.44 rows=10001 width=16) (actual time=0.009..0.010 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.69 rows=1 width=16) (actual time=0.005..0.006 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.005..0.005 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-000000004e1b'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=0.985..5.457 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=18387
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.010..4.236 rows=18337 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=18381
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=0.085..0.086 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=6
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.076..0.077 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=6
  ->  GroupAggregate  (cost=1529.44..1584.36 rows=1997 width=48) (actual time=11.124..11.148 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=375
        ->  Sort  (cost=1529.44..1534.43 rows=1997 width=75) (actual time=11.113..11.115 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=375
              ->  Hash Join  (cost=1375.00..1419.97 rows=1997 width=75) (actual time=11.102..11.105 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=375
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=875.00..875.00 rows=40000 width=75) (actual time=10.917..10.917 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=375
                          ->  Seq Scan on public.eav_data e  (cost=0.00..875.00 rows=40000 width=75) (actual time=0.003..4.176 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=375
Planning:
  Buffers: shared hit=1
Planning Time: 0.417 ms
Execution Time: 16.928 ms
```

### `token gt half`: note + token, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000002710"`
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
Merge Left Join  (cost=3661.37..5227.63 rows=1000 width=608) (actual time=22.334..28.544 rows=1000 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=2452
  CTE ordered
    ->  Limit  (cost=2059.32..2061.82 rows=1000 width=24) (actual time=9.314..9.393 rows=1000 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=110
          ->  Sort  (cost=2059.32..2089.81 rows=12198 width=24) (actual time=9.313..9.349 rows=1000 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: top-N heapsort  Memory: 167kB
                Buffers: shared hit=110
                ->  WindowAgg  (cost=1116.06..1390.51 rows=12198 width=24) (actual time=6.965..7.805 rows=10000 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=110
                      ->  HashAggregate  (cost=1116.06..1238.04 rows=12198 width=16) (actual time=4.997..5.678 rows=10000 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 913kB
                            Buffers: shared hit=110
                            ->  Append  (cost=879.51..1085.56 rows=12198 width=16) (actual time=2.539..3.726 rows=10000 loops=1)
                                  Buffers: shared hit=110
                                  ->  HashAggregate  (cost=879.51..1001.48 rows=12197 width=16) (actual time=2.538..3.281 rows=10000 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 913kB
                                        Buffers: shared hit=108
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..842.00 rows=15004 width=16) (actual time=0.008..1.199 rows=10000 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000002710'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=108
                                  ->  Nested Loop Semi Join  (cost=0.57..23.09 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.002..0.002 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-000000002710'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=9.611..10.671 rows=1000 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=2077
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.008..0.613 rows=1958 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=1967
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=9.601..9.645 rows=1000 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 71kB
              Buffers: shared hit=110
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=9.315..9.517 rows=1000 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=110
  ->  GroupAggregate  (cost=1529.44..1584.36 rows=1997 width=48) (actual time=12.718..17.380 rows=1000 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=375
        ->  Sort  (cost=1529.44..1534.43 rows=1997 width=75) (actual time=12.704..12.777 rows=2000 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 189kB
              Buffers: shared hit=375
              ->  Hash Join  (cost=1375.00..1419.97 rows=1997 width=75) (actual time=11.934..12.299 rows=2000 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=375
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.055 rows=1000 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=875.00..875.00 rows=40000 width=75) (actual time=11.737..11.738 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=375
                          ->  Seq Scan on public.eav_data e  (cost=0.00..875.00 rows=40000 width=75) (actual time=0.005..4.449 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=375
Planning:
  Buffers: shared hit=1
Planning Time: 0.427 ms
Execution Time: 28.658 ms
```

### `token gt 5`: note + token, pre-#881 anchor with the outer eav_data scan, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004e1b"`
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
Merge Left Join  (cost=5473.54..7039.80 rows=1000 width=608) (actual time=17.701..22.181 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=19137
  CTE ordered
    ->  Limit  (cost=3871.49..3873.99 rows=1000 width=24) (actual time=5.393..5.396 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=381
          ->  Sort  (cost=3871.49..3921.42 rows=19975 width=24) (actual time=5.392..5.395 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=381
                ->  WindowAgg  (cost=2326.84..2776.28 rows=19975 width=24) (actual time=5.388..5.391 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=381
                      ->  HashAggregate  (cost=2326.84..2526.59 rows=19975 width=16) (actual time=5.334..5.383 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 793kB
                            Buffers: shared hit=381
                            ->  Append  (cost=1963.56..2276.90 rows=19975 width=16) (actual time=5.274..5.330 rows=5 loops=1)
                                  Buffers: shared hit=381
                                  ->  HashAggregate  (cost=1963.56..2163.30 rows=19974 width=16) (actual time=5.273..5.322 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 793kB
                                        Buffers: shared hit=379
                                        ->  Hash Semi Join  (cost=686.45..1913.63 rows=19974 width=16) (actual time=2.640..5.266 rows=10 loops=1)
                                              Output: t.row_id
                                              Hash Cond: (t.row_id = x.row_id)
                                              Buffers: shared hit=379
                                              ->  Seq Scan on public.eav_data t  (cost=0.00..875.00 rows=40000 width=18) (actual time=0.003..2.663 rows=40000 loops=1)
                                                    Output: t.schema_id, t.row_id, t.attr_id, t.array_indices, t.value_text, t.value_numeric
                                                    Filter: (t.schema_id = '21'::smallint)
                                                    Buffers: shared hit=375
                                              ->  Hash  (cost=561.44..561.44 rows=10001 width=18) (actual time=0.015..0.015 rows=5 loops=1)
                                                    Output: x.schema_id, x.row_id
                                                    Buckets: 16384  Batches: 1  Memory Usage: 129kB
                                                    Buffers: shared hit=4
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data x  (cost=0.41..561.44 rows=10001 width=18) (actual time=0.011..0.012 rows=5 loops=1)
                                                          Output: x.schema_id, x.row_id
                                                          Index Cond: ((x.schema_id = '21'::smallint) AND (x.attr_id = '17'::smallint) AND (x.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                                          Heap Fetches: 0
                                                          Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.98..13.73 rows=1 width=16) (actual time=0.006..0.006 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.006..0.006 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Nested Loop Semi Join  (cost=0.83..5.49 rows=1 width=32) (never executed)
                                              Output: t_1.row_id, x_1.row_id
                                              Join Filter: (cl.row_id = x_1.row_id)
                                              ->  Index Only Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..4.45 rows=2 width=18) (never executed)
                                                    Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id))
                                                    Heap Fetches: 0
                                              ->  Index Scan using eav_data_pkey on public.eav_data x_1  (cost=0.41..0.51 rows=1 width=18) (never executed)
                                                    Output: x_1.schema_id, x_1.row_id, x_1.attr_id, x_1.array_indices, x_1.value_text, x_1.value_numeric
                                                    Index Cond: ((x_1.schema_id = '21'::smallint) AND (x_1.row_id = t_1.row_id) AND (x_1.attr_id = '17'::smallint))
                                                    Filter: (x_1.value_text > '00000000-0000-4000-8000-000000004e1b'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=6.362..10.811 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=18762
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.011..4.333 rows=18337 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=18381
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=5.399..5.400 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=381
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=5.393..5.395 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=381
  ->  GroupAggregate  (cost=1529.44..1584.36 rows=1997 width=48) (actual time=11.335..11.360 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=375
        ->  Sort  (cost=1529.44..1534.43 rows=1997 width=75) (actual time=11.324..11.325 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=375
              ->  Hash Join  (cost=1375.00..1419.97 rows=1997 width=75) (actual time=11.312..11.314 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=375
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=875.00..875.00 rows=40000 width=75) (actual time=11.119..11.119 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=375
                          ->  Seq Scan on public.eav_data e  (cost=0.00..875.00 rows=40000 width=75) (actual time=0.003..4.262 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=375
Planning:
  Buffers: shared hit=1
Planning Time: 0.713 ms
Execution Time: 22.849 ms
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
Merge Left Join  (cost=50.79..50.91 rows=2 width=608) (actual time=4.186..4.195 rows=2 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=488
  CTE ordered
    ->  Limit  (cost=17.19..17.19 rows=2 width=24) (actual time=4.153..4.155 rows=2 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=472
          ->  Sort  (cost=17.19..17.19 rows=2 width=24) (actual time=4.153..4.154 rows=2 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=472
                ->  WindowAgg  (cost=17.14..17.18 rows=2 width=24) (actual time=4.151..4.153 rows=2 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=472
                      ->  Unique  (cost=17.14..17.15 rows=2 width=16) (actual time=4.149..4.150 rows=2 loops=1)
                            Output: t.row_id
                            Buffers: shared hit=472
                            ->  Sort  (cost=17.14..17.15 rows=2 width=16) (actual time=4.148..4.149 rows=2 loops=1)
                                  Output: t.row_id
                                  Sort Key: t.row_id
                                  Sort Method: quicksort  Memory: 25kB
                                  Buffers: shared hit=472
                                  ->  Append  (cost=4.44..17.13 rows=2 width=16) (actual time=4.145..4.148 rows=2 loops=1)
                                        Buffers: shared hit=472
                                        ->  Unique  (cost=4.44..4.45 rows=1 width=16) (actual time=4.144..4.146 rows=2 loops=1)
                                              Output: t.row_id
                                              Buffers: shared hit=470
                                              ->  Sort  (cost=4.44..4.45 rows=1 width=16) (actual time=4.144..4.144 rows=2 loops=1)
                                                    Output: t.row_id
                                                    Sort Key: t.row_id
                                                    Sort Method: quicksort  Memory: 25kB
                                                    Buffers: shared hit=470
                                                    ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..4.43 rows=1 width=16) (actual time=0.014..4.143 rows=2 loops=1)
                                                          Output: t.row_id
                                                          Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint))
                                                          Filter: (t.value_text = ANY ('{00000000-0000-4000-8000-00000000002b,00000000-0000-4000-8000-00000000002c}'::text[]))
                                                          Rows Removed by Filter: 19998
                                                          Heap Fetches: 20000
                                                          Buffers: shared hit=470
                                        ->  Nested Loop Semi Join  (cost=0.56..12.67 rows=1 width=16) (actual time=0.001..0.002 rows=0 loops=1)
                                              Output: cl.row_id
                                              Join Filter: (cl.row_id = t_1.row_id)
                                              Buffers: shared hit=2
                                              ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                                    Output: cl.schema_id, cl.row_id, cl.flushed_at
                                                    Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=2
                                              ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.41..4.43 rows=1 width=16) (never executed)
                                                    Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                                    Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint))
                                                    Filter: (t_1.value_text = ANY ('{00000000-0000-4000-8000-00000000002b,00000000-0000-4000-8000-00000000002c}'::text[]))
                                                    Heap Fetches: 0
  ->  Sort  (cost=16.67..16.67 rows=2 width=540) (actual time=4.164..4.164 rows=2 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=478
        ->  Nested Loop  (cost=0.29..16.66 rows=2 width=540) (actual time=4.158..4.161 rows=2 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=478
              ->  CTE Scan on ordered o  (cost=0.00..0.04 rows=2 width=24) (actual time=4.154..4.155 rows=2 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=472
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..8.31 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=2)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=6
  ->  GroupAggregate  (cost=16.94..16.99 rows=2 width=48) (actual time=0.019..0.025 rows=2 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=10
        ->  Sort  (cost=16.94..16.94 rows=2 width=61) (actual time=0.010..0.010 rows=4 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=10
              ->  Nested Loop  (cost=0.41..16.93 rows=2 width=61) (actual time=0.003..0.008 rows=4 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=10
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.04 rows=2 width=16) (actual time=0.000..0.000 rows=2 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.41..8.43 rows=1 width=61) (actual time=0.002..0.003 rows=2 loops=2)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=10
Planning:
  Buffers: shared hit=1
Planning Time: 0.395 ms
Execution Time: 4.232 ms
```

### `token gt 5`: token only, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004e1b"`
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
Merge Left Join  (cost=67.72..67.89 rows=3 width=608) (actual time=0.054..0.069 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=41
  CTE ordered
    ->  Limit  (cost=17.30..17.30 rows=3 width=24) (actual time=0.017..0.018 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=17.30..17.30 rows=3 width=24) (actual time=0.016..0.017 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=17.20..17.27 rows=3 width=24) (actual time=0.014..0.016 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=17.20..17.23 rows=3 width=16) (actual time=0.012..0.013 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 24kB
                            Buffers: shared hit=6
                            ->  Append  (cost=4.47..17.20 rows=3 width=16) (actual time=0.007..0.010 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  Unique  (cost=4.47..4.48 rows=2 width=16) (actual time=0.007..0.009 rows=5 loops=1)
                                        Output: t.row_id
                                        Buffers: shared hit=4
                                        ->  Sort  (cost=4.47..4.47 rows=2 width=16) (actual time=0.007..0.007 rows=5 loops=1)
                                              Output: t.row_id
                                              Sort Key: t.row_id
                                              Sort Method: quicksort  Memory: 25kB
                                              Buffers: shared hit=4
                                              ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..4.46 rows=2 width=16) (actual time=0.005..0.005 rows=5 loops=1)
                                                    Output: t.row_id
                                                    Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                                    Heap Fetches: 0
                                                    Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.56..12.70 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Join Filter: (cl.row_id = t_1.row_id)
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t_1  (cost=0.41..4.46 rows=2 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.attr_id, t_1.value_text, t_1.row_id
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.attr_id = '17'::smallint) AND (t_1.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                              Heap Fetches: 0
  ->  Sort  (cost=25.01..25.01 rows=3 width=540) (actual time=0.032..0.032 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Sort Key: m.ltbase_row_id
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=21
        ->  Nested Loop  (cost=0.29..24.98 rows=3 width=540) (actual time=0.021..0.029 rows=5 loops=1)
              Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
              Inner Unique: true
              Buffers: shared hit=21
              ->  CTE Scan on ordered o  (cost=0.00..0.06 rows=3 width=24) (actual time=0.017..0.018 rows=5 loops=1)
                    Output: o.row_id, o.total
                    Buffers: shared hit=6
              ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..8.31 rows=1 width=532) (actual time=0.002..0.002 rows=1 loops=5)
                    Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
                    Index Cond: ((m.ltbase_schema_id = '21'::smallint) AND (m.ltbase_row_id = o.row_id))
                    Buffers: shared hit=15
  ->  GroupAggregate  (cost=25.41..25.49 rows=3 width=48) (actual time=0.019..0.030 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=20
        ->  Sort  (cost=25.41..25.42 rows=3 width=90) (actual time=0.013..0.014 rows=5 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=20
              ->  Nested Loop  (cost=0.41..25.39 rows=3 width=90) (actual time=0.003..0.011 rows=5 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Buffers: shared hit=20
                    ->  CTE Scan on ordered o_1  (cost=0.00..0.06 rows=3 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Index Scan using eav_data_pkey on public.eav_data e  (cost=0.41..8.43 rows=1 width=90) (actual time=0.002..0.002 rows=1 loops=5)
                          Output: e.schema_id, e.row_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Index Cond: ((e.schema_id = '21'::smallint) AND (e.row_id = o_1.row_id))
                          Buffers: shared hit=20
Planning:
  Buffers: shared hit=1
Planning Time: 0.347 ms
Execution Time: 0.103 ms
```

### `token gt 5`: note + token on 1 row in 20, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004dbc"`
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
Merge Left Join  (cost=1136.85..2662.02 rows=960 width=608) (actual time=5.464..9.979 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=16775
  CTE ordered
    ->  Limit  (cost=299.04..301.44 rows=960 width=24) (actual time=0.022..0.024 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=7
          ->  Sort  (cost=299.04..301.44 rows=960 width=24) (actual time=0.021..0.023 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=7
                ->  WindowAgg  (cost=229.89..251.49 rows=960 width=24) (actual time=0.019..0.021 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=7
                      ->  HashAggregate  (cost=229.89..239.49 rows=960 width=16) (actual time=0.014..0.017 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 73kB
                            Buffers: shared hit=7
                            ->  Append  (cost=188.41..227.49 rows=960 width=16) (actual time=0.008..0.013 rows=5 loops=1)
                                  Buffers: shared hit=7
                                  ->  HashAggregate  (cost=188.41..198.00 rows=959 width=16) (actual time=0.008..0.011 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 73kB
                                        Buffers: shared hit=5
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..186.01 rows=960 width=16) (actual time=0.004..0.005 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000004dbc'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=5
                                  ->  Nested Loop Semi Join  (cost=0.56..24.68 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-000000004dbc'::text)
  ->  Merge Join  (cost=67.04..1541.82 rows=960 width=540) (actual time=0.146..4.633 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=16616
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.006..3.647 rows=16571 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=16609
        ->  Sort  (cost=66.75..69.15 rows=960 width=24) (actual time=0.026..0.026 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=7
              ->  CTE Scan on ordered o  (cost=0.00..19.20 rows=960 width=24) (actual time=0.023..0.024 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=7
  ->  GroupAggregate  (cost=768.37..796.09 rows=1008 width=48) (actual time=5.315..5.339 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=159
        ->  Sort  (cost=768.37..770.89 rows=1008 width=62) (actual time=5.306..5.307 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=159
              ->  Hash Join  (cost=684.00..718.08 rows=1008 width=62) (actual time=5.300..5.303 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=159
                    ->  CTE Scan on ordered o_1  (cost=0.00..19.20 rows=960 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=421.50..421.50 rows=21000 width=62) (actual time=5.292..5.292 rows=21000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 32768  Batches: 1  Memory Usage: 1536kB
                          Buffers: shared hit=159
                          ->  Seq Scan on public.eav_data e  (cost=0.00..421.50 rows=21000 width=62) (actual time=0.003..2.140 rows=21000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=159
Planning:
  Buffers: shared hit=1
Planning Time: 0.369 ms
Execution Time: 10.021 ms
```

### `token gt 5`: note + token, statistics fresh, target 10000 + mcv(attr_id, value_text)

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004e1b"`
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
Merge Left Join  (cost=3745.36..5311.73 rows=1000 width=608) (actual time=10.777..15.104 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=18920
  CTE ordered
    ->  Limit  (cost=1985.08..1987.58 rows=1000 width=24) (actual time=0.065..0.067 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=6
          ->  Sort  (cost=1985.08..2006.96 rows=8752 width=24) (actual time=0.064..0.065 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=6
                ->  WindowAgg  (cost=1308.30..1505.22 rows=8752 width=24) (actual time=0.061..0.063 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=6
                      ->  HashAggregate  (cost=1308.30..1395.82 rows=8752 width=16) (actual time=0.037..0.059 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 409kB
                            Buffers: shared hit=6
                            ->  Append  (cost=1130.47..1286.42 rows=8752 width=16) (actual time=0.010..0.034 rows=5 loops=1)
                                  Buffers: shared hit=6
                                  ->  HashAggregate  (cost=1130.47..1217.98 rows=8751 width=16) (actual time=0.010..0.032 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 409kB
                                        Buffers: shared hit=4
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..1105.46 rows=10002 width=16) (actual time=0.006..0.006 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=4
                                  ->  Nested Loop Semi Join  (cost=0.57..24.69 rows=1 width=16) (actual time=0.001..0.002 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-000000004e1b'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=1.058..5.357 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=18387
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.009..4.249 rows=18337 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=18381
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=0.070..0.070 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=6
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.066..0.067 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=6
  ->  GroupAggregate  (cost=1687.66..1742.66 rows=2000 width=48) (actual time=9.716..9.739 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=533
        ->  Sort  (cost=1687.66..1692.66 rows=2000 width=75) (actual time=9.705..9.706 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=533
              ->  Hash Join  (cost=1533.00..1578.00 rows=2000 width=75) (actual time=9.699..9.701 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=533
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=1033.00..1033.00 rows=40000 width=75) (actual time=9.682..9.682 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=533
                          ->  Seq Scan on public.eav_data e  (cost=0.00..1033.00 rows=40000 width=75) (actual time=0.044..4.122 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=533
Planning:
  Buffers: shared hit=1
Planning Time: 4.053 ms
Execution Time: 15.180 ms
```

### `token gt 5`: note + token, pre-#728 SQL on a default-collation column, statistics fresh

Arguments:

- `$1` = `21`
- `$2` = `17`
- `$3` = `"00000000-0000-4000-8000-000000004e1b"`
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
Merge Left Join  (cost=3227.33..4793.71 rows=1000 width=608) (actual time=10.817..15.194 rows=5 loops=1)
  Output: m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, COALESCE(((json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text), '[]'::text), o.total, ceil(((o.total)::numeric / '1000'::numeric)), 1
  Inner Unique: true
  Merge Cond: (m.ltbase_row_id = e.row_id)
  Buffers: shared hit=18919 read=1
  CTE ordered
    ->  Limit  (cost=1467.06..1469.56 rows=1000 width=24) (actual time=0.088..0.090 rows=5 loops=1)
          Output: t.row_id, (count(*) OVER (?))
          Buffers: shared hit=5 read=1
          ->  Sort  (cost=1467.06..1489.29 rows=8893 width=24) (actual time=0.088..0.089 rows=5 loops=1)
                Output: t.row_id, (count(*) OVER (?))
                Sort Key: t.row_id
                Sort Method: quicksort  Memory: 25kB
                Buffers: shared hit=5 read=1
                ->  WindowAgg  (cost=779.37..979.46 rows=8893 width=24) (actual time=0.085..0.087 rows=5 loops=1)
                      Output: t.row_id, count(*) OVER (?)
                      Buffers: shared hit=5 read=1
                      ->  HashAggregate  (cost=779.37..868.30 rows=8893 width=16) (actual time=0.060..0.084 rows=5 loops=1)
                            Output: t.row_id
                            Group Key: t.row_id
                            Batches: 1  Memory Usage: 409kB
                            Buffers: shared hit=5 read=1
                            ->  Append  (cost=599.14..757.14 rows=8893 width=16) (actual time=0.033..0.058 rows=5 loops=1)
                                  Buffers: shared hit=5 read=1
                                  ->  HashAggregate  (cost=599.14..688.06 rows=8892 width=16) (actual time=0.033..0.055 rows=5 loops=1)
                                        Output: t.row_id
                                        Group Key: t.row_id
                                        Batches: 1  Memory Usage: 409kB
                                        Buffers: shared hit=3 read=1
                                        ->  Index Only Scan using eav_data_text_idx on public.eav_data t  (cost=0.41..573.67 rows=10189 width=16) (actual time=0.028..0.029 rows=5 loops=1)
                                              Output: t.schema_id, t.attr_id, t.value_text, t.row_id
                                              Index Cond: ((t.schema_id = '21'::smallint) AND (t.attr_id = '17'::smallint) AND (t.value_text > '00000000-0000-4000-8000-000000004e1b'::text))
                                              Heap Fetches: 0
                                              Buffers: shared hit=3 read=1
                                  ->  Nested Loop Semi Join  (cost=0.57..24.61 rows=1 width=16) (actual time=0.001..0.002 rows=0 loops=1)
                                        Output: cl.row_id
                                        Buffers: shared hit=2
                                        ->  Index Only Scan using change_log_pkey on public.change_log cl  (cost=0.15..8.22 rows=1 width=16) (actual time=0.001..0.001 rows=0 loops=1)
                                              Output: cl.schema_id, cl.row_id, cl.flushed_at
                                              Index Cond: ((cl.schema_id = '21'::smallint) AND (cl.flushed_at = 0))
                                              Heap Fetches: 0
                                              Buffers: shared hit=2
                                        ->  Index Scan using eav_data_pkey on public.eav_data t_1  (cost=0.41..8.44 rows=1 width=16) (never executed)
                                              Output: t_1.schema_id, t_1.row_id, t_1.attr_id, t_1.array_indices, t_1.value_text, t_1.value_numeric
                                              Index Cond: ((t_1.schema_id = '21'::smallint) AND (t_1.row_id = cl.row_id) AND (t_1.attr_id = '17'::smallint))
                                              Filter: (t_1.value_text > '00000000-0000-4000-8000-000000004e1b'::text)
  ->  Merge Join  (cost=70.12..1545.49 rows=1000 width=540) (actual time=0.963..5.311 rows=5 loops=1)
        Output: o.total, m.ltbase_schema_id, m.ltbase_row_id, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_deleted_by, m.ltbase_updated_by, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02
        Merge Cond: (m.ltbase_row_id = o.row_id)
        Buffers: shared hit=18386 read=1
        ->  Index Scan using entity_main_pkey on public.entity_main m  (cost=0.29..1410.66 rows=20000 width=532) (actual time=0.007..4.122 rows=18337 loops=1)
              Output: m.ltbase_schema_id, m.ltbase_row_id, m.text_01, m.text_02, m.text_03, m.text_04, m.text_05, m.text_06, m.text_07, m.text_08, m.text_09, m.text_10, m.smallint_01, m.smallint_02, m.smallint_03, m.integer_01, m.integer_02, m.integer_03, m.bigint_01, m.bigint_02, m.bigint_03, m.double_01, m.double_02, m.double_03, m.uuid_01, m.uuid_02, m.ltbase_created_at, m.ltbase_updated_at, m.ltbase_deleted_at, m.ltbase_created_by, m.ltbase_updated_by, m.ltbase_deleted_by
              Index Cond: (m.ltbase_schema_id = '21'::smallint)
              Buffers: shared hit=18381
        ->  Sort  (cost=69.83..72.33 rows=1000 width=24) (actual time=0.092..0.093 rows=5 loops=1)
              Output: o.total, o.row_id
              Sort Key: o.row_id
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=5 read=1
              ->  CTE Scan on ordered o  (cost=0.00..20.00 rows=1000 width=24) (actual time=0.089..0.091 rows=5 loops=1)
                    Output: o.total, o.row_id
                    Buffers: shared hit=5 read=1
  ->  GroupAggregate  (cost=1687.66..1742.66 rows=2000 width=48) (actual time=9.851..9.875 rows=5 loops=1)
        Output: e.row_id, (json_agg(json_build_object('schema_id', e.schema_id, 'row_id', e.row_id, 'attr_id', e.attr_id, 'array_indices', e.array_indices, 'value_text', e.value_text, 'value_numeric', e.value_numeric) ORDER BY e.attr_id, e.array_indices))::text
        Group Key: e.row_id
        Buffers: shared hit=533
        ->  Sort  (cost=1687.66..1692.66 rows=2000 width=75) (actual time=9.841..9.842 rows=10 loops=1)
              Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
              Sort Key: e.row_id, e.attr_id, e.array_indices COLLATE "C"
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=533
              ->  Hash Join  (cost=1533.00..1578.00 rows=2000 width=75) (actual time=9.834..9.837 rows=10 loops=1)
                    Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                    Hash Cond: (o_1.row_id = e.row_id)
                    Buffers: shared hit=533
                    ->  CTE Scan on ordered o_1  (cost=0.00..20.00 rows=1000 width=16) (actual time=0.000..0.001 rows=5 loops=1)
                          Output: o_1.row_id, o_1.total
                    ->  Hash  (cost=1033.00..1033.00 rows=40000 width=75) (actual time=9.820..9.820 rows=40000 loops=1)
                          Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                          Buckets: 65536  Batches: 1  Memory Usage: 3462kB
                          Buffers: shared hit=533
                          ->  Seq Scan on public.eav_data e  (cost=0.00..1033.00 rows=40000 width=75) (actual time=0.041..4.171 rows=40000 loops=1)
                                Output: e.row_id, e.schema_id, e.attr_id, e.array_indices, e.value_text, e.value_numeric
                                Filter: (e.schema_id = '21'::smallint)
                                Buffers: shared hit=533
Planning:
  Buffers: shared hit=1
Planning Time: 0.408 ms
Execution Time: 15.264 ms
```
