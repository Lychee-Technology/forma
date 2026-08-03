# Issue #251 — Spike findings: DuckDB partial/corrupt Parquet read semantics

**Date:** 2026-08-02
**Driver pinned:** `github.com/duckdb/duckdb-go/v2 v2.5.6` (bindings `github.com/duckdb/duckdb-go-bindings v0.3.5`)
**DuckDB engine version reported by `SELECT version()`:** **v1.4.5**
**Platform:** darwin/arm64
**Spike file (temporary, deleted after this run):** `internal/federated/spike_corruption_test.go`

> Note: the brief's spike code imported `github.com/marcboeker/go-duckdb/v2`. This repo does **not** use that
> module; it pins `github.com/duckdb/duckdb-go/v2` (see `internal/federated/duckdb_conn.go:10`). The import was
> adjusted accordingly. `duckdb.NewConnector(dsn, nil)` has the same shape in this fork, so the call site was
> otherwise unchanged.

---

## HEADLINE: Assumption B is FALSE. The gate is blocked.

`SELECT * FROM read_parquet(f)` **does not fail** on the brief's mid-file-corrupt fixture. It returns 500 rows,
no error — and **the returned data is silently wrong**. This invalidates the "full drain is the committed
verification statement" design.

---

## Raw test log — `TestSpike251CorruptionSemantics`

```
=== RUN   TestSpike251CorruptionSemantics
    spike_corruption_test.go:90: duckdb engine version: v1.4.5
    spike_corruption_test.go:80: DESCRIBE mid: OK rows=4
    spike_corruption_test.go:68: DESCRIBE trunc: QUERY ERROR: Invalid Input Error: No magic bytes found at end of file '/var/folders/.../trunc.parquet'

        LINE 1: DESCRIBE SELECT * FROM read_parquet('/var/folders/...
                                       ^
    spike_corruption_test.go:80: drain mid: OK rows=500
    spike_corruption_test.go:68: drain trunc: QUERY ERROR: Invalid Input Error: No magic bytes found at end of file '/var/folders/.../trunc.parquet'

        LINE 1: SELECT * FROM read_parquet('/var/folders/...
                              ^
    spike_corruption_test.go:80: count mid: OK rows=1
    spike_corruption_test.go:68: set ignore_errors: QUERY ERROR: Binder Error: Invalid named parameter "ignore_errors" for function read_parquet
        Candidates:
            binary_as_string BOOLEAN
            can_have_nan BOOLEAN
            compression VARCHAR
            debug_use_openssl BOOLEAN
            encryption_config ANY
            explicit_cardinality UBIGINT
            file_row_number BOOLEAN
            filename ANY
            hive_partitioning BOOLEAN
            hive_types ANY
            hive_types_autocast BOOLEAN
            parquet_version VARCHAR
            schema ANY
            union_by_name BOOLEAN

        LINE 1: SELECT COUNT(*) FROM read_parquet(['/var/folders/...
                                     ^
    spike_corruption_test.go:68: set drain ignore_errors: QUERY ERROR: Binder Error: Invalid named parameter "ignore_errors" for function read_parquet
        Candidates:
            (same list as above)

        LINE 1: SELECT * FROM read_parquet(['/var/folders/...
                              ^
    spike_corruption_test.go:80: set plain: OK rows=1000
--- PASS: TestSpike251CorruptionSemantics (0.03s)
PASS
ok  	github.com/lychee-technology/forma/internal/federated	0.564s
```

## Raw test log — `TestSpike251MidFileDiagnostic` (added to explain the surviving drain)

```
=== RUN   TestSpike251MidFileDiagnostic
    spike_corruption_test.go:143: parquet file size = 11723 bytes (500 rows)
    spike_corruption_test.go:146: row groups / codec: row_group_id=0 path_in_schema=row_id     compression=SNAPPY total_compressed_size=8032 data_page_offset=4
    spike_corruption_test.go:146: row groups / codec: row_group_id=0 path_in_schema=changed_at compression=SNAPPY total_compressed_size=2044 data_page_offset=8036
    spike_corruption_test.go:146: row groups / codec: row_group_id=0 path_in_schema=deleted_at compression=SNAPPY total_compressed_size=27   data_page_offset=10080
    spike_corruption_test.go:146: row groups / codec: row_group_id=0 path_in_schema=payload    compression=SNAPPY total_compressed_size=77   data_page_offset=10153
    spike_corruption_test.go:80:  drain @10%: OK rows=500
    spike_corruption_test.go:157: drain @10% payload-integrity: total=500 intact=500 distinct_ts=500
    spike_corruption_test.go:80:  drain @25%: OK rows=500
    spike_corruption_test.go:157: drain @25% payload-integrity: total=500 intact=500 distinct_ts=500
    spike_corruption_test.go:80:  drain @50%: OK rows=500
    spike_corruption_test.go:157: drain @50% payload-integrity: total=500 intact=500 distinct_ts=500
    spike_corruption_test.go:68:  drain @75%: QUERY ERROR: Invalid Input Error: Failed to read file ".../c75.parquet": Snappy decompression failure
    spike_corruption_test.go:157: drain @75% payload-integrity: QUERY ERROR: Invalid Input Error: Failed to read file ".../c75.parquet": Snappy decompression failure
    spike_corruption_test.go:68:  drain @90%: QUERY ERROR: Invalid Error: TProtocolException: Invalid data
    spike_corruption_test.go:157: drain @90% payload-integrity: QUERY ERROR: Invalid Error: TProtocolException: Invalid data
--- PASS: TestSpike251MidFileDiagnostic (0.04s)
```

## Raw test log — `TestSpike251SilentGarbage` (proves the data is wrong, not merely unchecked)

```
=== RUN   TestSpike251SilentGarbage
    spike_corruption_test.go:80:  drain corrupted copy: OK rows=500
    spike_corruption_test.go:181: row_id divergence: orig_rows=500 copy_rows=500 row_ids_lost=5 row_ids_invented=5
--- PASS: TestSpike251SilentGarbage (0.03s)
```

`parquet_metadata()` column list (checked for any page-level integrity signal):

```
file_name, row_group_id, row_group_num_rows, row_group_num_columns, row_group_bytes, column_id,
file_offset, num_values, path_in_schema, type, stats_min, stats_max, stats_null_count,
stats_distinct_count, stats_min_value, stats_max_value, compression, encodings, index_page_offset,
dictionary_page_offset, data_page_offset, total_compressed_size, total_uncompressed_size,
key_value_metadata, bloom_filter_offset, bloom_filter_length, min_is_exact, max_is_exact,
row_group_compressed_bytes, geo_bbox, geo_types
```

**No CRC / checksum column is exposed.**

---

## Q1 — Does `read_parquet(..., ignore_errors := true)` exist? What are its semantics?

**It does not exist.** On DuckDB v1.4.5 via `duckdb-go/v2 v2.5.6`, both the `COUNT(*)` and the drain form fail
at bind time with `Binder Error: Invalid named parameter "ignore_errors" for function read_parquet`, and DuckDB
helpfully enumerates the complete accepted parameter set — `binary_as_string, can_have_nan, compression,
debug_use_openssl, encryption_config, explicit_cardinality, file_row_number, filename, hive_partitioning,
hive_types, hive_types_autocast, parquet_version, schema, union_by_name`. There is no error-tolerance knob of
any kind in that list. `ignore_errors` is a `read_csv` parameter; it was never a `read_parquet` parameter, so
the question of "skips whole file vs. masks rows" is moot for the Parquet reader. This disposes of the issue's
first bullet definitively for the pinned version: the option cannot be adopted, cannot be rejected on
silence grounds, and cannot be relied upon — it simply is not there. Any design that assumed a reader-level
tolerance flag must instead be built out of statements we issue ourselves. (Note this is a *bind-time* error,
so it is cheap and deterministic to detect — but it also means a hypothetical future upgrade that adds the
parameter would silently change behaviour, which argues for not depending on it even if it appears later.)

## Q2 — Assumption A: does `DESCRIBE SELECT * FROM read_parquet(f)` succeed on mid-file corruption?

**Confirmed — Assumption A holds.** `DESCRIBE mid` returned `OK rows=4` (the four columns of the fixture
schema), because the XOR damage sits in a data page while the Thrift footer and the trailing `PAR1` magic are
untouched. The reader only needs the footer to answer a `DESCRIBE`, so it never touches the damaged bytes. The
contrast case behaves as expected in the other direction: `DESCRIBE trunc` fails with `Invalid Input Error: No
magic bytes found at end of file`, since truncation removes the footer entirely. The intended conclusion of
the assumption therefore stands and is in fact stronger than the brief anticipated: a footer-only probe cannot
preflight scenario 7, and — per Q3 — neither can the full drain. A footer probe is a valid detector for the
*truncation* class only.

## Q3 — Assumption B: does a full drain fail on both corruption classes? Does `COUNT(*)` succeed?

**Assumption B is FALSE, and this blocks the plan.** The drain on the mid-file-corrupt file returned
`OK rows=500` — full row count, no query error, no stream error. Only the truncation class failed
(`No magic bytes found at end of file`). `COUNT(*)` on the mid-file class also succeeded (`rows=1`, i.e. one
aggregate row), consistent with the metadata shortcut the brief predicted, but that is now irrelevant because
the drain it was supposed to contrast against does not fail either.

The diagnostic run explains the mechanism and shows this is not a fixture artifact. The 11,723-byte file has
one row group whose column chunks are laid out `row_id` [4, 8036), `changed_at` [8036, 10080), `deleted_at`
[10080, 10107), `payload` [10153, 10230), footer thereafter — all SNAPPY. Corruption at 10%, 25% and 50% of
the file all land inside the `row_id` chunk and are **never detected**; corruption at 75% lands in `changed_at`
and raises `Snappy decompression failure`; at 90% it lands in the footer and raises
`TProtocolException: Invalid data`. Whether damage is caught is therefore a function of *which bytes* are hit,
not of whether the file is corrupt. `row_id` holds random UUIDs, which are incompressible, so Snappy encodes
them as long literal runs — flipping bits inside a literal run yields a still-valid Snappy stream that decodes
to different bytes. `changed_at` is a dense integer sequence with back-references, so the same damage breaks
the stream. Parquet's per-page CRC is optional, DuckDB does not write or verify it here, and
`parquet_metadata()` exposes no checksum column, so there is no integrity signal to fall back on.

The `TestSpike251SilentGarbage` run converts this from "unchecked" to "provably wrong": corrupting a byte-copy
of a single file at the 50% mark yields `orig_rows=500 copy_rows=500 row_ids_lost=5 row_ids_invented=5`. Five
real `row_id` values vanished and five fabricated ones appeared, with the row count preserved and no error
raised. `row_id` is precisely the join key the federated dirty-set anti-join depends on, so this class of
corruption does not merely return bad payloads — it can cause a hot-tier row to fail to mask its stale S3
counterpart (lost id) or introduce a phantom id that matches nothing. Silent wrong answers, not a loud failure.

## Q4 — Does the scan error name the culprit file? (informational)

**Yes, when it errors at all — and in two of the three error shapes.** The truncation error embeds the full
path: `Invalid Input Error: No magic bytes found at end of file '/…/trunc.parquet'`, and the Snappy error does
too: `Invalid Input Error: Failed to read file "/…/c75.parquet": Snappy decompression failure`. The Thrift
footer error is the exception — `Invalid Error: TProtocolException: Invalid data` carries no filename at all,
only the offending SQL line, so a multi-file set that trips this shape gives no attribution. Additionally, the
mixed-set probe `set plain` over `[good, mid]` returned `OK rows=1000` rather than any error, which is just the
Q3 finding again at set level: the corrupt member contributed its 500 rows silently. So error-text attribution
is real but partial, and — more importantly — it is only ever available on the subset of corruptions that
actually raise, which per Q3 is not the subset that matters most.

---

## Decision-gate verdicts

| Assumption | Verdict | Evidence |
|---|---|---|
| **A** — `DESCRIBE` succeeds on mid-file corruption (footer probe cannot preflight scenario 7) | **CONFIRMED** | `DESCRIBE mid: OK rows=4`; `DESCRIBE trunc` fails on missing magic bytes |
| **B** — full drain fails on *both* corruption classes | **REFUTED** | `drain mid: OK rows=500`, plus `row_ids_lost=5 row_ids_invented=5` on a controlled copy |

**GATE STATUS: BLOCKED.** The brief's blocking condition ("if `drain mid` does NOT fail … STOP") is met. The
full drain is not a sound verification statement for the mid-file corruption class, and per the brief no
alternative design was improvised here.

## Notes for re-planning (observations only, not a design)

- The two corruption classes are not symmetric and probably should not share one detector. Truncation/footer
  damage is caught cheaply and loudly by any footer touch (`DESCRIBE`). Mid-file damage is caught only if it
  happens to break the codec stream or the footer Thrift.
- Detection is data-dependent: incompressible columns (UUID `row_id`) are the *most* likely to corrupt silently
  and the *most* consequential, since `row_id` drives the dirty-set anti-join.
- The brief's named fallback of `parquet_metadata()` page-level checks needs scrutiny: the function exposes no
  CRC/checksum column on v1.4.5, only offsets, encodings, and statistics. Per-column aggregates would have the
  same data-dependence problem unless they are checked against an independently stored expected value.
- Writing Parquet with page checksums (and a reader that verifies them) is an upstream/writer-side question,
  not something the read path can retrofit — worth confirming before any re-plan leans on it.
