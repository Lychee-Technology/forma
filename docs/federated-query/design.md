# **Design Document: Federated Query Engine for Hybrid EAV/OLAP Architecture**

## **1. Executive Summary**

This document defines the architecture for a **Federated Query System** designed to bridge the gap between transactional flexibility (PostgreSQL EAV) and analytical performance (S3 Parquet).

The system utilizes **DuckDB** as a stateless, read-only compute engine to execute **Merge-on-Read** operations. It unifies historical data stored in S3 (Cold/Warm) with real-time buffered data in PostgreSQL (Hot), ensuring sub-second data freshness while leveraging columnar storage performance for complex filtering and sorting.

## **2. System Architecture**

The architecture implements a **Real-Time Lakehouse** pattern. DuckDB acts as the virtualization layer, exposing a unified wide-table view of the underlying disparate storage engines.

### **2.1 Core Components**

1. **Search API / Orchestrator:**  
   * Parses incoming JSON DSL.  
   * Determines query routing (OLTP vs. OLAP vs. Hybrid).  
   * Manages the lifecycle of the DuckDB connection.  
2. **Query Translator:**  
   * Converts JSON filter trees into dialect-specific SQL fragments (DuckDB SQL vs. PostgreSQL SQL).  
   * Manages Schema Mapping (Logical JSON Path $\leftrightarrow$ Physical EAV Columns).  
3. **Compute Engine (DuckDB):**  
   * Embedded, stateless SQL engine.  
   * Extensions: postgres_scanner, httpfs (S3).  
   * Configuration: Read-Only, Memory-Limited.  
4. **Storage Layer:**  
   * **S3 (Parquet):** Flattened, columnar data (Base + Delta files).  
   * **PostgreSQL (Row):** change_log (Buffer pointer), entity_main (Fixed attributes), eav_data (Dynamic attributes).

## **3. Data Consistency Model**

To satisfy **Requirement 1.4 (Last-Write-Wins)** and **Requirement 3.1 (Real-time Integration)**, the system implements a strict **Anti-Join Strategy**.

### **3.1 Data Tiers & Definition**

| Tier | Source | Condition | Characteristics |
| :---- | :---- | :---- | :---- |
| **Hot (L0)** | PostgreSQL | change_log.flushed_at = 0 | Mutable, High IOPS, Row-oriented. |
| **Warm (L1)** | S3 (Delta) | Parquet in /delta/ path | Immutable, Small files, Recent history. |
| **Cold (L2)** | S3 (Base) | Parquet in /base/ path | Immutable, Large files (Compacted). |

### **3.2 The Anti-Join Logic**

Mere timestamp comparison is insufficient due to potential clock skew or race conditions. We treat the PostgreSQL "Dirty Buffer" as the source of truth for record existence.

Formula:

$$Result = (S3_{Data} \notin DirtySet) \cup PG_{HotData}$$

* **DirtySet:** The set of row_ids currently present in the PostgreSQL change_log with flushed_at = 0, widened by the **flush-visibility grace** (#252): rows marked flushed at or after the instant this query resolved its parquet path set (minus the `FlushVisibilityGraceMs` clock-skew margin, default 0) also count as dirty — inclusive, because millisecond stamps cannot order a mark and a path resolution landing in the same tick. Because the flush appends the manifest before marking, a row flushed before path resolution already has its delta listed — the widening therefore catches exactly the rows racing this query (their delta may be missing from the resolved set) and keeps them hot-readable, while the steady state is never widened. The widening renders only in the HasHot tier form: it is safe solely because pg_source serves the discarded rows, so hot-excluded queries keep the strict flushed_at = 0 barrier.
* Any record found in S3 that also exists in the DirtySet is **discarded** immediately during the read phase, regardless of its timestamp.
* **Flush ordering (#252):** the CDC flush publishes `copy tmp→final → manifest-append → mark-flushed`, so a batch is never simultaneously absent from the hot tier and from the manifest-listed delta set. The listed-but-unmarked middle state is resolved by this anti-join: the S3 copies of still-dirty rows are discarded and the hot versions serve.

## **4. Query Translation Layer**

The **Query Translator** is responsible for "Dual-Path Translation" to enable **Predicate Pushdown (Requirement 4.1)**.

### **4.1 Input DSL (JSON)**

The federated query path is activated by including a `"federated"` block on the standard `QueryRequest` payload:

```JSON
{
  "schema_name": "trade",
  "page": 1,
  "items_per_page": 50,
  "condition": {
    "l": "and",
    "c": [
      { "a": "symbol", "v": "eq:AAPL" },
      { "a": "trade_type", "v": "eq:2" }
    ]
  },
  "federated": {
    "enabled": true,
    "preferred_tiers": ["hot", "warm", "cold"],
    "prefer_hot": false,
    "use_main_as_anchor": true,
    "s3_parquet_path_template": "s3://bucket/prefix/{{.SchemaID}}/base/*.parquet, s3://bucket/prefix/{{.SchemaID}}/delta/*.parquet",
    "allow_partial_degraded_mode": true,
    "consistency_mode": "strict",
    "include_execution_plan": true
  }
}
```

When the federated block is absent or `enabled` is false, the query executes on the standard PostgreSQL-only OLTP path.

### **4.2 Translation Output**

The translator must traverse the filter tree and generate two distinct SQL fragments:

**A. PostgreSQL Pushdown Fragment ($PG_WHERE_CLAUSE)**

* **Target:** pg_source `WHERE` clause (DuckDB's postgres scanner pushes it down into the scan).  
* **Scope:** Only attributes mapping to entity_main.  
* **Syntax:** Physical Column Names.  
* **Sanitization:** Strict literal escaping to prevent SQL injection.  
* *Example:* (integer_01 > 18 AND text_01 LIKE 'John%')

**B. DuckDB Logical Fragment ($LOGICAL_WHERE_CLAUSE)**

* **Target:** WHERE clauses in CTEs and final projection.  
* **Scope:** All attributes (Main + EAV).  
* **Syntax:** Logical JSON Paths / Parquet Column Names.  
* *Example:* (age > 18 AND name LIKE 'John%' AND tag = 'developer')

### **4.3 Federated Request Controls**

The `"federated"` object carries optional controls that affect execution routing and failure semantics:

| Field | Type | Default | Description |
| :---- | :---- | :---- | :---- |
| `enabled` | bool | false | Routes the query through the federated (DuckDB/S3) path. |
| `preferred_tiers` | []string | `["hot","warm","cold"]` | Ordered list of data tiers to query. |
| `prefer_hot` | bool | false | Strong preference for Postgres hot tier. |
| `use_main_as_anchor` | bool | true | Use entity_main as the anchor for predicate pushdown. |
| `s3_parquet_path_template` | string | — | Template for locating Parquet files in S3. Wins over the server's manifest-driven resolution (§4.3.1). |
| `allow_partial_degraded_mode` | bool | false | Permit execution with a subset of available tiers. |
| `consistency_mode` | string | `"strict"` | Freshness/availability contract (`"strict"` or `"eventual"`). |
| `include_execution_plan` | bool | false | Attach diagnostic execution plan to the response. |

**Consistency modes:**

* **`strict`** (default): Requires PostgreSQL availability for dirty-set evaluation and hot-tier reads. Federated queries fail when PostgreSQL is unreachable.
* **`eventual`**: Permits S3-only degraded execution when PostgreSQL is unavailable, accepting possible ghost reads (deleted data reappearing) and missing hot-tier rows. Suitable for best-effort analytics where bounded staleness is acceptable.

These controls are part of the request payload; they are not conveyed via HTTP headers.

### **4.3.1 Parquet Path Resolution and Manifest-Driven Reads**

`$S3_PATHS` (§5) is resolved once per query, before the DuckDB template renders. Four levels, first match wins:

1. **Per-request hint** — `federated.s3_parquet_path_template`, rendered for the query's schema (comma-separated templates become a path list). An explicit hint directs `read_parquet` at exactly the requested location and always wins. **A hint that fails to render — or that renders to no usable path at all (`","`, whitespace-only) — is invalid input** (wraps `forma.ErrInvalidInput` → 4xx) and never falls through to the manifest source: silently serving a different path set than the caller asked for would misreport the answer.
2. **Manifest source** — when the server is configured for manifest reads, the schema's manifest is loaded and its file entries become the scan set as full `s3://` URIs. Reads then scan exactly the listed objects rather than expanding a storage glob, which is what makes cold-tier loss detectable (§4.3.1 *Inconsistency detection*).
3. **Fallback glob** — a schema whose manifest is missing or empty resolves to `s3://{s3Bucket}/{s3DataPrefix}/{schemaID}/*.parquet`, preserving pre-manifest read behavior for never-flushed schemas. A trailing `/` on `s3DataPrefix` is stripped exactly as the writers strip it (`internal/cdc.BuildDeltaPath`), so `"delta/"` and `"delta"` address the same objects; a prefix of `/` alone disables the level like an empty one. The single `*` does not cross `/`, so in-flight `_tmp/` staging objects stay excluded; it must never be widened to `**`. An empty `s3DataPrefix` disables this level. **The fallback is not a general recovery path for schemas that already have data:** a schema whose parquet exists but whose manifest is empty or absent (writers previously ran with manifests disabled, or the manifest was lost) takes this same glob, and the glob spans exactly one prefix — the configured `s3DataPrefix`. The CLI tools do not share one root by default (`cdc-init --s3-prefix base`, `cdc-flush --s3-prefix delta`, `compactor --data-prefix data`), so objects written under any root other than the reader's `s3DataPrefix` are simply not scanned and their rows go missing without an error. Repair the manifest (`forma-tools manifest-reconcile`, or re-run `cdc-init`) rather than relying on the glob to find the data.
4. **No paths — the read fails fast, with its own classification.** With no hint, no configured source, and no fallback, the path set resolves empty and the engine rejects the query at resolution with `ErrNoParquetPaths` (`NoParquetPathsError{SchemaID, SourceConfigured}` — see `docs/error-handling.md`), before rendering, before the pre-read schema validator, and before DuckDB is touched. It is **not degradable**: `allow_partial_degraded_mode` does not absorb it, because every query reaching the DuckDB engine wants warm and/or cold data (hot-only and `prefer_hot` short-circuit to Postgres earlier), so a Postgres-only answer would be silently short exactly where the cold tier was requested. The message names the schema and distinguishes "no source configured" from "source consulted, manifest empty, fallback disabled", which have different remedies.

   Note that an empty path set is not a normal state: a never-flushed schema takes the level-3 fallback glob, so reaching level 4 means the fallback is disabled too. Before #299 this case rendered `read_parquet(<no value>)` and died as a DuckDB parser error classified `ErrFederatedReadFailed` — loud, but indistinguishable from a transient S3 outage to any programmatic discriminator, so degraded mode converted a configuration mistake into a quietly incomplete answer. The renderer now also refuses to render an unbound `$S3_PATHS` at all (`sqlgen.requireS3Paths`), so no caller can reconstruct that statement.

**Server configuration.** Manifest-driven reads are configured on `DuckDBConfig` and, for `cmd/server`, from the environment. The `DUCKDB_*` names override the shared names in parentheses; those shared names are **reserved as the single-stack configuration point** — nothing reads them today (the CDC tools take `--manifest-prefix` / `--manifest-template` flags), and they exist so a future runner can drive writer and reader from one value:

| Config field | Env var | Meaning |
| :---- | :---- | :---- |
| `duckdb.s3Bucket` | `DUCKDB_S3_BUCKET` (`S3_BUCKET`) | Bucket holding both the parquet objects and the manifests that index them. Required whenever `manifestTemplate` is set. |
| `duckdb.s3DataPrefix` | `DUCKDB_S3_PREFIX` (`S3_PREFIX`) | Mirrors the CDC write side's parquet prefix. Used *only* to build the level-3 fallback glob. |
| `duckdb.manifestPrefix` | `DUCKDB_MANIFEST_PREFIX` (`MANIFEST_PREFIX`) | Root prefix for manifest objects. Must match the CDC/compaction write side. |
| `duckdb.manifestTemplate` | `DUCKDB_MANIFEST_TEMPLATE` (`MANIFEST_TEMPLATE`) | Per-schema manifest path template, e.g. `manifest/{{.SchemaID}}.json`. **Non-empty is the single enable gate** for manifest reads. |

* **Env vars are read only when `DUCKDB_ENABLED` is truthy:** with DuckDB off, `cmd/server` returns the base config untouched, so setting `DUCKDB_MANIFEST_TEMPLATE` alone is a silent no-op — not a startup failure and not a behavior change.
* **Gate:** the source is built only when `duckdb.enabled` *and* `manifestTemplate` is non-empty. All four fields default to empty, so an upgrade never flips an existing deployment from glob reads to manifest reads on its own.
* **Shared prefix names are all-or-nothing with the template.** `S3_PREFIX` and `MANIFEST_PREFIX` are adopted **only when the effective `manifestTemplate` (resolved first, from `DUCKDB_MANIFEST_TEMPLATE` then `MANIFEST_TEMPLATE`) is non-empty**; otherwise they are ignored and the field stays at its base value. Without this, a stack that already exports `S3_PREFIX` for its CDC tooling and merely sets `DUCKDB_ENABLED=1` would produce a prefix-without-template config — precisely the inert combination the startup validation rejects — and would stop booting on upgrade. `S3_BUCKET` carries no such condition: a bucket alone is never inert. The explicit `DUCKDB_S3_PREFIX` / `DUCKDB_MANIFEST_PREFIX` names bypass the condition and are **always** adopted, so an operator who names one by hand without a template gets the startup rejection rather than a silently dropped value.
* **Must match the writer:** `manifestPrefix` / `manifestTemplate` must be identical to the CDC and compaction write side, and `s3DataPrefix` must match the writers' parquet prefix. A reader pointed at a manifest path the writers never use resolves an empty manifest for every schema and falls back to the glob — or, without `s3DataPrefix`, resolves no paths at all — then every read that *reaches the DuckDB engine* fails per level 4 with `ErrNoParquetPaths`, while PG-routed reads (hybrid routing's hot-only and small-page cases, which never render the DuckDB template) keep returning hot-tier-only rows silently. A level-4 misconfiguration is therefore loud only for the queries that were going to touch the cold tier. **The template is probe-validated at startup** so the most common way to point the reader at a path the writers never use — a field-name typo such as `{{.SchemaId}}`, which parses but renders `<no value>` for every schema — is rejected before the server boots, along with any template that does not vary by schema.
* **Invalid configuration fails at startup**, and the failure is fatal to server construction (a half-configured read surface would silently drop the cold tier). Where exactly it fails differs by entry point: `cmd/server` resolves and validates the DuckDB config as its first act, **before it opens any connection**, so a bad value never reaches the database; programmatic callers of `factory.NewEntityManagerWithConfigContext` are validated at the top of the factory, **before the factory's own I/O** (table collection, metadata load) but after they have built the pool they pass in. Rejected combinations: `manifestTemplate` set without `s3Bucket`; `manifestPrefix` or `s3DataPrefix` set without `manifestTemplate` (they would sit inert while reading to an operator as "manifest reads are on"); a `manifestTemplate` that is not a parsable `text/template`; **any of the four fields carrying leading or trailing whitespace** — the write side never trims them, so a padded value would resolve a different object key on each side, and the divergence would surface only as a silently empty cold tier (byte parity with the writer is the contract, not reader-side normalization).
* **Endpoint addressing:** when `s3Endpoint` is set, the manifest client uses path-style addressing, mirroring the `s3_url_style='path'` DuckDB httpfs receives — both must address the same objects the same way.
* **Boundary — no startup bucket probe:** the S3 client is constructed at startup but not exercised (no `HeadBucket`). A mistyped bucket, a wrong endpoint, or bad credentials therefore surface as **query-time** read failures, not as a startup failure.

**Inconsistency detection.** `ErrParquetSetInconsistent` (see `docs/error-handling.md`) can only ever trigger on **level 2** paths. A failed read is classified by probing the exact scanned set for listed-but-absent objects; that classification is skipped when the paths came from a per-request hint or when no source is configured, and the probe additionally skips glob entries (a glob's absence is unprovable), so a level-3 fallback can never classify. A deployment reading via globs alone therefore keeps the pre-manifest failure mode — a shrinking glob silently returns fewer rows.

**Migration note — the silent-loss window.** The write side is already on for most deployments: both `forma-tools cdc-flush` and `forma-tools compactor` default `--manifest-template` to `manifest/{{.SchemaID}}.json`. CDC gates manifest writing on that template being non-empty; compaction does not gate at all — its manifest provider always loads and saves. Either way, a stack running the CLI tools with default flags **has been writing manifests all along**. The read side is off by default. Upgrading without setting `DUCKDB_MANIFEST_TEMPLATE` (plus `DUCKDB_S3_BUCKET`) is therefore not a no-op-by-choice: the deployment stays in the pre-existing window where a lost cold-tier object degrades to a silently short result set instead of a loud, classified error. Turn the read side on with the same prefix/template values the flusher uses.

An upgrade is nevertheless boot-safe in both directions. Setting `DUCKDB_ENABLED=1` with only the shared CDC vars exported (the single-stack `.env` shape) leaves both prefix fields empty per the all-or-nothing rule, so the config validates and the server starts with the pre-existing glob read path. Setting `DUCKDB_MANIFEST_TEMPLATE` is what flips the deployment to manifest reads, and it pulls the shared prefixes in with it — one variable, one behavior change, no half state.

### **4.4 Attribute → Column Naming Contract**

Attribute names are logical and may contain dots (nested JSON objects flatten
to `contact.annualIncome`). Physical parquet columns and every unified-CTE
column use the folded form produced by `sqlgen.ParquetAttrColumn` (dots and
spaces → underscores, backticks/brackets stripped): `contact_annualIncome`.

The fold is shared by the CDC exporter (write side) and the federated SQL
generator (read side) and cannot diverge: the logical WHERE clause is applied
both against raw `read_parquet` output (physical columns) and against the
`visible` CTE (unified columns), so both must expose the same names (#260).
The fold is lossy, so both sides fail fast if two attributes of one schema
collide on the same folded column. Keyset cursors only accept system columns
and never carry attribute names.

## **5. SQL Execution Template**

This SQL template represents the core logic of the Federated Query Engine.

It is a simplified sketch of the runtime template (`internal/sqlgen/advanced_query_template_duckdb.go`) and is kept executable: `internal/federated/design_doc_sql_test.go` extracts this block, runs it on DuckDB, and pins its scan shapes, source aliases, and LWW/filter semantics — update that test when editing this section.

```SQL

-- 1. Configuration  
PRAGMA memory_limit='4GB';  
PRAGMA threads=4;

-- 2. Define Query Parameters (To be interpolated by the Host Application)  
-- $SCHEMA_ID:       Integer (e.g., 1)  
-- $PG_CONN:         String (Postgres Connection String)  
-- $PG_WHERE_CLAUSE: String (Generated Physical SQL for Pushdown)  
-- $S3_PATHS:        List (e.g., ['s3://bucket/base/*.parquet'])
-- $FLUSH_GRACE_CUTOFF_MS: BIGINT (the instant this query resolved $S3_PATHS,
--                    minus the clock-skew margin, #252; MaxInt64 disables
--                    the widening — hot-excluded renders omit it entirely)

WITH   
-- =========================================================================  
-- CTE 1: The Dirty Set  
-- Identifies records currently in the transaction buffer.  
-- =========================================================================  
dirty_ids AS (  
    SELECT row_id   
    FROM postgres_scan($PG_CONN, 'public', 'change_log')  
    WHERE schema_id = $SCHEMA_ID  
        AND (flushed_at = 0 OR flushed_at >= $FLUSH_GRACE_CUTOFF_MS)
),

-- =========================================================================  
-- CTE 2: S3 Source (Cold & Warm)  
-- Reads historical data with the dirty-set anti-join and semijoin pushdown.  
-- =========================================================================  
s3_source AS (  
    SELECT   
        row_id,   
        changed_at AS created_at,  
        changed_at AS ver_ts,  
        deleted_at AS deleted_ts,  
        -- Logical Columns (Native in Parquet)  
        name,   
        age,   
        tag,  
        1 AS source_tier_priority  
    -- union_by_name resolves the schema UNION across parquet generations  
    -- (#189): files written before an attribute existed contribute NULL,  
    -- and same-named columns widen to the common supertype. Corruption  
    -- loudness is preserved by the pre-read system-column invariant  
    -- validator (internal/federated/parquet_schema_validation.go).  
    FROM read_parquet($S3_PATHS, union_by_name=true)  
    WHERE   
        -- 1. Anti-Join: Exclude if a newer version exists in PG  
        row_id NOT IN (SELECT row_id FROM dirty_ids)  
        -- 2. Predicate pushdown as a row_id SEMIJOIN: a row qualifies when  
        --    ANY of its parquet versions matches; ALL of its versions then  
        --    enter dedup so the latest wins BEFORE the real filter below.  
        --    Filtering versions directly here drops newer non-matching  
        --    versions pre-dedup and resurrects stale rows (#173/#178).  
        AND row_id IN (  
            SELECT row_id FROM read_parquet($S3_PATHS, union_by_name=true)  
            WHERE (age > 18 AND name LIKE 'John%' AND tag = 'developer')  
        )  
),

-- =========================================================================  
-- CTE 3: PostgreSQL Source (Hot)  
-- Performs Dynamic Pivoting and Predicate Pushdown.  
-- =========================================================================  
pg_source AS (  
    SELECT   
        m.ltbase_row_id AS row_id,  
        m.ltbase_created_at AS created_at,  
        cl.changed_at AS ver_ts,   
        cl.deleted_at AS deleted_ts,  
          
        -- [Type Casting] MANDATORY: Cast PG types to match Parquet Schema  
        CAST(m.text_01 AS VARCHAR) AS name,  
        CAST(m.integer_01 AS INTEGER) AS age,  
          
        -- [EAV Pivot] Aggregation for dynamic attributes  
        -- Note: EAV filtering is done in the WHERE clause below, not pushed to EAV scan  
        MAX(CASE WHEN e.attr_id = 205 THEN e.value_text END) AS tag,  
        3 AS source_tier_priority

    FROM postgres_scan($PG_CONN, 'public', 'change_log') cl  
      
    -- [Optimization] PUSHDOWN: $PG_WHERE_CLAUSE is a plain predicate in the  
    -- WHERE clause below; DuckDB's postgres scanner pushes it down to  
    -- PostgreSQL so entity_main is filtered by PG indexes, not in DuckDB.  
    JOIN postgres_scan($PG_CONN, 'public', 'entity_main_dev') m   
      ON cl.schema_id = m.ltbase_schema_id AND cl.row_id = m.ltbase_row_id  
        
    LEFT JOIN postgres_scan($PG_CONN, 'public', 'eav_data_dev') e   
      ON cl.schema_id = e.schema_id AND cl.row_id = e.row_id  
      
    WHERE cl.schema_id = $SCHEMA_ID  
        AND (cl.flushed_at = 0 OR cl.flushed_at >= $FLUSH_GRACE_CUTOFF_MS)
        AND m.ltbase_schema_id = $SCHEMA_ID  
        AND ($PG_WHERE_CLAUSE)  
    GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01  
),

-- =========================================================================  
-- CTE 4: Unified View  
-- =========================================================================  
unified AS (  
    SELECT * FROM s3_source  
    UNION ALL  
    SELECT * FROM pg_source  
),

-- =========================================================================  
-- CTE 5: Ranked (Last-Write-Wins Deduplication)  
-- =========================================================================  
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY row_id
            ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
        ) AS rn
    FROM unified
)

SELECT
    row_id, name, age, tag, created_at
FROM ranked
WHERE rn = 1
    -- Exclude Soft Deletes (the tombstone must WIN dedup first, then be dropped)
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    -- Final logical filter, applied to the LWW WINNER only (#173/#178):
    -- a newer non-matching version must never expose an older matching one.
    AND (age > 18 AND name LIKE 'John%' AND tag = 'developer')

-- Sorting & Pagination
-- A trailing row_id ASC tiebreak gives equal-key rows a total order, so
-- LIMIT/OFFSET page windows stay stable across requests (#183). This mirrors
-- buildNonKeysetOrderBy, the PG optimized template's trailing m.ltbase_row_id,
-- and the production-harness oracle.
ORDER BY created_at DESC, row_id ASC
LIMIT $PAGE_SIZE OFFSET $OFFSET;
```

Keyset (cursor) pagination carries the same total-order requirement, and it is
now **enforced**, not merely documented: the engine rejects any cursor whose
final column is not `row_id` (`validateKeysetTiebreak`, guarding both the live
renderer path in `DBFederatedQueryEngine.Query` and the `KeysetEnabled`
`executeFederatedKeysetQuery` seam). A cursor ending on a non-unique key applies
a strict inequality on that key at the boundary, which silently skips every row
tied there; the trailing `row_id` gives the composite key a unique tiebreak so
each boundary tie is resolvable (#183).

**Never-flushed columns (#255).** `union_by_name` can only union columns that
exist in *some* file. An attribute added to the schema before its first flush is
absent from the entire scan set, so the scan source (both `s3_source` and the
semijoin) is wrapped as
`(SELECT *, NULL::<type> AS <col> … FROM read_parquet(…)) AS cold_scan`,
projecting each such column as a typed NULL — computed per query from the
pre-read validator's footer-column union, and only when that union is complete
(an incomplete union falls back to the unaugmented scan and today's loud
classified failure). The missing-column set participates in the compiled-plan
scope hash, so a skeleton compiled while a column was cold-absent is re-keyed the
moment the first flush lands it (the plan-cache poisoning hazard the issue
mandates addressing). With no missing columns the rendered SQL is byte-identical
to this document's §5 sketch. One residual race is accepted: for glob-hint path
sets the validator's listing and the scan's listing are separate S3 LISTs, so a
flush landing between them can collide the NULL alias with the newly-landed real
column — a one-time loud classified failure that self-heals on the next query,
since the missing set is recomputed per query.

## **6. Optimization Strategies**

### **6.1 Predicate Pushdown (Critical)**

* **Mechanism:** `$PG_WHERE_CLAUSE` is rendered as a plain predicate in the pg_source `WHERE` clause; DuckDB's postgres scanner pushes supported predicates down into the PostgreSQL scan.  
* **Rationale:** entity_main may contain millions of rows. Pulling all rows to DuckDB for filtering is unacceptable. Pushdown leverages PostgreSQL indexes.  
* **Limitation:** Only applicable to entity_main columns. EAV columns and complex functions must be filtered in DuckDB memory after the join.

### **6.2 Streaming Result Processing**

* **Requirement:** 4.2 (Memory Management).  
* **Implementation:** The Go/Java application **MUST NOT** load the full DuckDB result set into a slice/array.  
* **Pattern:** Use database/sql (Go) or JDBC ResultSet iterator patterns to stream row-by-row JSON serialization to the HTTP response writer.

### **6.3 Smart Type Casting**

* PostgreSQL numeric $\rightarrow$ DuckDB DOUBLE (Precision loss acceptable for search, not for finance).  
* PostgreSQL smallint $\rightarrow$ DuckDB INTEGER or BIGINT (Safe).  
* PostgreSQL text (containing UUID) $\rightarrow$ DuckDB UUID (Explicit cast required).

## **7. Resilience and Error Handling**

### **7.1 Circuit Breaker**

* **Trigger:** 5 consecutive failures (Timeout or OOM) within 30 seconds.  
* **Action:** Immediately fail requests with storage=['olap']. Fallback to storage=['oltp'] (Postgres only) if allowed by the request.

### **7.2 Degraded Modes**

1. **S3 Unavailable:**  
   * Log Error.  
   * Rewrite query to select *only* from pg_source.  
   * Return HTTP 200 with metadata: `{"partial_result": true, "warning": "Historical data unavailable"}`.  
2. **PostgreSQL Unavailable:**  
   * Cannot query dirty_ids or hot-tier rows.  
   * Rewrite query to select *only* from s3_source.  
   * **Risk:** "Ghost Reads" (Deleted data reappearing) and missing unflushed hot rows.  
   * Action: Only permissible if `federated.consistency_mode = "eventual"` is set on the request; otherwise return HTTP 503.  
   * S3-only responses carry metadata: `{"partial_result": true, "warning": "Hot-tier data unavailable, results may be stale"}`, plus an `X-Forma-Consistency: eventual` HTTP warning header.

### **7.3 Partial-Read Resilience (#251)**

One unreadable parquet object no longer fails a whole manifest-authored scan. The mechanism is **verify-and-exclude**, and it runs only on the failure path — a healthy query pays nothing.

**Mechanism chain.** A DuckDB read failure (at `Query` time or mid-stream — listed objects are opened lazily, so either is possible) enters `failDuckDBScan`, which resolves it in a fixed order:

1. **Missing-object classification first.** `classifyDuckDBReadError` probes the exact scanned set via `ParquetSource.MissingIn`. A listed-but-absent object is manifest **inconsistency** (§4.3.1, #187 scenario 2) — non-degradable, breaker-worthy, **never retried**. Missing is not corrupt, and this ordering is a contract: a deleted object must never be quietly excluded as "unreadable".
2. **Per-file verification.** Otherwise the engine re-reads each object of the failed set individually (`verifyParquetPaths` → `SELECT * FROM read_parquet('<one object>')`, drained to exhaustion). Objects whose solo drain fails are the confirmed-corrupt set. Verification only runs for source-authored sets of **two or more** objects; glob and quote-bearing entries are skipped (unverifiable means unexcludable), and a cancelled context confirms nothing, since cancellation is indistinguishable from corruption for the paths not yet drained.
3. **TTL cache.** Confirmed-corrupt objects are recorded in an engine-level `corruptParquetCache` with a bounded lifetime — default 5 minutes, overridable with `WithCorruptPathRetention`. Entries always expire: a terminal verdict must never be memoized forever, because repair, compaction retiring the key, or a manifest reconcile can only self-heal through re-verification after expiry.
4. **Resolve-time exclusion.** `resolveParquetPaths` filters the source-authored set through the cache before the template renders, and reports what it dropped.
5. **One retry.** `ExecuteDuckDBFederatedQuery` retries the query exactly once against the readable remainder. A second retryable failure surfaces to the caller: corruption appearing mid-flight is indistinguishable from a sick store and must not loop.

**Why a drain, not a footer probe.** DuckDB answers `DESCRIBE` and `COUNT(*)` from the Thrift footer and row-group metadata without touching data pages, so a footer probe passes cleanly on a page-corrupt object — the pre-read schema validator already footer-probes, and scenario 7 still failed. Only a drain reads the pages. A **solo drain reads a superset of the columns the scan projects**, so whatever per-file failure made the set scan fail is deterministically reproduced by the per-file pass; there is no failure that the set scan can detect and the verification pass cannot. That superset property is what makes attribution sound: an object is excluded because it was proven unreadable on its own, not because it was in the set when something went wrong.

**Why not `ignore_errors`.** It does not exist. On the pinned engine (DuckDB **v1.4.5** via `github.com/duckdb/duckdb-go/v2`) `read_parquet(..., ignore_errors := true)` fails at bind time with `Binder Error: Invalid named parameter "ignore_errors" for function read_parquet`, and DuckDB enumerates the complete accepted parameter list — it contains no error-tolerance knob of any kind (`ignore_errors` is a `read_csv` parameter). The option therefore cannot be adopted today, and it would be rejected even if a later version added it: a reader-level skip is **silent** — no execution-plan marker, no classification, no per-object attribution — which recreates exactly the scenario-2 silent-loss class this subsystem exists to prevent. Depending on a flag whose appearance in a future upgrade would silently change read semantics is the wrong shape of dependency. See `docs/superpowers/plans/2026-08-02-issue-251-spike-findings.md` for the raw evidence.

**Loudness contract.** A partial answer is never quiet. Path resolution records `NotePartialParquetExclusion` on the internal execution plan, naming every excluded object, and the exported constant — not a retyped literal — is what tests assert on (#197). Notes remain **embedder-only by design**: `toExecutionPlan` projects `Sources` and drops `Notes` at the HTTP boundary (#301/#306), so object keys never reach an API caller. Because Notes cannot explain anything to an HTTP consumer, the plan must be truthful by construction rather than by annotation: `ExecuteDuckDBFederatedQuery` snapshots the plan's `Sources`/`Notes` length before the first pass (`markExecutionPlan`) and truncates back to that mark (`rewind`) before retrying, so the attached plan describes only the pass that actually produced the returned page. Without the rewind a successful retry would report both passes — two identically-labelled DuckDB scans, the failed one carrying `ActualRows=0` and therefore indistinguishable from a scan that legitimately matched nothing, plus a double-counted hot-tier row estimate. Everything recorded *before* the first pass survives the rewind: the routing decision and its note are the caller's, not the failed pass's. The retry re-records the exclusion note itself, through path resolution.

**Breaker contract.** Confirmed corruption is the one read failure that is **not** engine sickness, so it calls `ReleaseProbe` — handing back a half-open probe reservation — instead of `RecordFailure`. The justification is empirical rather than assumed: the verification pass just read every *other* object of the set through the same engine, the same session, and the same store, which is a live proof of health. Without this, a permanently corrupt object would drive the breaker open on every query and hold the DuckDB route off indefinitely. **Store-wide unreadability still records a failure:** if verification finds that *every* object in the set fails (`len(corrupt) >= len(paths)`), the store or engine is sick, not the files, so nothing is cached, nothing is excluded, and the failure takes the ordinary `RecordFailure` path with its original classification. Missing-object inconsistency (branch 1 above) likewise still records a failure.

**Scope.** Exclusion applies only to **level-2, source-authored** path sets (§4.3.1):

* **Hint-authored sets keep all-or-nothing.** An explicit `federated.s3_parquet_path_template` means the operator pinned the set; silently scanning a subset of a pinned set would misreport the answer for the same reason a hint never falls through to the manifest source.
* **Glob sets keep all-or-nothing.** Exclusion is inexpressible in a glob — you cannot write "this prefix minus that object" — so the level-3 fallback keeps today's behavior, consistent with it never being able to classify inconsistency either.
* **Excluding everything is forbidden.** If the cache would empty the set, resolution passes the *full* set through unfiltered. Total corruption must fail loudly with its own classification, not decay into a quiet `ErrNoParquetPaths` misconfiguration.
* **Plan-cache safety is free.** The resolved path set is already part of the `scope-v2` cache key (`internal/federated/duckdb_query_build.go`, #255), because the paths render into the skeleton as SQL literals. Removing an object changes the key, so the excluded-set query compiles its own plan and can never reuse a plan built over the corrupt object.

**Interaction — a cached object escapes the missing-object probe until the entry expires.** `MissingIn` probes the *scanned* set, and an excluded object is by definition not in it. So if a confirmed-corrupt object is subsequently deleted from storage while its cache entry is live, that deletion does not classify as manifest inconsistency during the retention window; it surfaces only after the entry expires and the object is scanned again. This is a bounded, deliberate consequence of the TTL: the window is `WithCorruptPathRetention` (default 5 minutes), and it is why the retention is bounded at all. The rows of a corrupt object are already absent from the answer either way, so the exposure is a delay in *classification*, not a new class of silent loss.

**Retention interplay.** This is what makes bounded PostgreSQL retention survivable. Before #251, a single unreadable cold object failed the entire federated scan, and the only remaining answer was the §7.2 degraded fallback to Postgres alone — complete only for as long as Postgres still happened to hold everything that had been flushed. Deployments were relying on that coincidence: shortening hot-tier retention would have converted one corrupt object from "degraded but complete" into "degraded and silently short". With verify-and-exclude, the query is answered from the readable parquet remainder **plus** the hot tier, so completeness no longer depends on how much history Postgres retains, and the answer's shortfall is exactly the corrupt object's rows — named on the plan.

**Known limitation — silent mis-decode (pre-existing, out of scope).** DuckDB v1.4.5 neither writes nor verifies Parquet page checksums, and `parquet_metadata()` exposes no CRC column, so byte corruption inside an incompressible column chunk can decode into *different valid values* with no error at all. The spike proved this concretely: a 64-byte XOR at the 50% offset of a 500-row file returned 500 rows with **5 `row_id` values lost and 5 fabricated**, zero errors raised. A mis-decoded `row_id` defeats the dirty-set anti-join (§3.2), so a stale S3 row can escape masking by its hot version. Whether damage is caught at all is a function of *which bytes* are hit — the same corruption landing in a delta-encoded integer column raises `Snappy decompression failure`, and in the footer raises a Thrift error. This class is invisible to **every** reader, including the all-or-nothing scan that preceded #251, so it is neither introduced nor worsened here; #251 covers scan-detectable failures only. The gap is tracked in #347; candidate directions are writer-side page CRCs, manifest-level content checksums, and reader-side verification once DuckDB exposes checksum checking.

## **8. Observability**

The following metrics MUST be emitted to opentelemetry:

* `fed_query_latency_histogram`: Labeled by `{stage: "translation", "execution", "streaming"}`.  
* `fed_query_row_count`: Count of rows returned by S3 vs. PG (helps tune compaction frequency).  
* `fed_query_pushdown_efficiency`: Ratio of PG_Scan_Rows / Final_Result_Rows. High ratio indicates poor pushdown logic.

The execution plan and response metadata MUST include:

* `consistency_mode`: The requested freshness contract (`strict` or `eventual`).  
* `degraded_mode`: Boolean indicating whether results are partial due to a degraded data source.  
* `circuit_breaker_state`: Current breaker state (`closed`, `open`, `half_open`) when relevant.  
* `source_availability`: Per-source status snapshot (PG available, S3 available).  
* `warning`: Human-readable warning when results are partial or consistency is reduced.