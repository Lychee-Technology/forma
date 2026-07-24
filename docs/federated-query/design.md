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
| `s3_parquet_path_template` | string | — | Template for locating Parquet files in S3. |
| `allow_partial_degraded_mode` | bool | false | Permit execution with a subset of available tiers. |
| `consistency_mode` | string | `"strict"` | Freshness/availability contract (`"strict"` or `"eventual"`). |
| `include_execution_plan` | bool | false | Attach diagnostic execution plan to the response. |

**Consistency modes:**

* **`strict`** (default): Requires PostgreSQL availability for dirty-set evaluation and hot-tier reads. Federated queries fail when PostgreSQL is unreachable.
* **`eventual`**: Permits S3-only degraded execution when PostgreSQL is unavailable, accepting possible ghost reads (deleted data reappearing) and missing hot-tier rows. Suitable for best-effort analytics where bounded staleness is acceptable.

These controls are part of the request payload; they are not conveyed via HTTP headers.

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