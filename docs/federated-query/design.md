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

* **DirtySet:** The set of row_ids currently present in the PostgreSQL change_log with flushed_at = 0.  
* Any record found in S3 that also exists in the DirtySet is **discarded** immediately during the read phase, regardless of its timestamp.

## **4. Query Translation Layer**

The **Query Translator** is responsible for "Dual-Path Translation" to enable **Predicate Pushdown (Requirement 4.1)**.

### **4.1 Input DSL (JSON)**

```JSON

{  
  "filters": {  
    "l": "and",  
    "c": [  
      { "a": "age", "v": "gt:18" },        // Mapped to entity_main.integer_01  
      { "a": "name", "v": "^:John" },      // Mapped to entity_main.text_01  
      { "a": "tag",  "v": "eq:developer" } // Mapped to eav_data.attr_id=205  
    ]  
  }  
}
```

### **4.2 Translation Output**

The translator must traverse the filter tree and generate two distinct SQL fragments:

**A. PostgreSQL Pushdown Fragment ($PG_WHERE_CLAUSE)**

* **Target:** postgres_scan string argument.  
* **Scope:** Only attributes mapping to entity_main.  
* **Syntax:** Physical Column Names.  
* **Sanitization:** Strict literal escaping to prevent SQL injection.  
* *Example:* (integer_01 > 18 AND text_01 LIKE 'John%')

**B. DuckDB Logical Fragment ($LOGICAL_WHERE_CLAUSE)**

* **Target:** WHERE clauses in CTEs and final projection.  
* **Scope:** All attributes (Main + EAV).  
* **Syntax:** Logical JSON Paths / Parquet Column Names.  
* *Example:* (age > 18 AND name LIKE 'John%' AND tag = 'developer')

## **5. SQL Execution Template**

This SQL template represents the core logic of the Federated Query Engine.

```SQL

-- 1. Configuration  
PRAGMA memory_limit='4GB';  
PRAGMA threads=4;

-- 2. Define Query Parameters (To be interpolated by the Host Application)  
-- $SCHEMA_ID:       Integer (e.g., 1)  
-- $PG_CONN:         String (Postgres Connection String)  
-- $PG_WHERE_CLAUSE: String (Generated Physical SQL for Pushdown)  
-- $S3_PATHS:        List (e.g., ['s3://bucket/base/*.parquet'])

WITH   
-- =========================================================================  
-- CTE 1: The Dirty Set  
-- Identifies records currently in the transaction buffer.  
-- =========================================================================  
dirty_ids AS (  
    SELECT row_id   
    FROM postgres_scan($PG_CONN, 'change_log', 'flushed_at = 0')  
    WHERE schema_id = $SCHEMA_ID  
),

-- =========================================================================  
-- CTE 2: S3 Source (Cold & Warm)  
-- Reads historical data with partition pruning and anti-join.  
-- =========================================================================  
s3_source AS (  
    SELECT   
        row_id,   
        ltbase_created_at AS created_at,  
        ltbase_updated_at AS ver_ts,  
        ltbase_deleted_at AS deleted_ts,  
        -- Logical Columns (Native in Parquet)  
        name,   
        age,   
        tag  
    FROM read_parquet($S3_PATHS)  
    WHERE   
        -- 1. S3 Partition Pruning (via Min/Max stats)  
        (age > 18 AND name LIKE 'John%' AND tag = 'developer')  
        -- 2. Anti-Join: Exclude if a newer version exists in PG  
        AND row_id NOT IN (SELECT row_id FROM dirty_ids)  
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
        MAX(CASE WHEN e.attr_id = 205 THEN e.value_text END) AS tag

    FROM postgres_scan($PG_CONN, 'change_log', 'flushed_at = 0') cl  
      
    -- [Optimization] PUSHDOWN: Filter entity_main INSIDE the scan string  
    JOIN postgres_scan($PG_CONN,   
        'SELECT * FROM entity_main_dev   
         WHERE ltbase_schema_id = ' || $SCHEMA_ID || '   
         AND (' || $PG_WHERE_CLAUSE || ')'   
    ) m   
      ON cl.schema_id = m.ltbase_schema_id AND cl.row_id = m.ltbase_row_id  
        
    LEFT JOIN postgres_scan($PG_CONN, 'eav_data_dev') e   
      ON cl.schema_id = e.schema_id AND cl.row_id = e.row_id  
      
    WHERE cl.schema_id = $SCHEMA_ID  
    GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01  
),

-- =========================================================================  
-- CTE 4: Unified View  
-- =========================================================================  
unified AS (  
    SELECT * FROM s3_source  
    UNION ALL  
    SELECT * FROM pg_source  
)

-- =========================================================================  
-- Final Selection  
-- Deduplication -> Sorting -> Pagination  
-- =========================================================================  
SELECT   
    row_id, name, age, tag, created_at  
FROM unified  
WHERE   
    -- Final Logical Filter check (Ensures PG EAV data matches criteria)  
    (age > 18 AND name LIKE 'John%' AND tag = 'developer')

-- Deduplication (Last-Write-Wins)  
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY ver_ts DESC) = 1

-- Exclude Soft Deletes  
AND (deleted_ts IS NULL OR deleted_ts = 0)

-- Sorting & Pagination  
ORDER BY created_at DESC  
LIMIT $PAGE_SIZE OFFSET $OFFSET;
```

## **6. Optimization Strategies**

### **6.1 Predicate Pushdown (Critical)**

* **Mechanism:** Injecting SQL strings into postgres_scan's second argument.  
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
   * Return HTTP 200 with metadata: {"partial_result": true, "warning": "Historical data unavailable"}.  
2. **PostgreSQL Unavailable:**  
   * Cannot query dirty_ids.  
   * Rewrite query to select *only* from s3_source.  
   * **Risk:** "Ghost Reads" (Deleted data reappearing).  
   * Action: Only permissible if consistency=eventual is set in headers; otherwise return HTTP 503.

## **8. Observability**

The following metrics MUST be emitted to opentelemetry:

* fed_query_latency_histogram: Labeled by {stage: "translation", "execution", "streaming"}.  
* fed_query_row_count: Count of rows returned by S3 vs. PG (helps tune compaction frequency).  
* fed_query_pushdown_efficiency: Ratio of PG_Scan_Rows / Final_Result_Rows. High ratio indicates poor pushdown logic.