# Federated Query Engine: Detailed Design Document

## 1. Executive Summary

This document defines the architecture for a **Federated Query System** designed to bridge the gap between transactional flexibility (PostgreSQL EAV) and analytical performance (S3 Parquet). The system implements a **dual-track query strategy**:

- **Simple Queries**: Execute directly on PostgreSQL with full pagination support
- **Advanced Queries**: Utilize DuckDB as a stateless compute engine to perform **Merge-on-Read** operations across three data tiers

The design leverages bounded data volume constraints (Hot ≤ 10K rows, Warm ≤ 240K rows) to simplify federated query execution while maintaining sub-second data freshness.

---

## 2. System Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Request                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Query Router                                    │
│  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────────┐  │
│  │ Query Classifier │───▶│ Simple Query?    │───▶│ Route to PostgreSQL   │  │
│  │                 │    │ (AND-only, indexed│    │ Direct Execution      │  │
│  │                 │    │  sort column)     │    └───────────────────────┘  │
│  │                 │    └──────────────────┘                                │
│  │                 │              │ No                                      │
│  │                 │              ▼                                         │
│  │                 │    ┌───────────────────────┐                          │
│  │                 │    │ Route to Federated    │                          │
│  │                 │    │ Query Engine          │                          │
│  └─────────────────┘    └───────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                 ┌────────────────────┴────────────────────┐
                 │                                         │
                 ▼                                         ▼
┌────────────────────────────────┐       ┌────────────────────────────────────┐
│     Simple Query Path          │       │      Advanced Query Path           │
│                                │       │                                    │
│  ┌──────────────────────────┐  │       │  ┌──────────────────────────────┐  │
│  │      PostgreSQL          │  │       │  │         DuckDB               │  │
│  │   (Direct Execution)     │  │       │  │   (Federated Compute)        │  │
│  │                          │  │       │  │                              │  │
│  │  • entity_main           │  │       │  │  ┌────────────────────────┐  │  │
│  │  • eav_data              │  │       │  │  │ postgres_scanner       │  │  │
│  │  • B-tree indexes        │  │       │  │  │ (Hot: PG change_log)   │  │  │
│  │                          │  │       │  │  └────────────────────────┘  │  │
│  │  Features:               │  │       │  │  ┌────────────────────────┐  │  │
│  │  • OFFSET pagination     │  │       │  │  │ httpfs / S3            │  │  │
│  │  • Exact COUNT           │  │       │  │  │ (Warm: Delta Parquet)  │  │  │
│  │  • Random page jump      │  │       │  │  │ (Cold: Base Parquet)   │  │  │
│  └──────────────────────────┘  │       │  │  └────────────────────────┘  │  │
│                                │       │  └──────────────────────────────┘  │
│                                │       │                                    │
│                                │       │  Features:                         │
│                                │       │  • Keyset pagination only          │
│                                │       │  • Estimated COUNT                 │
│                                │       │  • Two-phase merge                 │
└────────────────────────────────┘       └────────────────────────────────────┘
```

### 2.2 Core Components

| Component | Responsibility |
|-----------|----------------|
| **Query Router** | Parses incoming requests, classifies queries, determines execution path |
| **Query Translator** | Converts JSON DSL to SQL (PostgreSQL or DuckDB dialect) |
| **Simple Query Executor** | Executes queries directly on PostgreSQL |
| **Federated Query Engine** | Orchestrates two-phase merge across data tiers using DuckDB |
| **Result Processor** | Formats responses, handles streaming, computes pagination metadata |

### 2.3 Data Tier Definitions

| Tier | Source | Storage Format | Max Records | Characteristics |
|------|--------|----------------|-------------|-----------------|
| **Hot (L0)** | PostgreSQL `change_log` | Row-oriented (EAV) | 10,000 | Mutable, unflushed changes |
| **Warm (L1)** | S3 Delta Parquet | Columnar (Wide Table) | 240,000 | Immutable, recent flushes |
| **Cold (L2)** | S3 Base Parquet | Columnar (Wide Table) | Unbounded | Immutable, compacted history |

**Critical Constraint:** The bounded volumes of Hot and Warm tiers (≤ 250K total) enable full-scan strategies that simplify query execution without sacrificing performance.

---

## 3. Query Classification and Routing

### 3.1 Classification Rules

```python
def classify_query(query: Query) -> QueryType:
    """
    Classify a query as SIMPLE or ADVANCED based on its characteristics.
    """
    # Rule 1: Check filter logic
    if has_or_conditions(query.filters) or has_nested_groups(query.filters):
        return QueryType.ADVANCED
    
    # Rule 2: Check sort column
    if not is_indexed_column(query.sort_by):
        return QueryType.ADVANCED
    
    # Rule 3: Check EAV condition count
    if count_eav_conditions(query.filters) > 5:
        return QueryType.ADVANCED
    
    # Rule 4: Check for cross-entity joins
    if requires_cross_entity_join(query):
        return QueryType.ADVANCED
    
    return QueryType.SIMPLE
```

### 3.2 Routing Decision Matrix

| Condition | Simple Query | Advanced Query |
|-----------|--------------|----------------|
| Filter logic | AND only | AND, OR, nested |
| Sort column | Indexed (main or EAV) | Any column |
| EAV conditions | ≤ 5 | Unlimited |
| Cross-entity joins | No | Yes |
| Pagination | OFFSET (≤ 10K) | Keyset only |
| Total count | Exact (≤ 10K results) | Estimated |

### 3.3 Query Hint Override

Clients may force routing via the `query_hint` parameter:

```json
{
  "filters": { ... },
  "sort": { ... },
  "query_hint": "force_simple" | "force_advanced"
}
```

---

## 4. Simple Query Path

### 4.1 Execution Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Parse     │────▶│  Translate  │────▶│   Execute   │────▶│   Format    │
│   Request   │     │   to SQL    │     │   on PG     │     │   Response  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### 4.2 SQL Generation Patterns

#### 4.2.1 Main Table Filters Only

```sql
-- Scenario: Filter and sort by indexed main table columns
SELECT 
    m.row_id,
    m.created_at,
    m.text_01 AS name,
    m.integer_01 AS age
FROM entity_main m
WHERE m.schema_id = $1
  AND m.integer_01 > $2           -- age > 18
  AND m.text_01 LIKE $3           -- name LIKE 'John%'
  AND (m.deleted_at IS NULL OR m.deleted_at = 0)
ORDER BY m.created_at DESC
LIMIT $4 OFFSET $5;
```

#### 4.2.2 Single EAV Filter with EAV Sort

```sql
-- Scenario: Filter and sort by EAV attribute "priority"
WITH sorted_eav AS (
    SELECT row_id, value_int AS priority
    FROM eav_data
    WHERE schema_id = $1
      AND attr_id = $2              -- priority attribute ID
      AND value_int > $3            -- priority > 5
    ORDER BY value_int DESC
    LIMIT $4 OFFSET $5
)
SELECT 
    m.row_id,
    m.created_at,
    m.text_01 AS name,
    s.priority
FROM sorted_eav s
JOIN entity_main m ON s.row_id = m.row_id
WHERE (m.deleted_at IS NULL OR m.deleted_at = 0);
```

#### 4.2.3 Multiple EAV Filters (INTERSECT Pattern)

```sql
-- Scenario: Multiple EAV conditions (priority > 5 AND tag = 'urgent')
WITH matched_ids AS (
    -- Condition 1: priority > 5
    SELECT row_id FROM eav_data
    WHERE schema_id = $1 AND attr_id = $2 AND value_int > $3
    
    INTERSECT
    
    -- Condition 2: tag = 'urgent'
    SELECT row_id FROM eav_data
    WHERE schema_id = $1 AND attr_id = $4 AND value_text = $5
)
SELECT 
    m.row_id,
    m.created_at,
    m.text_01 AS name,
    m.integer_01 AS age
FROM entity_main m
WHERE m.row_id IN (SELECT row_id FROM matched_ids)
  AND m.schema_id = $1
  AND (m.deleted_at IS NULL OR m.deleted_at = 0)
ORDER BY m.created_at DESC
LIMIT $6 OFFSET $7;
```

#### 4.2.4 Multiple EAV Filters (EXISTS Pattern)

```sql
-- Alternative: EXISTS pattern (better when conditions have high selectivity)
SELECT 
    m.row_id,
    m.created_at,
    m.text_01 AS name
FROM entity_main m
WHERE m.schema_id = $1
  AND (m.deleted_at IS NULL OR m.deleted_at = 0)
  AND EXISTS (
      SELECT 1 FROM eav_data e
      WHERE e.row_id = m.row_id 
        AND e.attr_id = $2 AND e.value_int > $3
  )
  AND EXISTS (
      SELECT 1 FROM eav_data e
      WHERE e.row_id = m.row_id
        AND e.attr_id = $4 AND e.value_text = $5
  )
ORDER BY m.created_at DESC
LIMIT $6 OFFSET $7;
```

### 4.3 Pagination Handling

#### 4.3.1 Standard OFFSET Pagination

```sql
-- Page N (1-indexed), page_size = 20
LIMIT 20 OFFSET ((N - 1) * 20)
```

**Constraints:**
- Maximum OFFSET: 10,000 rows
- If `OFFSET > 10,000`, return error with suggestion to refine filters

#### 4.3.2 Deep Pagination Optimization

For page depths > 500, internally convert to keyset:

```sql
-- Step 1: Find cursor position
WITH cursor_pos AS (
    SELECT created_at, row_id
    FROM entity_main
    WHERE schema_id = $1 AND $FILTER_CONDITIONS
    ORDER BY created_at DESC
    OFFSET $TARGET_OFFSET - 1
    LIMIT 1
)
-- Step 2: Use keyset from cursor
SELECT *
FROM entity_main
WHERE schema_id = $1 
  AND $FILTER_CONDITIONS
  AND (created_at, row_id) < (
      SELECT created_at, row_id FROM cursor_pos
  )
ORDER BY created_at DESC
LIMIT $PAGE_SIZE;
```

### 4.4 Count Optimization

```sql
-- Bounded COUNT with timeout protection
WITH bounded_count AS (
    SELECT COUNT(*) AS cnt
    FROM entity_main m
    WHERE m.schema_id = $1
      AND $FILTER_CONDITIONS
      AND (m.deleted_at IS NULL OR m.deleted_at = 0)
    LIMIT 10001  -- Stop counting after 10,001
)
SELECT 
    LEAST(cnt, 10000) AS total,
    cnt <= 10000 AS total_is_exact
FROM bounded_count;
```

---

## 5. Advanced Query Path: Two-Phase Merge Strategy

### 5.1 Design Rationale

The two-phase merge strategy avoids expensive global sorting across all data tiers:

| Approach | Complexity | Problem |
|----------|------------|---------|
| Global Sort (Naive) | O(N log N) where N = Hot + Warm + Cold | Cold tier can be massive |
| Two-Phase Merge | O((H+W) log(H+W)) + O((M+B') log(M+B')) | Bounded working sets |

Where:
- H = Hot rows (≤ 10K)
- W = Warm rows (≤ 240K)
- M = Phase-1 intermediate result (bounded)
- B' = Relevant Base subset (pruned by cursor/boundary)

### 5.2 Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Two-Phase Merge Strategy                          │
└─────────────────────────────────────────────────────────────────────────────┘

Phase 1: Merge Hot + Warm (Bounded: ≤ 250K rows)
═══════════════════════════════════════════════

  ┌─────────────┐         ┌─────────────┐
  │  PostgreSQL │         │  S3 Delta   │
  │  (Hot)      │         │  (Warm)     │
  │  ≤ 10K rows │         │  ≤ 240K rows│
  └──────┬──────┘         └──────┬──────┘
         │                       │
         │   Full Scan OK        │   Parquet Predicate Pushdown
         │   (bounded volume)    │
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  DuckDB: UNION ALL    │
         │  + Deduplication      │
         │  + Sort + Limit M     │
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │  Phase-1 Result       │
         │  (M candidates)       │
         │  + Boundary Values    │
         └───────────┬───────────┘
                     │
                     │
Phase 2: Merge with Cold (Pruned by Boundaries)
═══════════════════════════════════════════════
                     │
                     ▼
         ┌───────────────────────┐         ┌─────────────────┐
         │  Phase-1 Result       │         │  S3 Base        │
         │  (M candidates)       │         │  (Cold)         │
         │                       │         │                 │
         │  Boundary:            │────────▶│  File Pruning:  │
         │  min_sort_key         │         │  • cursor       │
         │  max_sort_key         │         │  • boundary     │
         └───────────┬───────────┘         └────────┬────────┘
                     │                              │
                     └──────────────┬───────────────┘
                                    │
                                    ▼
                     ┌───────────────────────┐
                     │  DuckDB: UNION ALL    │
                     │  + Deduplication      │
                     │  + Final Sort         │
                     │  + LIMIT page_size+1  │
                     └───────────┬───────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │  Final Result         │
                     │  + Next/Prev Cursors  │
                     └───────────────────────┘
```

### 5.3 Data Precedence Rules

When the same `row_id` exists in multiple tiers, apply precedence:

```
Hot (PG) > Warm (Delta) > Cold (Base)
```

Implementation via `precedence` column:

| Tier | Precedence Value |
|------|------------------|
| Hot (PG) | 3 |
| Warm (Delta) | 2 |
| Cold (Base) | 1 |

Deduplication:
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY row_id 
    ORDER BY precedence DESC
) = 1
```

### 5.4 Anti-Join: Dirty Set Exclusion

**Critical for Consistency:** Rows in the Dirty Set (unflushed changes in PG) must be excluded from Delta and Base reads.

```sql
-- Dirty Set Definition
dirty_ids AS (
    SELECT row_id
    FROM postgres_scan($PG_CONN, 'change_log')
    WHERE flushed_at = 0 
      AND schema_id = $SCHEMA_ID
)

-- Delta Query: Exclude dirty rows
delta_source AS (
    SELECT ...
    FROM read_parquet($DELTA_PATHS)
    WHERE row_id NOT IN (SELECT row_id FROM dirty_ids)  -- Anti-join
      AND ...
)

-- Base Query: Exclude dirty rows
base_source AS (
    SELECT ...
    FROM read_parquet($BASE_PATHS)
    WHERE row_id NOT IN (SELECT row_id FROM dirty_ids)  -- Anti-join
      AND ...
)
```

---

## 6. SQL Execution Templates

### 6.1 Complete Phase-1 Template

```sql
-- ============================================================================
-- Phase 1: Merge Hot (PostgreSQL) + Warm (Delta)
-- ============================================================================
-- Configuration
PRAGMA memory_limit='4GB';
PRAGMA threads=4;

WITH
-- ============================================================================
-- CTE 1: Dirty Set (Unflushed row_ids in PostgreSQL)
-- ============================================================================
dirty_ids AS (
    SELECT row_id
    FROM postgres_scan(
        $PG_CONN,
        'SELECT row_id FROM change_log 
         WHERE flushed_at = 0 AND schema_id = ' || $SCHEMA_ID
    )
),

-- ============================================================================
-- CTE 2: Hot Source (PostgreSQL)
-- Full scan is acceptable due to bounded volume (≤ 10K rows)
-- ============================================================================
pg_raw AS (
    SELECT 
        row_id,
        changed_at AS ver_ts,
        deleted_at
    FROM postgres_scan(
        $PG_CONN,
        'SELECT row_id, changed_at, deleted_at 
         FROM change_log 
         WHERE flushed_at = 0 AND schema_id = ' || $SCHEMA_ID
    )
),

pg_main AS (
    SELECT 
        m.ltbase_row_id AS row_id,
        m.ltbase_created_at AS created_at,
        m.text_01 AS name,
        m.integer_01 AS age
    FROM postgres_scan(
        $PG_CONN,
        'SELECT * FROM entity_main WHERE ltbase_schema_id = ' || $SCHEMA_ID
    ) m
    WHERE m.ltbase_row_id IN (SELECT row_id FROM pg_raw)
),

pg_eav AS (
    SELECT
        row_id,
        MAX(CASE WHEN attr_id = 205 THEN value_text END) AS tag,
        MAX(CASE WHEN attr_id = 206 THEN value_int END) AS priority
    FROM postgres_scan(
        $PG_CONN,
        'SELECT * FROM eav_data WHERE schema_id = ' || $SCHEMA_ID
    )
    WHERE row_id IN (SELECT row_id FROM pg_raw)
    GROUP BY row_id
),

pg_source AS (
    SELECT
        r.row_id,
        m.created_at,
        r.ver_ts,
        r.deleted_at,
        m.name,
        m.age,
        e.tag,
        e.priority,
        3 AS precedence  -- Highest precedence
    FROM pg_raw r
    JOIN pg_main m ON r.row_id = m.row_id
    LEFT JOIN pg_eav e ON r.row_id = e.row_id
    -- Early filter: Apply all conditions in DuckDB
    WHERE (r.deleted_at IS NULL OR r.deleted_at = 0)
      AND m.age > 18                              -- $FILTER: age
      AND m.name LIKE 'John%'                     -- $FILTER: name
      AND (e.tag = 'developer' OR e.tag IS NULL)  -- $FILTER: tag (LEFT JOIN aware)
),

-- ============================================================================
-- CTE 3: Warm Source (Delta Parquet)
-- Parquet predicate pushdown is applied automatically
-- ============================================================================
delta_source AS (
    SELECT
        row_id,
        ltbase_created_at AS created_at,
        ltbase_updated_at AS ver_ts,
        ltbase_deleted_at AS deleted_at,
        name,
        age,
        tag,
        priority,
        2 AS precedence  -- Medium precedence
    FROM read_parquet($DELTA_PATHS)
    WHERE 
        -- Soft delete filter (early)
        (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0)
        -- Anti-join: Exclude rows in Dirty Set
        AND row_id NOT IN (SELECT row_id FROM dirty_ids)
        -- User filters (pushed down to Parquet reader)
        AND age > 18
        AND name LIKE 'John%'
        AND tag = 'developer'
        -- Cursor filter (if provided)
        AND ($CURSOR_SORT_KEY IS NULL 
             OR (created_at, row_id) < ($CURSOR_SORT_KEY, $CURSOR_ROW_ID))
),

-- ============================================================================
-- CTE 4: Phase-1 Combined Result
-- ============================================================================
phase1_combined AS (
    SELECT * FROM pg_source
    UNION ALL
    SELECT * FROM delta_source
),

phase1_deduped AS (
    SELECT *
    FROM phase1_combined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY row_id 
        ORDER BY precedence DESC
    ) = 1
)

-- ============================================================================
-- Phase-1 Output: Sorted candidates with boundary values
-- ============================================================================
SELECT 
    *,
    -- Compute boundaries for Phase-2 pruning
    MIN(created_at) OVER () AS phase1_min_sort_key,
    MAX(created_at) OVER () AS phase1_max_sort_key
FROM phase1_deduped
ORDER BY created_at DESC, row_id DESC
LIMIT $M;  -- M = page_size * 3 (heuristic buffer)
```

### 6.2 Complete Phase-2 Template

```sql
-- ============================================================================
-- Phase 2: Merge Phase-1 Result with Cold (Base)
-- ============================================================================

WITH
-- (Include dirty_ids CTE from Phase-1)
dirty_ids AS ( ... ),

-- (Phase-1 result passed as parameter or CTE)
phase1_result AS (
    -- Materialized from Phase-1 execution
    SELECT * FROM $PHASE1_TEMP_TABLE
),

phase1_boundary AS (
    SELECT
        MIN(created_at) AS min_sort_key,
        MAX(created_at) AS max_sort_key,
        COUNT(*) AS phase1_count
    FROM phase1_result
),

-- ============================================================================
-- CTE: Cold Source (Base Parquet)
-- Heavily pruned using cursor and Phase-1 boundaries
-- ============================================================================
base_source AS (
    SELECT
        row_id,
        ltbase_created_at AS created_at,
        ltbase_updated_at AS ver_ts,
        ltbase_deleted_at AS deleted_at,
        name,
        age,
        tag,
        priority,
        1 AS precedence  -- Lowest precedence
    FROM read_parquet($BASE_PATHS)
    WHERE
        -- Soft delete filter (early)
        (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0)
        
        -- Anti-join: Exclude rows in Dirty Set
        AND row_id NOT IN (SELECT row_id FROM dirty_ids)
        
        -- Cursor pruning: Only rows before cursor
        AND ($CURSOR_SORT_KEY IS NULL 
             OR (created_at, row_id) < ($CURSOR_SORT_KEY, $CURSOR_ROW_ID))
        
        -- Boundary pruning: Only rows that could enter final result
        -- If Phase-1 has page_size+ rows, Base only needs rows
        -- with sort_key > Phase-1's minimum (they might displace Phase-1 tail)
        AND (
            (SELECT phase1_count FROM phase1_boundary) < $PAGE_SIZE
            OR created_at > (SELECT min_sort_key FROM phase1_boundary)
        )
        
        -- User filters (pushed down to Parquet reader)
        AND age > 18
        AND name LIKE 'John%'
        AND tag = 'developer'
    
    -- Limit Base scan: At most page_size rows needed
    ORDER BY created_at DESC, row_id DESC
    LIMIT $PAGE_SIZE
),

-- ============================================================================
-- CTE: Final Merge
-- ============================================================================
final_combined AS (
    SELECT 
        row_id, created_at, ver_ts, deleted_at,
        name, age, tag, priority,
        precedence
    FROM phase1_result
    
    UNION ALL
    
    SELECT 
        row_id, created_at, ver_ts, deleted_at,
        name, age, tag, priority,
        precedence
    FROM base_source
),

final_deduped AS (
    SELECT *
    FROM final_combined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY row_id 
        ORDER BY precedence DESC
    ) = 1
)

-- ============================================================================
-- Final Output: Page results with cursor computation
-- ============================================================================
SELECT
    row_id,
    created_at,
    name,
    age,
    tag,
    priority
FROM final_deduped
ORDER BY created_at DESC, row_id DESC
LIMIT $PAGE_SIZE + 1;  -- +1 to determine has_next
```

### 6.3 Cursor Encoding/Decoding

```python
# Cursor structure
@dataclass
class Cursor:
    sort_key: str      # e.g., "2024-01-15T10:30:00Z"
    row_id: str        # e.g., "018d1234-5678-..."
    direction: str     # "forward" | "backward"

# Encoding (to base64 JSON)
def encode_cursor(cursor: Cursor) -> str:
    payload = json.dumps({
        "sk": cursor.sort_key,
        "rid": cursor.row_id,
        "dir": cursor.direction
    })
    return base64.urlsafe_b64encode(payload.encode()).decode()

# Decoding
def decode_cursor(encoded: str) -> Cursor:
    payload = json.loads(base64.urlsafe_b64decode(encoded))
    return Cursor(
        sort_key=payload["sk"],
        row_id=payload["rid"],
        direction=payload["dir"]
    )
```

**Decoder numeric exactness (#281):** the decoder MUST NOT route numeric cursor
fields through a default JSON number path — Go's `encoding/json` decodes into
`float64`, which silently rounds integers above 2^53 and makes the resulting
`BIGINT col > DOUBLE param` boundary skip or duplicate one row of the next page.
Integer sort keys must either stay strings (as the pseudocode's `sk` already
models) or decode via `json.Number` to an exact `int64` (`numutil.Int64Exact`)
before reaching `model.KeysetCursor.Values`, which the SQL generator binds
verbatim without coercion (pinned by `TestKeysetArgsPreserveInt64Above2p53`).

### 6.4 Backward Pagination SQL

```sql
-- Backward pagination: Get previous page
WITH backward_query AS (
    SELECT *
    FROM final_deduped
    WHERE (created_at, row_id) > ($CURSOR_SORT_KEY, $CURSOR_ROW_ID)
    ORDER BY created_at ASC, row_id ASC  -- Reverse order
    LIMIT $PAGE_SIZE + 1
)
-- Re-order to descending for display
SELECT * FROM backward_query
ORDER BY created_at DESC, row_id DESC;
```

---

## 7. Optimization Strategies

### 7.1 Phase-1 Simplification (No PG Pushdown)

**Rationale:** Given the bounded Hot tier (≤ 10K rows), predicate pushdown to PostgreSQL is unnecessary and introduces complexity/risk.

| Approach | Complexity | Security Risk | Performance |
|----------|------------|---------------|-------------|
| **Pushdown (Old)** | High (SQL string interpolation) | SQL injection vectors | ~40ms |
| **Full Scan (New)** | Low (static queries) | Minimal | ~50ms |

**Decision:** Accept 10ms latency increase for significantly reduced complexity and security risk.

### 7.2 Base File Pruning

Leverage Parquet metadata for file-level pruning:

```python
def select_base_files(
    all_files: List[ParquetFile],
    cursor: Optional[Cursor],
    phase1_boundary: Boundary,
    page_size: int
) -> List[str]:
    """Select only Base files that could contain relevant rows."""
    
    selected = []
    for f in all_files:
        # Skip files entirely after cursor
        if cursor and f.min_created_at > cursor.sort_key:
            continue
        
        # Skip files entirely before Phase-1 boundary (if Phase-1 is full)
        if (phase1_boundary.count >= page_size 
            and f.max_created_at < phase1_boundary.min_sort_key):
            continue
        
        selected.append(f.path)
    
    return selected
```

### 7.3 Intermediate Result Size (M)

The intermediate result size M affects Phase-2 behavior:

| M Value | Trade-off |
|---------|-----------|
| Too small | May miss valid results from Phase-1 that should appear |
| Too large | Increased memory and processing in Phase-2 |

**Recommended heuristic:**
```python
M = page_size * 3  # Base multiplier

# Adjust based on filter selectivity estimate
if estimated_selectivity < 0.01:  # Highly selective
    M = page_size * 2
elif estimated_selectivity > 0.5:  # Low selectivity
    M = page_size * 5
```

### 7.4 Streaming Result Processing

**Requirement:** Never load full DuckDB result into memory.

```go
// Go example: Streaming JSON serialization
func streamResults(ctx context.Context, rows *sql.Rows, w http.ResponseWriter) error {
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(`{"data":[`))
    
    first := true
    for rows.Next() {
        if !first {
            w.Write([]byte(","))
        }
        first = false
        
        var record Record
        if err := rows.Scan(&record); err != nil {
            return err
        }
        
        json.NewEncoder(w).Encode(record)
    }
    
    w.Write([]byte(`]}`))
    return nil
}
```

---

## 8. Type Mapping and Conversion

### 8.1 PostgreSQL to DuckDB Type Mapping

| PostgreSQL Type | DuckDB Type | Notes |
|-----------------|-------------|-------|
| `BIGINT` | `BIGINT` | Direct mapping |
| `INTEGER` | `INTEGER` | Direct mapping |
| `SMALLINT` | `INTEGER` | Upcast for safety |
| `NUMERIC` | `DOUBLE` | Precision loss acceptable for search |
| `TEXT` | `VARCHAR` | Direct mapping |
| `UUID` | `UUID` | Explicit cast may be required |
| `TIMESTAMP` | `TIMESTAMP` | Timezone handling required |
| `JSONB` | `JSON` | Parse if needed |

### 8.2 Explicit Casting in SQL

```sql
-- PostgreSQL scan with explicit casts
SELECT
    CAST(m.ltbase_row_id AS UUID) AS row_id,
    CAST(m.ltbase_created_at AS TIMESTAMP) AS created_at,
    CAST(m.text_01 AS VARCHAR) AS name,
    CAST(m.integer_01 AS INTEGER) AS age
FROM postgres_scan(...)
```

---

## 9. Error Handling and Resilience

### 9.1 Failure Modes and Responses

| Failure | Detection | Response |
|---------|-----------|----------|
| PostgreSQL unavailable | Connection timeout | Fail all queries (required for Dirty Set) |
| S3 unavailable | HTTP 5xx / timeout | Simple: continue; Advanced: fail with 503 |
| DuckDB OOM | Memory allocation failure | Return 507 with query refinement suggestion |
| Query timeout | Execution exceeds 30s | Return partial results with warning |
| Invalid cursor | Decode failure | Return 400 with error details |

### 9.2 Circuit Breaker Implementation

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, window_seconds=30, probe_interval=10):
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.probe_interval = probe_interval
        self.failures = []
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_probe = None
    
    def record_failure(self):
        now = time.time()
        self.failures = [t for t in self.failures if now - t < self.window_seconds]
        self.failures.append(now)
        
        if len(self.failures) >= self.failure_threshold:
            self.state = "OPEN"
            self.last_probe = now
    
    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            if time.time() - self.last_probe > self.probe_interval:
                self.state = "HALF_OPEN"
                return True  # Allow probe request
            return False
        
        return True  # HALF_OPEN allows one request
    
    def record_success(self):
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failures = []
```

### 9.3 Degraded Mode: S3 Unavailable

```python
def execute_advanced_query_degraded(query: Query) -> Response:
    """
    Execute with S3 unavailable - PG only mode.
    """
    warning = DegradedModeWarning(
        message="Historical data unavailable. Results may be incomplete.",
        missing_tiers=["warm", "cold"],
        data_freshness="last_24_hours"
    )
    
    # Execute Phase-1 only (PG data)
    results = execute_phase1_only(query)
    
    return Response(
        data=results,
        partial_result=True,
        warnings=[warning]
    )
```

---

## 10. Observability

### 10.1 Metrics

The metrics Forma emits are the ones `forma.MetricCatalogue()` returns. This
section does not restate them, so it cannot drift from the code. Two documents
describe them:

- `docs/telemetry.md` is the contract: each metric's name, kind, unit and
  label keys, the retired names, and how an embedding application receives
  the metrics through a `forma.MetricEmitter`.
- `docs/federated-query/design.md` §8 says what the two federated query
  series, `fed_query_latency_histogram` and `fed_query_row_count`, measure and
  when the engine emits them.

The DuckDB circuit breaker (§9.2) reports its transitions, not its current
state: `duckdb_circuit_breaker_transition_total` counts each state it enters,
and there is no state gauge (#634). `docs/federated-query/design.md` §7.1
says which events count as a transition.

### 10.2 Structured Logging

Forma's query path logs through zap's global logger, which discards every
line until the embedding application installs one with `zap.ReplaceGlobals`.
The application must do that before it builds the manager. Most log sites
look the global logger up when they write a line, but the federated engine
and its DuckDB client take it while the manager is built and keep it. A
logger installed later receives the "query results" line, the Debug SQL and
the HTTP lines described below. It never receives the engine's anomaly lines
or the DuckDB client's warning about a failed connection-init step.
`cmd/server` and `cmd/lambda` install zap's production logger, whose
threshold is Info, before they build the manager.

No single log event reports a query's phases, per-tier row counts or files
read. Which lines a query leaves depends on the layer it passes through.

The library's lines reach every caller, an embedding application included:

- `EntityManager.Query` logs "query results" at Info with the record and page
  counts. `CrossSchemaSearch` logs no such line.
- At Debug, the Postgres repository logs the SQL it runs and its arguments.
- The federated engine logs two anomalies and the circuit breaker's
  transitions, and nothing else. The anomalies are, at Error, a Parquet
  scan-guard failure attributed to specific objects, and at Warn, a manifest
  column stamp that disagrees with the Parquet footer. The breaker writes one
  line per transition (#634): "duckdb circuit breaker opened" at Warn, with
  the state it left (`from`) and the error of the DuckDB pass that opened it,
  and "duckdb circuit breaker half-open: probe admitted" and "duckdb circuit
  breaker closed" (with `from`) at Info. The engine reports every other
  failure only through the error it returns. Silence from the engine
  therefore does not mean a federated query was healthy: the degraded
  fallback described at the end of this section absorbs a failure without a
  line.

The HTTP server (`internal/httpapi`) adds the per-request lines. An
application that calls the library directly gets none of them:

- A query is logged at Info when its handler starts work on it and again when
  it has succeeded ("advanced query request received" and "advanced query
  request completed"; the simple and cross-schema search routes log the same
  pair under their own names). The simple route logs its received line twice,
  once on entry and once after parsing. The lines carry the schema name (the
  search route logs its schema list instead, plus the search term on
  receipt), page and page size, and on completion the returned and total row
  counts.
- The HTTP layer logs a failed request at most once. When the query itself
  fails, that line takes the place of the completion line. A failure that
  goes through the error gate (`respondErrorWithStatus`) is logged once, at
  the level that "Log levels are contract" in `docs/error-handling.md`
  assigns. For a 4xx whose body already tells the caller everything that
  level is Debug, so the production logger does not show it. A body that
  withholds detail carries an `error_id` that matches its log line; see
  "Public HTTP error surface" in the same document.
- A request that a handler rejects by its own checks (a method the route does
  not accept, a missing required field such as `condition`) is answered
  through `writeError`, which logs nothing. On the search and advanced-query
  routes those checks run before the "request received" line, so the request
  leaves no line at all. On the simple route the router has already logged
  "handling request" with the path and method, so a rejection there leaves
  that line.

#637 tracks whether the rejections answered through `writeError` should be
logged and where each route writes its "request received" line.

Per-stage latency and row counts are metrics (§10.1). Routing, per-source and
timing detail for one query is returned to the caller on request (§10.3) and
is not logged.

One kind of failure leaves no log line or metric of its own. When a request
sets `federated.allow_partial_degraded_mode`, the engine absorbs most
DuckDB-path failures, a circuit-breaker rejection included, and answers from
PostgreSQL alone (`docs/federated-query/design.md` §7.2). The request
succeeds, so:

- The engine does not log the failure it absorbed. Two lines are written
  before the fallback and so survive it: the scan-guard line above, and the
  breaker's "opened" line when the absorbed failure is the one that opens the
  breaker.
- The failed pass emits no `fed_query_*` sample (§10.1). During an S3 outage
  those series go flat, which reads the same as no traffic. The breaker's
  transition counter does move: a sustained outage trips the breaker, and
  each later probe that fails re-opens it. A request the open breaker
  rejects leaves nothing itself; the trip it follows was counted and logged
  once.
- `EntityManager.Query` and the HTTP layer log their ordinary success lines.

The only traces are in the response (§10.3). The `partial` marker reads
`hot_tier_only`, which the routing shortcut for small pages sets too, so it
does not identify a failure. A requested `execution_plan` names the degraded
fallback in its `routing` reason. Neither carries the cause of the failure.
`docs/federated-query/design.md` §7.2 lists "Log Error" as the first step of
this fallback. That step is not implemented, and #638 tracks it.

### 10.3 Query Diagnostics in the Response

There is no `debug` request flag. `forma.QueryResult` in `types.go` is the
field-level contract for the diagnostics a response carries.

Every query response carries `execution_time`: the wall time the query took
inside the library, as integer nanoseconds. It excludes HTTP decoding and
response encoding. The PostgreSQL-only path and cross-schema search set it
too, so it does not depend on a federated execution plan.

Two more fields carry diagnostics, and only the federated path sets them. A
request without `federated.enabled` is answered by the PostgreSQL-only path
(`docs/federated-query/design.md` §4.1) and carries neither field, whatever
else its `federated` block says.

- `execution_plan` is attached when a federated request also sets
  `federated.include_execution_plan` (`docs/federated-query/design.md` §4.3).
  It reports the routing decision (`routing`: whether DuckDB was used, the
  tiers read, the reason) and the sources the engine recorded (`sources`).
  Every source names its tier, engine and reason. A source's other fields
  (estimated rows, actual rows, whether a predicate was pushed down, duration)
  are omitted when zero, and `timings` is omitted when empty. Which of them
  appear depends on the path that answered:
  - A page answered by PostgreSQL alone, through the hot-only gate or because
    routing chose PostgreSQL, has one `postgres` source with no other fields
    and no `timings`. This is the usual shape under the default `hybrid`
    strategy, which answers a small page with implicit tiers from PostgreSQL
    (`docs/federated-query/design.md` §4.3).
  - A page answered by DuckDB has a `postgres` source for the dirty-ID set,
    with the set's size as `row_estimate`. When the hot tier is read, a second
    `postgres` source for the pushdown fragment sets `predicate_pushdown` if a
    predicate was pushed. One `duckdb` source carries `actual_rows` (the rows
    returned for the page) and `duration_ms`; its `tier` names one requested
    Parquet tier, but the scan reads warm and cold together. `timings` holds
    `translate`, `duckdb_fetch` and `total` in milliseconds, plus
    `plan_cache_hit` or `plan_cache_miss` (value 1) when the compiled-plan
    cache rendered the query.
  - A degraded answer (§10.2) has the same shape as a page answered by
    PostgreSQL alone: `routing` names the degraded fallback, and its one
    `postgres` source is the fallback's, with no other fields and no
    `timings`. The engine drops the sources and timings that the abandoned
    DuckDB attempt recorded, so the plan describes the answer, not the
    attempt.

  The generated SQL, its bind parameters and the engine's internal notes are
  not projected into the response. `forma.ExecutionPlan` also declares
  `merge`. Nothing populates it, so it never appears in a response; #613
  tracks retiring or populating it.
- `partial` is set on a federated request, whether or not a plan was
  requested, when the page was answered from less than the request asked for:
  `corrupt_parquet_excluded` with the number of excluded objects, or
  `hot_tier_only` with the tiers that were not consulted
  (`docs/federated-query/design.md` §7.3).

---

## 11. Security Considerations

### 11.1 SQL Injection Prevention

| Risk Area | Mitigation |
|-----------|------------|
| Filter values | Parameterized queries / strict escaping |
| Column names | Allowlist validation against schema |
| Table names | Hardcoded in templates |
| postgres_scan queries | Static SQL (no dynamic predicates) |
| S3 paths | Constructed from validated schema_id only |

### 11.2 Access Control

```python
def validate_query_access(user: User, query: Query) -> bool:
    """Validate user has access to requested schema."""
    
    # Check schema access
    if query.schema_id not in user.accessible_schemas:
        raise ForbiddenError(f"Access denied to schema {query.schema_id}")
    
    # Check column access (for sensitive fields)
    for field in query.requested_fields:
        if field in SENSITIVE_FIELDS and not user.has_sensitive_access:
            raise ForbiddenError(f"Access denied to field {field}")
    
    return True
```

---

## 12. Configuration Reference

### 12.1 Query Engine Configuration

```yaml
query_engine:
  # Query classification
  classification:
    simple_query:
      max_eav_conditions: 5
      max_offset: 10000
      count_timeout_ms: 200
    
  # Simple query path
  simple:
    connection_pool:
      min_connections: 5
      max_connections: 20
      acquire_timeout_ms: 5000
  
  # Advanced query path
  advanced:
    duckdb:
      memory_limit_mb: 4096
      thread_limit: 4
      temp_directory: "/tmp/duckdb"
    
    phase1:
      intermediate_multiplier: 3  # M = page_size * this
    
    phase2:
      base_file_scan_limit: 100
    
    pagination:
      max_page_size: 100
      default_page_size: 20
  
  # Timeouts
  timeouts:
    query_timeout_seconds: 30
    pg_statement_timeout_ms: 10000
  
  # Circuit breaker
  circuit_breaker:
    failure_threshold: 5
    window_seconds: 30
    probe_interval_seconds: 10

# S3 configuration
storage:
  s3:
    bucket: "data-lake"
    region: "us-west-2"
    paths:
      delta: "/{project_id}/{schema_id}/delta/"
      base: "/{project_id}/{schema_id}/base/"
    
    # Caching
    metadata_cache_ttl_seconds: 300
    file_cache_enabled: false

# PostgreSQL configuration
database:
  postgresql:
    host: "localhost"
    port: 5432
    database: "ltbase"
    sslmode: "require"
```

---

## 13. Appendix: Index Requirements

### 13.1 Required PostgreSQL Indexes

```sql
-- ============================================================================
-- entity_main indexes
-- ============================================================================

-- Primary lookup
CREATE INDEX idx_entity_schema_row 
    ON entity_main (ltbase_schema_id, ltbase_row_id);

-- Time-based sorting (most common)
CREATE INDEX idx_entity_schema_created 
    ON entity_main (ltbase_schema_id, ltbase_created_at DESC);

CREATE INDEX idx_entity_schema_updated 
    ON entity_main (ltbase_schema_id, ltbase_updated_at DESC);

-- Soft delete filtering
CREATE INDEX idx_entity_active 
    ON entity_main (ltbase_schema_id, ltbase_created_at DESC) 
    WHERE (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0);

-- ============================================================================
-- change_log indexes
-- ============================================================================

-- Dirty set lookup (critical for federated queries)
CREATE INDEX idx_changelog_dirty 
    ON change_log (schema_id, row_id) 
    WHERE flushed_at = 0;

-- Flush processing
CREATE INDEX idx_changelog_flush_pending 
    ON change_log (schema_id, changed_at) 
    WHERE flushed_at = 0;

-- ============================================================================
-- eav_data indexes
-- ============================================================================

-- Primary lookup
CREATE INDEX idx_eav_lookup 
    ON eav_data (schema_id, attr_id, row_id);

-- Value-based filtering (per type)
CREATE INDEX idx_eav_int_value 
    ON eav_data (schema_id, attr_id, value_int) 
    WHERE value_int IS NOT NULL;

CREATE INDEX idx_eav_text_value 
    ON eav_data (schema_id, attr_id, value_text) 
    WHERE value_text IS NOT NULL;

CREATE INDEX idx_eav_float_value 
    ON eav_data (schema_id, attr_id, value_float) 
    WHERE value_float IS NOT NULL;

-- Text prefix search (if needed)
CREATE INDEX idx_eav_text_prefix 
    ON eav_data (schema_id, attr_id, value_text varchar_pattern_ops) 
    WHERE value_text IS NOT NULL;
```

### 13.2 Parquet File Metadata Schema

```python
@dataclass
class ParquetFileMetadata:
    """Metadata tracked for each Parquet file for query optimization."""
    
    file_path: str
    file_size_bytes: int
    row_count: int
    
    # Row ID range (for deduplication pruning)
    min_row_id: str
    max_row_id: str
    
    # Time range (for cursor pruning)
    min_created_at: datetime
    max_created_at: datetime
    
    # Per-column statistics (for predicate pushdown)
    column_stats: Dict[str, ColumnStats]
    
    # File classification
    tier: Literal["delta", "base"]
    
    # Compaction tracking
    created_at: datetime
    compacted_from: Optional[List[str]]  # Source files if compacted

@dataclass
class ColumnStats:
    min_value: Any
    max_value: Any
    null_count: int
    distinct_count_estimate: int
```

---

## 14. Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | - | Initial design |
| 2.0 | - | Added dual-track query strategy |
| 2.1 | - | Simplified PG scanning (removed pushdown) |
| 2.2 | - | Added keyset-only pagination for advanced queries |
| 2.3 | - | Enhanced Phase-2 boundary pruning |
| 2.4 | - | Added comprehensive Anti-Join requirements |
| 2.5 | - | Replaced the unimplemented §10 observability sketches with the shipped metric catalogue, logging and response diagnostics (#597) |
| 2.6 | - | §10.1 and §10.2: the DuckDB circuit breaker's transition metric and log lines (#634) |