package internal

import "text/template"

// AdvancedQueryTemplateDuckDB is the DuckDB SQL template used for federated queries.
// Placeholders expected to be substituted by the renderer:
//
//	$SCHEMA_ID, $PG_CONN, $PG_WHERE_CLAUSE, $LOGICAL_WHERE_CLAUSE, $S3_PATHS, $PAGE_SIZE, $OFFSET
var AdvancedQueryTemplateDuckDB = template.Must(template.New("optimizedQueryDuckDB").Funcs(template.FuncMap{
	"add": func(a, b int) int { return a + b },
}).Parse(`
-- PRAGMA & tuning
PRAGMA memory_limit='4GB';
PRAGMA threads=4;

-- Parameters:
-- $SCHEMA_ID       : integer
-- $PG_CONN         : postgres connection string for postgres_scan
-- $PG_WHERE_CLAUSE : pushdown predicate for entity_main (physical columns)
-- $LOGICAL_WHERE_CLAUSE : logical predicate for final filtering (DuckDB / Parquet columns)
-- $S3_PATHS        : comma-separated paths for read_parquet()
-- $PAGE_SIZE, $OFFSET

WITH
-- Dirty set: rows currently present/dirty in PG change_log (flushed_at = 0)
dirty_ids AS (
  SELECT row_id
  FROM postgres_scan($PG_CONN, 'change_log', 'flushed_at = 0')
  WHERE schema_id = $SCHEMA_ID
),

-- S3 source (Cold/Warm). Read Parquet files and apply logical filters + anti-join
s3_source AS (
  SELECT
    row_id,
    ltbase_created_at AS created_at,
    ltbase_updated_at AS ver_ts,
    ltbase_deleted_at AS deleted_ts,

    -- Logical columns (native in Parquet)
    name,
    age,
    tag

  FROM read_parquet($S3_PATHS)
  WHERE
    ($LOGICAL_WHERE_CLAUSE)
    -- Anti-join: exclude rows that are present in the Dirty Set (PG hot buffer)
    AND row_id NOT IN (SELECT row_id FROM dirty_ids)
),

-- PG source (Hot). Use postgres_scan with pushdown for entity_main, pivot EAV attributes.
pg_source AS (
  SELECT
    m.ltbase_row_id AS row_id,
    m.ltbase_created_at AS created_at,
    cl.changed_at AS ver_ts,
    cl.deleted_at AS deleted_ts,

    -- Explicit casts to align PG types with Parquet schema
    CAST(m.text_01 AS VARCHAR) AS name,
    CAST(m.integer_01 AS INTEGER) AS age,

    -- EAV pivot (explicit casts). Replace attr_id constants with dynamic mapping if needed.
    MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag

  FROM postgres_scan($PG_CONN, 'change_log', 'flushed_at = 0') cl

  -- Pushdown: restrict entity_main at the scan-level using $PG_WHERE_CLAUSE
  JOIN postgres_scan($PG_CONN,
    'SELECT * FROM entity_main_dev
     WHERE ltbase_schema_id = ' || $SCHEMA_ID || '
       AND (' || $PG_WHERE_CLAUSE || ')'
  ) m
    ON cl.schema_id = m.ltbase_schema_id
    AND cl.row_id = m.ltbase_row_id

  LEFT JOIN postgres_scan($PG_CONN, 'eav_data_dev') e
    ON cl.schema_id = e.schema_id AND cl.row_id = e.row_id

  WHERE cl.schema_id = $SCHEMA_ID
  GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01
),

-- Union warm/cold S3 data with hot PG data
unified AS (
  SELECT * FROM s3_source
  UNION ALL
  SELECT * FROM pg_source
)

-- Final selection:
-- - Apply final logical filters to ensure EAV & other logical predicates are respected
-- - Remove soft-deleted rows
-- - Deduplicate using Last-Write-Wins (ver_ts ordering)
SELECT
  row_id,
  name,
  age,
  tag,
  created_at
FROM unified
WHERE
  ($LOGICAL_WHERE_CLAUSE)
  AND (deleted_ts IS NULL OR deleted_ts = 0)

-- Deduplicate: keep most recent version per row_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY ver_ts DESC) = 1

ORDER BY created_at DESC
LIMIT $PAGE_SIZE OFFSET $OFFSET;
`))
