package sqlgen

import "text/template"

// AdvancedQueryTemplateDuckDB is the DuckDB SQL template used for federated queries.
// It accepts dynamically-generated SQL fragments for S3 source projection, PG source
// projection, EAV pivot, and final outer SELECT, supporting any schema layout.
// Metadata columns (total_records, total_pages, current_page) are rendered in the
// template directly so that PAGE_SIZE and OFFSET template vars are properly expanded.
var AdvancedQueryTemplateDuckDB = template.Must(template.New("optimizedQueryDuckDB").Funcs(template.FuncMap{
	"add": func(a, b int) int { return a + b },
}).Parse(`
-- PRAGMA & tuning
PRAGMA memory_limit='4GB';
PRAGMA threads=4;

WITH
dirty_ids AS (
  SELECT row_id
  FROM postgres_scan('{{.PG_CONN}}', '{{.ChangeLogSchema}}', '{{.ChangeLogScanTable}}')
  WHERE schema_id = {{.SCHEMA_ID}}
    AND flushed_at = 0
),

s3_source AS (
  SELECT
    {{.S3SourceSelect}},
    1 AS source_tier_priority
  FROM read_parquet({{.S3_PATHS}})
  WHERE
    ({{.LOGICAL_WHERE_CLAUSE}})
    AND CAST(row_id AS UUID) NOT IN (SELECT row_id FROM dirty_ids)
),

pg_source AS (
  SELECT
    {{.PGSourceSelect}},
    3 AS source_tier_priority
  FROM postgres_scan('{{.PG_CONN}}', '{{.ChangeLogSchema}}', '{{.ChangeLogScanTable}}') cl
  JOIN postgres_scan('{{.PG_CONN}}',
    '{{.MainSchema}}',
    '{{.MainScanTable}}'
  ) m
    ON cl.schema_id = m.ltbase_schema_id
    AND cl.row_id = m.ltbase_row_id
  {{if .HasEAVPivot}}
  LEFT JOIN (
    SELECT row_id::VARCHAR as row_id, schema_id,
      {{.EAVPivotSelect}}
    FROM postgres_scan('{{.PG_CONN}}', '{{.EAVSchema}}', '{{.EAVScanTable}}')
    WHERE attr_id IN ({{.EAVPivotAttrs}})
    GROUP BY schema_id, row_id
  ) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
  {{end}}
  WHERE cl.schema_id = {{.SCHEMA_ID}}
    AND cl.flushed_at = 0
    AND m.ltbase_schema_id = {{.SCHEMA_ID}}
    AND ({{.PG_WHERE_CLAUSE}})
  GROUP BY {{.PGGroupBy}}
),

unified AS (
  SELECT * FROM s3_source
  UNION ALL
  SELECT * FROM pg_source
),

ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY row_id
      ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
    ) AS rn
  FROM unified
  WHERE
    {{if .HAS_KEYSET}}({{.KEYSET_WHERE_CLAUSE}}) AND{{end}}
    1=1
),

visible AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    AND ({{.LOGICAL_WHERE_CLAUSE}})
)

SELECT
  {{.OuterSelect}},
  COUNT(*) OVER() AS total_records,
  CEIL(COUNT(*) OVER()::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0))::BIGINT AS total_pages,
  {{if .HAS_KEYSET}}
  1::BIGINT AS current_page
  {{else}}
  (FLOOR({{.OFFSET}}::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0)) + 1)::BIGINT AS current_page
  {{end}}
FROM visible
{{if .HAS_KEYSET}}
ORDER BY {{.ORDER_BY}}
LIMIT {{.PAGE_SIZE}}
{{else}}
ORDER BY {{.NON_KEYSET_ORDER_BY}}
LIMIT {{.PAGE_SIZE}} OFFSET {{.OFFSET}}
{{end}};
`))
