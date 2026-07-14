package sqlgen

import "text/template"

// AdvancedQueryTemplateDuckDB is the DuckDB SQL template used for federated queries.
// It accepts dynamically-generated SQL fragments for S3 source projection, PG source
// projection, EAV pivot, and final outer SELECT, supporting any schema layout.
// Metadata columns (total_records, total_pages, current_page) are rendered in the
// template directly so that PAGE_SIZE and OFFSET template vars are properly expanded.
// Resource pragmas (threads / memory_limit) are deliberately absent: they are
// connection-level configuration (DuckDBConfig via applyResourcePragmas), and a
// per-query PRAGMA would override the configured values on every execution.
// HasHot selects the tier form (#184): hot-excluded PreferredTiers drop the
// pg_source data CTE and its UNION ALL branch, while dirty_ids always renders
// — it is the consistency barrier, not a hot data source, so unflushed rows
// stay consistently invisible instead of resurfacing as stale parquet
// versions. Callers must always set HasHot (a missing map key renders as
// false and would silently prune pg_source).
var AdvancedQueryTemplateDuckDB = template.Must(template.New("optimizedQueryDuckDB").Funcs(template.FuncMap{
	"add": func(a, b int) int { return a + b },
}).Parse(`
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
    CAST(row_id AS UUID) NOT IN (SELECT row_id FROM dirty_ids)
    -- Predicate pushdown as a row_id semijoin: a row qualifies when ANY of
    -- its parquet versions matches, and ALL of its versions then enter the
    -- ranked dedup so the latest version wins before the final filter in
    -- visible. Filtering versions directly here dropped newer non-matching
    -- versions pre-dedup and resurrected stale base rows whose old values
    -- still matched (#173).
    AND row_id IN (
      SELECT row_id FROM read_parquet({{.S3_PATHS}})
      WHERE ({{.LOGICAL_WHERE_CLAUSE}})
    )
),
{{if .HasHot}}
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
{{end}}
unified AS (
  SELECT * FROM s3_source{{if .HasHot}}
  UNION ALL
  SELECT * FROM pg_source{{end}}
),

ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY row_id
      ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
    ) AS rn
  FROM unified
),

visible AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    AND ({{.LOGICAL_WHERE_CLAUSE}})
    -- Keyset cursor evaluates post-dedup: a WHERE in ranked would filter row
    -- versions before ROW_NUMBER, letting a superseded version win rn = 1
    -- and resurrect for cursors over any version-varying column — business
    -- attributes and created_at alike (#212), the keyset twin of #173. It
    -- renders after the logical clause so positional placeholder order keeps
    -- matching arg order (keyset args are appended last).
    {{if .HAS_KEYSET}}AND ({{.KEYSET_WHERE_CLAUSE}}){{end}}
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
