package sqlgen

import "text/template"

// StagedDirtyIDsTable is the temp table the parquet-only template reads the
// dirty set from (plan step 3, #854). The pgx pass stages the set into it
// through the Appender on the DuckDB connection it pins, runs the template on
// that connection, and drops the table when the pass ends. A temp table is
// private to its connection, so concurrent passes on other connections never
// see each other's sets. The statement names the table, never the IDs, so the
// SQL text does not change with the set's size and one compiled plan serves
// every set (the step 1 measurement chose the Appender over a bound UUID[]
// parameter for this reason and for its cost past 10,000 IDs, #849).
const StagedDirtyIDsTable = "forma_dirty_ids"

// CreateStagedDirtyIDsSQL creates, or empties, the staged dirty-set table.
// CREATE OR REPLACE makes a pass that failed before its drop harmless to the
// next pass on the same connection.
const CreateStagedDirtyIDsSQL = "CREATE OR REPLACE TEMP TABLE " + StagedDirtyIDsTable + " (row_id UUID)"

// stagedDirtyIDsTableRef qualifies the staged table with the temp catalog, so
// a persistent table of the same name in the database can never answer for it.
const stagedDirtyIDsTableRef = "temp.main." + StagedDirtyIDsTable

// DropStagedDirtyIDsSQL drops the staged dirty-set table at the end of a pass.
const DropStagedDirtyIDsSQL = "DROP TABLE IF EXISTS " + stagedDirtyIDsTableRef

// ParquetOnlyQueryTemplateDuckDB is the parquet leg of the pgx hot-leg pass
// (design.md §6.1.1, plan step 3). It is AdvancedQueryTemplateDuckDB without
// PostgreSQL: no pg_source, no catalog reference, so no ATTACH and no
// connection string. Go serves the hot rows and merges, so this statement
// answers only the parquet rows of the page.
//
// It differs from today's template in four ways, each with a reason:
//
//   - dirty_ids is the staged temp table (StagedDirtyIDsTable), not a
//     change_log scan. The Go side stages whichever set the tier form needs
//     (the widened set for the hot form, the strict one for the hot-excluded
//     form), so the template has no HasHot branch and no flush-grace cutoff.
//   - Every ORDER BY key renders NULLS LAST. Go's comparator places NULL
//     last in both directions; DuckDB's default does the same today, but the
//     default_null_order setting can change a default, and the two orders
//     must agree for the merge to be correct.
//   - The window is bound, not rendered: LIMIT ? OFFSET ? for offset pages
//     (the parquet ranks [max(0, offset-h), offset+limit) where h is the hot
//     match count, ParquetWindow), LIMIT ? for keyset pages. h changes with
//     every pass, so a rendered window would compile one plan per value.
//   - total_pages and current_page are typed NULLs. The rows answer a window
//     of the parquet leg, not the page, so only the merge can compute them;
//     the columns stay so the result keeps the positional layout
//     duckDBScanBuffers scans. total_records stays: it is the parquet
//     count the merge adds h to.
//
// The dedup, the filter on the dedup winner (#173), the keyset predicate
// after dedup (#212) and the scan source (#189/#255/#256/#371) are today's,
// rendered by the same builders.
var ParquetOnlyQueryTemplateDuckDB = template.Must(template.New("parquetOnlyQueryDuckDB").Parse(`
WITH
s3_source AS (
  SELECT
    {{.S3SourceSelect}},
    1 AS source_tier_priority
  FROM {{.S3_SCAN_SOURCE}}
  WHERE
    CAST(row_id AS UUID) NOT IN (SELECT row_id FROM ` + stagedDirtyIDsTableRef + `)
    AND row_id IN (
      SELECT row_id FROM {{.S3_SCAN_SOURCE}}
      WHERE ({{.LOGICAL_WHERE_CLAUSE}})
    )
),

ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY row_id
      ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
    ) AS rn
  FROM s3_source
),

visible AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    AND ({{.LOGICAL_WHERE_CLAUSE}})
    {{if .HAS_KEYSET}}AND ({{.KEYSET_WHERE_CLAUSE}}){{end}}
)

SELECT
  {{.OuterSelect}},
  COUNT(*) OVER() AS total_records,
  NULL::BIGINT AS total_pages,
  NULL::BIGINT AS current_page
FROM visible
ORDER BY {{.ORDER_BY}}
{{if .HAS_KEYSET}}LIMIT ?{{else}}LIMIT ? OFFSET ?{{end}};
`))
