package federated

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// dottedAttrCache mirrors the e2e_nested fixture (#260): column-bound and
// EAV-only attributes with dotted names, plus a flat control attribute.
func dottedAttrCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.name": {
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "text_01",
			},
		},
		"contact.annualIncome": {
			AttributeID: 2,
			ValueType:   forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "bigint_01",
			},
		},
		"contact.note": {
			AttributeID: 3,
			ValueType:   forma.ValueTypeText,
		},
		"flag": {
			AttributeID: 4,
			ValueType:   forma.ValueTypeText,
		},
	}
}

// TestDuckDBReadDottedAttributes executes the real projection, WHERE and
// outer-select fragments against a real parquet file whose columns carry the
// CDC exporter's physical names (ParquetAttrColumn fold). Pre-#260 this
// failed at bind time: `Binder Error: Referenced table "contact" not found`.
// The hot tier and dirty set are elided (same reduction as
// singlescan_pushdown_test.go): #260 is purely about identifier emission,
// which this pipeline exercises at every surface — s3 projection, semijoin
// WHERE, visible WHERE, outer select.
func TestDuckDBReadDottedAttributes(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	parquet := filepath.ToSlash(filepath.Join(t.TempDir(), "delta.parquet"))
	_, err = db.Exec(fmt.Sprintf(`COPY (
		SELECT * FROM (VALUES
			('11111111-1111-1111-1111-111111111111', 100, 0, 8000, 'alice', 'note-a', 'x'),
			('22222222-2222-2222-2222-222222222222', 100, 0, 3000, 'bob',   'note-b', 'y'),
			('33333333-3333-3333-3333-333333333333', 100, 0, 9000, 'carol', NULL,     'z')
		) t(row_id, changed_at, deleted_at, contact_annualIncome, contact_name, contact_note, flag)
	) TO '%s' (FORMAT PARQUET)`, parquet))
	require.NoError(t, err)

	cache := dottedAttrCache()
	sp, err := sqlgen.BuildSchemaProjection(30, cache)
	require.NoError(t, err)

	idx := 1
	dual, err := sqlgen.ToDualClauses(&forma.KvCondition{
		Attr:  "contact.annualIncome",
		Value: "gt:5000",
	}, "eav_data", 30, cache, &idx)
	require.NoError(t, err)

	pipeline := fmt.Sprintf(`
WITH s3_source AS (
  SELECT %[1]s, 1 AS source_tier_priority
  FROM read_parquet('%[2]s', union_by_name=true)
  WHERE row_id IN (
    SELECT row_id FROM read_parquet('%[2]s', union_by_name=true)
    WHERE (%[3]s)
  )
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY row_id
    ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
  ) AS rn
  FROM s3_source
),
visible AS (
  SELECT * FROM ranked
  WHERE rn = 1 AND (deleted_ts IS NULL OR deleted_ts = 0) AND (%[3]s)
)
SELECT bigint_01, text_01 FROM (SELECT %[4]s FROM visible) q ORDER BY bigint_01 DESC`,
		sp.S3SourceSelect, parquet, dual.DuckClause, sp.OuterSelect)

	// The clause renders twice (semijoin + visible), so its args bind twice —
	// mirroring the production template's interleave.
	args := append(append([]any{}, dual.DuckArgs...), dual.DuckArgs...)
	rows, err := db.Query(pipeline, args...)
	require.NoError(t, err, "pipeline must bind and execute:\n%s", pipeline)
	defer rows.Close()

	var incomes []int64
	var names []string
	for rows.Next() {
		var income int64
		var name string
		require.NoError(t, rows.Scan(&income, &name))
		incomes = append(incomes, income)
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{9000, 8000}, incomes)
	require.Equal(t, []string{"carol", "alice"}, names)
}
