package federated

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// TestDuckClause_NonBenchmark_ReferencesAttributeAliasedColumns is the #167
// regression guard (row correctness, real DuckDB driver).
//
// The DuckDB federated CTEs (s3_source / pg_source / unified / visible) alias
// every column by ATTRIBUTE name — e.g. `COALESCE(hot_vals.age, m.integer_01)
// AS age`. The DuckClause (LOGICAL_WHERE_CLAUSE) is applied against those CTEs,
// so it must reference the attribute name (`age`), not the physical entity_main
// column (`integer_01`). Non-benchmark schemas have no translation shim, so a
// DuckClause that emitted the physical column name would reference a column that
// does not exist in the unified CTE — a binder error / empty result in
// production. (Benchmark schemas masked this via translateDuckClauseToBenchmark.)
//
// This drives a `unified`-shaped CTE with attribute-aliased columns through the
// real DuckDB engine using the DuckClause that ToDualClauses produces for a
// column-bound (main) + pure-EAV composite, and asserts the correct intersection
// comes back. Against the pre-fix code (physical column names) DuckDB raises a
// binder error for the missing `integer_01` column.
func TestDuckClause_NonBenchmark_ReferencesAttributeAliasedColumns(t *testing.T) {
	// Non-benchmark schema cache: `age` is main-column-bound (integer_01),
	// `tag` is a pure-EAV attribute.
	cache := forma.SchemaAttributeCache{
		"age": {AttributeID: 6, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},
		&forma.KvCondition{Attr: "tag", Value: "equals:x"},
	}}

	idx := 0
	dc, err := sqlgen.ToDualClauses(cond, "eav_data", 7 /* non-benchmark */, cache, &idx)
	require.NoError(t, err)
	require.NotEmpty(t, dc.DuckClause)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// A `unified`-shaped relation whose columns are aliased by attribute name,
	// exactly as SchemaProjection builds them (COALESCE(...) AS age, ... AS tag).
	const rel = `(VALUES
		('r1', 20, 'x'),
		('r2', 5,  'x'),
		('r3', 30, 'y'),
		('r4', 15, 'x')
	) AS unified(row_id, age, tag)`

	query := "SELECT row_id FROM " + rel + " WHERE " + dc.DuckClause + " ORDER BY row_id"
	rows, err := db.Query(query, dc.DuckArgs...)
	require.NoError(t, err, "DuckClause must reference attribute-aliased columns present in the CTE (query=%s)", query)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())

	// age>10 AND tag='x' -> r1 (20,x) and r4 (15,x); r2 fails age, r3 fails tag.
	require.Equal(t, []string{"r1", "r4"}, got)
}
