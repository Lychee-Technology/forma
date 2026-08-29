package sqlgen

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// wantGuardREPLACE is the exact REPLACE list the scan source must emit: a
// presence guard on row_id and changed_at, a type pin (CAST) on changed_at and
// deleted_at, and deliberately NO cast on row_id (UUID in production, VARCHAR
// in the benchmark shape) and NO presence guard on deleted_at (legitimately
// NULL for live rows in pre-#274 legacy delta objects, #365). Spelled out
// rather than composed from the production expression so a shape change has
// to be made twice, on purpose.
const wantGuardREPLACE = "* REPLACE (COALESCE(row_id, error('" + ParquetNullRowIDMessage + "')) AS row_id, " +
	"CAST(COALESCE(changed_at, error('" + ParquetNullChangedAtMessage + "')) AS BIGINT) AS changed_at, " +
	"CAST(deleted_at AS BIGINT) AS deleted_at)"

// Idle state (#256): with no missing columns the scan source is the
// system-column guard alone. The pre-#256 bare read_parquet form is retired —
// the guard is unconditional, because the object it protects against is
// exactly the one no probe ran on.
func TestBuildParquetScanSourceEmptyIsGuardOnly(t *testing.T) {
	got := BuildParquetScanSource("'s3://b/base/a.parquet'", nil)
	require.Equal(t,
		"(SELECT "+wantGuardREPLACE+
			" FROM read_parquet('s3://b/base/a.parquet', union_by_name=true)) AS cold_scan",
		got)
}

func TestBuildParquetScanSourceWrapsMissingColumnsAsTypedNulls(t *testing.T) {
	got := BuildParquetScanSource("['s3://b/a.parquet', 's3://b/b.parquet']", []NullScanColumn{
		{Name: "score", DuckDBType: "INTEGER"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	})
	require.Equal(t,
		"(SELECT "+wantGuardREPLACE+", "+
			"NULL::INTEGER AS score, NULL::BIGINT[] AS tags "+
			"FROM read_parquet(['s3://b/a.parquet', 's3://b/b.parquet'], union_by_name=true)) AS cold_scan",
		got)
}

// TestDuckDBNullScanTypeMirrorsTierTypes documents the intended map as a
// literal table. It cannot detect divergence from the other tiers on its own
// (it validates the table against itself) — the cross-leg proof is
// TestDuckDBNullScanTypeMatchesHotLegTypeof below (hot pivot) and
// cdc.TestCastEAVValueMatchesNullScanTypeof (export leg).
func TestDuckDBNullScanTypeMirrorsTierTypes(t *testing.T) {
	bound := &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger01}
	cases := []struct {
		meta forma.AttributeMetadata
		want string
	}{
		{forma.AttributeMetadata{ValueType: forma.ValueTypeBool}, "BOOLEAN"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt}, "BIGINT"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeDate}, "BIGINT"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime}, "BIGINT"},
		// #384: EAV-only integer/smallint carry storage width DOUBLE (the
		// write funnel narrows everything through float64), while
		// column-bound ones keep the physical int4/int2 width.
		{forma.AttributeMetadata{ValueType: forma.ValueTypeInteger}, "DOUBLE"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeSmallInt}, "DOUBLE"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeInteger, ColumnBinding: bound}, "INTEGER"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeSmallInt, ColumnBinding: bound}, "SMALLINT"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeNumeric}, "DOUBLE"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeText}, "VARCHAR"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeUUID}, "VARCHAR"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt}, "BIGINT[]"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger}, "DOUBLE[]"},
		{forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText}, "VARCHAR[]"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, DuckDBNullScanType(c.meta), "vt=%s items=%s bound=%v",
			c.meta.ValueType, c.meta.ItemsType, c.meta.ColumnBinding != nil)
	}
}

// minimalAdvancedParams builds a fully renderable advanced-template param map
// whose parquet scan source carries the given cold-missing columns. It mirrors
// the production param set (buildDuckDBQueryWithPlan) closely enough to render
// the template directly, without going through BuildDuckDBQuery — so the two
// scan sites can be counted in isolation.
func minimalAdvancedParams(t *testing.T, missing []NullScanColumn) map[string]any {
	t.Helper()
	return injectTestRenderParams(t, map[string]any{
		"PG_CONN":              "dbname=forma host=localhost",
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"SCHEMA_ID":            int16(1),
		"HasHot":               true,
		"HasDirtyIDs":          false,
		"DirtyIDsCSV":          "",
		"FlushGraceCutoffMs":   FlushGraceCutoffDisabled,
		"LOGICAL_WHERE_CLAUSE": "1=1",
		"PG_WHERE_CLAUSE":      "1=1",
		"HAS_KEYSET":           false,
		"PAGE_SIZE":            10,
		"OFFSET":               0,
		"NON_KEYSET_ORDER_BY":  "created_at DESC, row_id ASC",
		"S3_SCAN_SOURCE":       BuildParquetScanSource("'s3://b/a.parquet'", missing),
	}, 1)
}

// The advanced template must consume the scan source at BOTH sites:
// s3_source's FROM and the pushdown semijoin's inner FROM.
func TestAdvancedTemplateRendersScanSourceAtBothSites(t *testing.T) {
	params := map[string]any{"S3_PATHS": "'s3://b/base/a.parquet'"}
	injectDuckDBTemplateParams(params, nil, nil)
	// nil query keeps this a pure param-derivation probe.
	src, _ := params["S3_SCAN_SOURCE"].(string)
	require.Equal(t, BuildParquetScanSource("'s3://b/base/a.parquet'", nil), src)
	require.Contains(t, src, ParquetNullRowIDMessage)

	params = map[string]any{
		"S3_PATHS":           "'s3://b/base/a.parquet'",
		"ColdMissingColumns": []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}},
	}
	injectDuckDBTemplateParams(params, nil, nil)
	src, _ = params["S3_SCAN_SOURCE"].(string)
	require.Contains(t, src, "NULL::INTEGER AS score")

	// End-to-end render: exactly the template's two scan sites carry the
	// wrapped source (rendered via the real advanced template).
	var b strings.Builder
	require.NoError(t, AdvancedQueryTemplateDuckDB.Execute(&b, minimalAdvancedParams(t, []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})))
	require.Equal(t, 2, strings.Count(b.String(), "NULL::INTEGER AS score"))
	require.Equal(t, 2, strings.Count(b.String(), "read_parquet("))
	// The #256 guard reaches both sites too — the semijoin is where a
	// NULL-row_id row would silently under-qualify the outer scan, and a
	// NULL/VARCHAR changed_at would silently misorder the LWW merge.
	require.Equal(t, 2, strings.Count(b.String(), ParquetNullRowIDMessage))
	require.Equal(t, 2, strings.Count(b.String(), ParquetNullChangedAtMessage))
	require.Equal(t, 2, strings.Count(b.String(), "CAST(deleted_at AS BIGINT) AS deleted_at"))
}

// eavFixtureSubquery mimics the eav_data columns the hot-leg pivot reads, at
// the types they actually carry in the pivot: value_numeric is DOUBLE (the
// numeric-family column every scalar cast starts from) and value_text is
// VARCHAR.
const eavFixtureSubquery = "(SELECT CAST(1 AS DOUBLE) AS value_numeric, CAST('x' AS VARCHAR) AS value_text)"

// TestDuckDBNullScanTypeMatchesHotLegTypeof is the #255 type-lockstep proof
// against a real DuckDB engine: for every value type, the type DuckDB gives
// the hot-leg EAV cast (eavElementCastExpr) must equal the type DuckDB gives
// NULL::DuckDBNullScanType(...). Divergence widens the UNION ALL between the
// pg_source leg and the NULL-augmented parquet leg and re-opens #205.
//
// The comparison is on typeof() rather than on the rendered SQL text: bool
// renders as `(value_numeric <> 0)` and text as a bare `value_text`, so
// neither carries a type literal to extract — only the engine knows.
func TestDuckDBNullScanTypeMatchesHotLegTypeof(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	typeOf := func(t *testing.T, expr string) string {
		t.Helper()
		var typ string
		require.NoError(t, db.QueryRow("SELECT typeof("+expr+")").Scan(&typ), "expr %q must evaluate", expr)
		return typ
	}

	scalars := []forma.ValueType{
		forma.ValueTypeBool, forma.ValueTypeBigInt, forma.ValueTypeDate,
		forma.ValueTypeDateTime, forma.ValueTypeInteger, forma.ValueTypeSmallInt,
		forma.ValueTypeNumeric, forma.ValueTypeText, forma.ValueTypeUUID,
	}
	for _, vt := range scalars {
		t.Run(string(vt), func(t *testing.T) {
			var hot string
			hotExpr := "typeof((" + eavElementCastExpr(vt) + ")) FROM " + eavFixtureSubquery
			require.NoError(t, db.QueryRow("SELECT "+hotExpr).Scan(&hot),
				"hot-leg cast %q must evaluate", eavElementCastExpr(vt))

			cold := typeOf(t, "NULL::"+DuckDBNullScanType(forma.AttributeMetadata{ValueType: vt}))
			require.Equal(t, hot, cold,
				"cold NULL scan type must equal the hot-leg EAV cast type for %s (#205 no-widening)", vt)
		})
	}

	// LIST: the elem+"[]" construction has to name the same type DuckDB gives
	// an aggregated list of the element cast — for every items type, since each
	// takes a different eavElementCastExpr arm.
	for _, items := range scalars {
		t.Run("list_"+string(items), func(t *testing.T) {
			var hot string
			require.NoError(t, db.QueryRow(
				"SELECT typeof(list(x)) FROM (SELECT "+eavElementCastExpr(items)+
					" AS x FROM "+eavFixtureSubquery+")").Scan(&hot))
			cold := typeOf(t, "NULL::"+DuckDBNullScanType(forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: items}))
			require.Equal(t, hot, cold, "LIST element type must round-trip through the []-suffix construction for items=%s", items)
		})
	}
}

// eavPivotFixtureSubquery extends eavFixtureSubquery with the columns the full
// pivot expression additionally reads: attr_id (matching the probe's attribute
// ID 9) and array_indices (a non-empty element index, so the list branch's
// FILTER keeps the row).
const eavPivotFixtureSubquery = "(SELECT 9 AS attr_id, CAST(1 AS DOUBLE) AS value_numeric, " +
	"CAST('x' AS VARCHAR) AS value_text, CAST('0' AS VARCHAR) AS array_indices)"

// TestDuckDBNullScanTypeMatchesPivotLegTypeof asserts the SAME lockstep against
// the full hot-leg pivot expression (buildEAVPivotExpr) rather than only the
// element cast. The two are type-identical today because every pivot arm wraps
// the element cast's type unchanged (MAX/list preserve it, the bool arm renders
// BOOLEAN either way) — but nothing else enforces that, and it is the pivot,
// not the bare element cast, that pg_source actually projects into the UNION
// ALL. If a future pivot arm diverges from eavElementCastExpr for some scalar,
// this test fails where the sibling above cannot (round-2 review observation).
func TestDuckDBNullScanTypeMatchesPivotLegTypeof(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	pivotTypeof := func(t *testing.T, meta forma.AttributeMetadata) string {
		t.Helper()
		expr := buildEAVPivotExpr(attrProjectionInfo{attrID: 9, meta: meta})
		var typ string
		require.NoError(t, db.QueryRow(
			"SELECT typeof(("+expr+")) FROM "+eavPivotFixtureSubquery).Scan(&typ),
			"pivot expr %q must evaluate", expr)
		return typ
	}
	nullTypeof := func(t *testing.T, rendered string) string {
		t.Helper()
		var typ string
		require.NoError(t, db.QueryRow("SELECT typeof(NULL::"+rendered+")").Scan(&typ))
		return typ
	}

	scalars := []forma.ValueType{
		forma.ValueTypeBool, forma.ValueTypeBigInt, forma.ValueTypeDate,
		forma.ValueTypeDateTime, forma.ValueTypeInteger, forma.ValueTypeSmallInt,
		forma.ValueTypeNumeric, forma.ValueTypeText, forma.ValueTypeUUID,
	}
	for _, vt := range scalars {
		t.Run(string(vt), func(t *testing.T) {
			hot := pivotTypeof(t, forma.AttributeMetadata{ValueType: vt})
			cold := nullTypeof(t, DuckDBNullScanType(forma.AttributeMetadata{ValueType: vt}))
			require.Equal(t, hot, cold,
				"cold NULL scan type must equal the hot-leg PIVOT type for %s (#205 no-widening)", vt)
		})
	}
	for _, items := range scalars {
		t.Run("list_"+string(items), func(t *testing.T) {
			hot := pivotTypeof(t, forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: items})
			cold := nullTypeof(t, DuckDBNullScanType(forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: items}))
			require.Equal(t, hot, cold,
				"cold NULL scan type must equal the hot-leg LIST pivot type for items=%s", items)
		})
	}
}
