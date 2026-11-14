package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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
	"CAST(deleted_at AS BIGINT) AS deleted_at, CASE WHEN ltbase_created_at IS NULL AND COALESCE(CAST(deleted_at AS BIGINT), 0) = 0 THEN error('parquet scan produced NULL ltbase_created_at on a live row: a scanned object violates the export schema invariant (#189/#256/#460)') ELSE CAST(ltbase_created_at AS BIGINT) END AS ltbase_created_at)"

// Idle state (#256): with no missing columns the scan source is the
// system-column guard alone. The pre-#256 bare read_parquet form is retired —
// the guard is unconditional, because the object it protects against is
// exactly the one no probe ran on.
func TestBuildParquetScanSourceEmptyIsGuardOnly(t *testing.T) {
	got := BuildParquetScanSource("'s3://b/base/a.parquet'", nil, nil)
	require.Equal(t,
		"(SELECT "+wantGuardREPLACE+
			" FROM read_parquet('s3://b/base/a.parquet', union_by_name=true)) AS cold_scan",
		got)
}

// Missing columns (#255) reach the projection through a BY NAME union with a
// typed relation that holds no rows, and are re-pinned inside the REPLACE
// list rather than projected beside the star, so no rendering can name a
// column twice (#409).
func TestBuildParquetScanSourceWrapsMissingColumnsAsTypedNulls(t *testing.T) {
	got := BuildParquetScanSource("['s3://b/a.parquet', 's3://b/b.parquet']", []ScanColumn{
		{Name: "score", DuckDBType: "INTEGER"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	}, nil)
	require.Equal(t,
		"(SELECT "+wantGuardREPLACE[:len(wantGuardREPLACE)-1]+
			", CAST(score AS INTEGER) AS score, CAST(tags AS BIGINT[]) AS tags) "+
			"FROM (SELECT * FROM read_parquet(['s3://b/a.parquet', 's3://b/b.parquet'], union_by_name=true) "+
			"UNION ALL BY NAME SELECT NULL::INTEGER AS score, NULL::BIGINT[] AS tags WHERE false)) AS cold_scan",
		got)
}

// TestDuckDBNullScanTypeMirrorsTierTypes documents the intended map as a
// literal table. It cannot detect divergence from the other tiers on its own
// (it validates the table against itself) — the cross-leg proof is
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

// The parquet-only template must consume the scan source at BOTH sites:
// s3_source's FROM and the pushdown semijoin's inner FROM, on the offset and
// the keyset skeleton alike. A #255 cold-missing set must render identically
// at both, or the semijoin would scan a different shape than s3_source.
func TestParquetOnlyTemplateRendersScanSourceAtBothSites(t *testing.T) {
	params := map[string]any{"S3_PATHS": "'s3://b/base/a.parquet'"}
	require.NoError(t, injectDuckDBTemplateParams(params, nil, nil))
	// nil query keeps this a pure param-derivation probe.
	src, _ := params["S3_SCAN_SOURCE"].(string)
	require.Equal(t, BuildParquetScanSource("'s3://b/base/a.parquet'", nil, nil), src)
	require.Contains(t, src, ParquetNullRowIDMessage)

	params = map[string]any{
		"S3_PATHS":           "'s3://b/base/a.parquet'",
		"ColdMissingColumns": []ScanColumn{{Name: "score", DuckDBType: "INTEGER"}},
	}
	require.NoError(t, injectDuckDBTemplateParams(params, nil, nil))
	src, _ = params["S3_SCAN_SOURCE"].(string)
	require.Contains(t, src, "NULL::INTEGER AS score")

	// End-to-end render: exactly the template's two scan sites carry the
	// wrapped source.
	for name, cursor := range map[string]*model.KeysetCursor{
		"offset": nil,
		"keyset": parquetOnlyTestCursor(),
	} {
		t.Run(name, func(t *testing.T) {
			renderParams := parquetOnlyTestParams(t)
			renderParams["ColdMissingColumns"] = []ScanColumn{
				{Name: "score", DuckDBType: "INTEGER"},
				{Name: "tags", DuckDBType: "BIGINT[]"},
			}
			compiled, err := CompileParquetOnlyQuery(renderParams, parquetOnlyTestQuery(cursor), nil)
			require.NoError(t, err)
			sqlText := compiled.Skeleton
			require.Equal(t, 2, strings.Count(sqlText, "NULL::INTEGER AS score"))
			require.Equal(t, 2, strings.Count(sqlText, "NULL::BIGINT[] AS tags"))
			require.Equal(t, 2, strings.Count(sqlText, "read_parquet("))
			// The #256 guard reaches both sites too: the semijoin is where a
			// NULL-row_id row would silently under-qualify the outer scan, and
			// a NULL/VARCHAR changed_at would silently misorder the LWW merge.
			require.Equal(t, 2, strings.Count(sqlText, ParquetNullRowIDMessage))
			require.Equal(t, 2, strings.Count(sqlText, ParquetNullChangedAtMessage))
			require.Equal(t, 2, strings.Count(sqlText, "CAST(deleted_at AS BIGINT) AS deleted_at"))
		})
	}
}
