package cdc

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestBuildBaseExportSQL_UsesRowIDsAndConfig(t *testing.T) {
	cfg := CDCConfig{EntityMainTable: "entity_main_dev", DuckMemLimit: "6GB", ParquetCompression: "zstd", ParquetCompressionLevel: 5}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", cfg, 1, []uuid.UUID{rowID}, testAttrCache())
	if err != nil {
		t.Fatalf("buildBaseExportSQL returned error: %v", err)
	}

	rowFilter := "row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')"
	if !strings.Contains(mQuery, "ltbase_row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')") {
		t.Fatalf("main query missing row filter: %s", mQuery)
	}
	if !strings.Contains(mQuery, "ltbase_deleted_at IS NULL") {
		t.Fatalf("main query missing active-row filter: %s", mQuery)
	}
	if !strings.Contains(eQuery, rowFilter) {
		t.Fatalf("eav query missing row filter: %s", eQuery)
	}
	if !strings.Contains(sql, "PRAGMA memory_limit='6GB'") {
		t.Fatalf("sql missing configured memory limit: %s", sql)
	}
	if !strings.Contains(sql, "PARQUET_VERSION V2") {
		t.Fatalf("sql missing parquet v2 export option: %s", sql)
	}
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing projected columns (changed_at/flag): %s", sql)
	}
}

func TestBuildBaseExportSQL_ErrorsWithoutAttrCache(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
	_, _, _, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, []uuid.UUID{rowID}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "1")
}

// #519: a base export with no row ids is a zero-row export, not an error. A
// schema with zero live rows still gets one base object with the full
// projected column set (its manifest must never list nothing), so both
// source queries select no rows via a FALSE filter and the projection is
// unchanged. The attribute-cache requirement still applies.
func TestBuildBaseExportSQL_EmptyRowIDsSelectsNoRowsWithFullProjection(t *testing.T) {
	sql, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, nil, testAttrCache())
	require.NoError(t, err)
	require.Contains(t, mQuery, "ltbase_schema_id = 1 AND ltbase_deleted_at IS NULL AND FALSE")
	require.Contains(t, eQuery, "schema_id = 1 AND FALSE")
	require.NotContains(t, mQuery, "ltbase_row_id IN (")
	require.NotContains(t, eQuery, "row_id IN (")
	require.Contains(t, sql, "TO 's3://bucket/base/1/_tmp/tmp.parquet'")

	withRows, _, _, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1,
		[]uuid.UUID{uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")}, testAttrCache())
	require.NoError(t, err)
	require.Equal(t, selectClause(t, withRows), selectClause(t, sql), "zero-row export projects the same columns as a populated one")

	_, _, _, err = buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, nil, nil)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
}

// selectClause returns the export's outer SELECT list, up to its FROM.
func selectClause(t *testing.T, sql string) string {
	t.Helper()
	start := strings.Index(sql, "SELECT ")
	require.NotEqual(t, -1, start, "no SELECT in %s", sql)
	rest := sql[start:]
	end := strings.Index(rest, " FROM ")
	require.NotEqual(t, -1, end, "no FROM in %s", sql)
	return rest[:end]
}

func TestBuildBaseExportSQL_WithSchemaCacheProjectsColumns(t *testing.T) {
	attrCache := forma.SchemaAttributeCache{
		"name": {
			AttributeName: "name",
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"flag": {
			AttributeName: "flag",
			AttributeID:   11,
			ValueType:     forma.ValueTypeBool,
		},
	}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, []uuid.UUID{rowID}, attrCache)
	if err != nil {
		t.Fatalf("buildBaseExportSQL returned error: %v", err)
	}

	if !strings.Contains(mQuery, "text_01") {
		t.Fatalf("main query missing bound column: %s", mQuery)
	}
	if !strings.Contains(sql, "m.text_01") || !strings.Contains(sql, "name") {
		t.Fatalf("sql missing main projection for name: %s", sql)
	}
	if !strings.Contains(eQuery, "attr_id IN (11)") {
		t.Fatalf("eav query missing attr_id filter: %s", eQuery)
	}
	if !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing eav projection for flag: %s", sql)
	}
}

func TestBuildBaseExportSQL_UsesCustomTableNames(t *testing.T) {
	cfg := CDCConfig{
		EntityMainTable: "entity_main_dev",
		EAVDataTable:    "eav_data_dev",
	}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	_, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", cfg, 1, []uuid.UUID{rowID}, testAttrCache())
	if err != nil {
		t.Fatalf("buildBaseExportSQL returned error: %v", err)
	}

	if !strings.Contains(mQuery, `FROM "entity_main_dev"`) {
		t.Fatalf("main query not using custom table name: %s", mQuery)
	}
	if !strings.Contains(eQuery, `FROM "eav_data_dev"`) {
		t.Fatalf("eav query not using custom table name: %s", eQuery)
	}
}
