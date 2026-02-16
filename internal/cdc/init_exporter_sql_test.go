package cdc

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestBuildBaseExportSQL_UsesRowIDsAndConfig(t *testing.T) {
	cfg := CDCConfig{EntityMainTable: "entity_main_dev", DuckMemLimit: "6GB", ParquetCompression: "zstd", ParquetCompressionLevel: 5}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", cfg, 1, []uuid.UUID{rowID}, nil)
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
	if !strings.Contains(sql, "time_slot") || !strings.Contains(sql, "attributes") {
		t.Fatalf("sql missing projected columns (time_slot/attributes): %s", sql)
	}
}

func TestBuildBaseExportSQL_ErrorsOnEmptyRowIDs(t *testing.T) {
	_, _, _, err := buildBaseExportSQL("pg", "s3://bucket/base/1/_tmp/tmp.parquet", CDCConfig{}, 1, nil, nil)
	if err == nil {
		t.Fatalf("expected error for empty row ids")
	}
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

	_, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", cfg, 1, []uuid.UUID{rowID}, nil)
	if err != nil {
		t.Fatalf("buildBaseExportSQL returned error: %v", err)
	}

	if !strings.Contains(mQuery, "FROM entity_main_dev") {
		t.Fatalf("main query not using custom table name: %s", mQuery)
	}
	if !strings.Contains(eQuery, "FROM eav_data_dev") {
		t.Fatalf("eav query not using custom table name: %s", eQuery)
	}
}
