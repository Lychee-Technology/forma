package cdc

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type exportSQLResult struct {
	sql     string
	modeSQL string
	mQuery  string
	eQuery  string
}

func TestBuildExportSQL_CommonSemanticsAcrossModes(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
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

	tests := []struct {
		name                  string
		build                 func(attrCache forma.SchemaAttributeCache) (exportSQLResult, error)
		expectedMemoryLimit   string
		expectedModeTable     string
		expectedTimeSlotExpr  string
		expectActiveOnly      bool
		expectChangeLogInMode bool
	}{
		{
			name: "delta",
			build: func(attrCache forma.SchemaAttributeCache) (exportSQLResult, error) {
				cfg := CDCConfig{
					ChangeLogTable:          "change_log_dev",
					EntityMainTable:         "entity_main_dev",
					EAVDataTable:            "eav_data_dev",
					DuckMemLimit:            "2GB",
					ParquetCompression:      "zstd",
					ParquetCompressionLevel: 5,
				}
				sql, clQuery, mQuery, eQuery, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", cfg, 1, 1700000000000, []uuid.UUID{rowID}, attrCache)
				return exportSQLResult{sql: sql, modeSQL: clQuery, mQuery: mQuery, eQuery: eQuery}, err
			},
			expectedMemoryLimit:   "2GB",
			expectedModeTable:     `FROM "change_log_dev"`,
			expectedTimeSlotExpr:  "cl.changed_at AS time_slot",
			expectChangeLogInMode: true,
		},
		{
			name: "base",
			build: func(attrCache forma.SchemaAttributeCache) (exportSQLResult, error) {
				cfg := CDCConfig{
					EntityMainTable:         "entity_main_dev",
					EAVDataTable:            "eav_data_dev",
					DuckMemLimit:            "6GB",
					ParquetCompression:      "zstd",
					ParquetCompressionLevel: 5,
				}
				sql, mQuery, eQuery, err := buildBaseExportSQL("host=pg", "s3://bucket/base/1/_tmp/tmp.parquet", cfg, 1, []uuid.UUID{rowID}, attrCache)
				return exportSQLResult{sql: sql, mQuery: mQuery, eQuery: eQuery}, err
			},
			expectedMemoryLimit:  "6GB",
			expectedModeTable:    "",
			expectedTimeSlotExpr: "m.ltbase_created_at AS time_slot",
			expectActiveOnly:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/generic", func(t *testing.T) {
			res, err := tt.build(nil)
			if err != nil {
				t.Fatalf("build failed: %v", err)
			}

			rowFilter := "row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')"
			if !strings.Contains(res.mQuery, "ltbase_row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')") {
				t.Fatalf("main query missing row filter: %s", res.mQuery)
			}
			if !strings.Contains(res.eQuery, rowFilter) {
				t.Fatalf("eav query missing row filter: %s", res.eQuery)
			}
			if !strings.Contains(res.sql, "PRAGMA memory_limit='"+tt.expectedMemoryLimit+"'") {
				t.Fatalf("sql missing configured memory limit: %s", res.sql)
			}
			if !strings.Contains(res.sql, "PARQUET_VERSION V2") {
				t.Fatalf("sql missing parquet v2 export option: %s", res.sql)
			}
			if !strings.Contains(res.sql, tt.expectedTimeSlotExpr) {
				t.Fatalf("sql missing expected time_slot projection %q: %s", tt.expectedTimeSlotExpr, res.sql)
			}
			if tt.expectChangeLogInMode && !strings.Contains(res.modeSQL, tt.expectedModeTable) {
				t.Fatalf("mode query missing expected table %q: %s", tt.expectedModeTable, res.modeSQL)
			}
			if !strings.Contains(res.mQuery, `FROM "entity_main_dev"`) {
				t.Fatalf("main query not using custom table name: %s", res.mQuery)
			}
			if !strings.Contains(res.eQuery, `FROM "eav_data_dev"`) {
				t.Fatalf("eav query not using custom table name: %s", res.eQuery)
			}
			if tt.expectActiveOnly && !strings.Contains(res.mQuery, "ltbase_deleted_at IS NULL") {
				t.Fatalf("main query missing active-only filter: %s", res.mQuery)
			}
		})

		t.Run(tt.name+"/schema-driven", func(t *testing.T) {
			res, err := tt.build(attrCache)
			if err != nil {
				t.Fatalf("build failed: %v", err)
			}

			if !strings.Contains(res.mQuery, "text_01") {
				t.Fatalf("main query missing bound column: %s", res.mQuery)
			}
			if !strings.Contains(res.sql, "name") || !strings.Contains(res.sql, "flag") {
				t.Fatalf("sql missing schema-driven projections: %s", res.sql)
			}
			if !strings.Contains(res.eQuery, "attr_id IN (11)") {
				t.Fatalf("eav query missing attr_id filter: %s", res.eQuery)
			}
		})
	}
}
