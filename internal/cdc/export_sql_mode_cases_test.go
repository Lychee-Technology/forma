package cdc

import (
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// exportSQLModeCase is one export mode (delta or base) plus the mode-specific
// SQL shapes TestBuildExportSQL_CommonSemanticsAcrossModes pins.
type exportSQLModeCase struct {
	name                  string
	build                 func(attrCache forma.SchemaAttributeCache) (exportSQLResult, error)
	expectedMemoryLimit   string
	expectedModeTable     string
	expectedTimeSlotExpr  string
	expectedDeletedAtExpr string
	expectActiveOnly      bool
	expectChangeLogInMode bool
}

func exportSQLAttrCacheFixture() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
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
		"joined": {
			AttributeName: "joined",
			AttributeID:   12,
			ValueType:     forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint02, Encoding: forma.MainColumnEncodingUnixMs},
		},
		"touched": {
			AttributeName: "touched",
			AttributeID:   13,
			ValueType:     forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint03, Encoding: forma.MainColumnEncodingUnixMs},
		},
	}
}

func exportSQLModeCases(rowID uuid.UUID) []exportSQLModeCase {
	return []exportSQLModeCase{
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
			expectedTimeSlotExpr:  "cl.changed_at AS changed_at",
			expectedDeletedAtExpr: "COALESCE(cl.deleted_at, 0) AS deleted_at",
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
			expectedMemoryLimit:   "6GB",
			expectedModeTable:     "",
			expectedTimeSlotExpr:  "m.ltbase_updated_at AS changed_at",
			expectedDeletedAtExpr: "COALESCE(m.ltbase_deleted_at, 0) AS deleted_at",
			expectActiveOnly:      true,
		},
	}
}
