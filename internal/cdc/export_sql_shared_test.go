package cdc

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
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

func TestResolveExportSQLOptions_AppliesDefaultsWhenConfigOmitsOverrides(t *testing.T) {
	opts := resolveExportSQLOptions(CDCConfig{}, "4GB")

	require.Equal(t, DefaultParquetCompression, opts.compression)
	require.Equal(t, DefaultParquetCompressionLevel, opts.compressionLevel)
	require.Equal(t, "4GB", opts.memoryLimit)
	require.Equal(t, defaultParquetVersion, opts.parquetVersion)
}

func TestResolveMainAndEAVTableNames_QuotesUnsafeNamesAndFallsBackWhenEmpty(t *testing.T) {
	mainTable, eavTable := resolveMainAndEAVTableNames(CDCConfig{
		EntityMainTable: "entity-main prod",
		EAVDataTable:    "",
	})

	require.Equal(t, `"entity-main prod"`, mainTable)
	require.Equal(t, "eav_data", eavTable)
}

func TestBuildMainEntityQuery_OmitsDeletedGuardWhenActiveOnlyIsFalse(t *testing.T) {
	query := buildMainEntityQuery("entity_main", 7, []string{"ltbase_row_id", "text_01"}, "ltbase_row_id IS NOT NULL", false)

	require.Contains(t, query, "FROM entity_main")
	require.NotContains(t, query, "ltbase_deleted_at IS NULL")
	require.Contains(t, query, "AND ltbase_row_id IS NOT NULL")
}

func TestBuildMainEntityQuery_IncludesDeletedGuardWhenActiveOnlyIsTrue(t *testing.T) {
	query := buildMainEntityQuery("entity_main", 7, []string{"ltbase_row_id", "text_01"}, "ltbase_row_id IS NOT NULL", true)

	require.Contains(t, query, "FROM entity_main")
	require.Contains(t, query, "ltbase_deleted_at IS NULL")
	require.Contains(t, query, "AND ltbase_row_id IS NOT NULL")
}

func TestBuildEAVQuery_OmitsAttrFilterWhenAttrIDsAreEmpty(t *testing.T) {
	query := buildEAVQuery("eav_data", 9, "row_id IS NOT NULL", nil)

	require.Contains(t, query, "FROM eav_data")
	require.NotContains(t, query, "attr_id IN")
	require.Contains(t, query, "AND row_id IS NOT NULL")
}

func TestBuildSchemaDrivenProjection_DeduplicatesMainColumnsAndSeparatesEAVAggregates(t *testing.T) {
	projection := buildSchemaDrivenProjection(forma.SchemaAttributeCache{
		"display_name": {
			AttributeName: "display_name",
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"legal_name": {
			AttributeName: "legal_name",
			AttributeID:   11,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"is_active": {
			AttributeName: "is_active",
			AttributeID:   12,
			ValueType:     forma.ValueTypeBool,
		},
	})

	mainColumnCount := 0
	for _, col := range projection.mainColumns {
		if col == string(forma.MainColumnText01) {
			mainColumnCount++
		}
	}

	require.Equal(t, 1, mainColumnCount)
	require.Len(t, projection.mainProjections, 2)
	require.Contains(t, projection.mainProjections[0], "AS display_name")
	require.Contains(t, projection.mainProjections[1], "AS legal_name")
	require.Len(t, projection.eavAgg, 1)
	require.Contains(t, projection.eavAgg[0], "attr_id = 12")
	require.Contains(t, projection.eavAgg[0], "AS is_active")
	require.Equal(t, []int16{12}, projection.eavAttrIDs)
}

func TestBuildParquetCopyOptions_FormatsResolvedOptionsForDuckDB(t *testing.T) {
	options := buildParquetCopyOptions(exportSQLOptions{
		compression:      "zstd",
		compressionLevel: 7,
		memoryLimit:      "3GB",
		parquetVersion:   "V2",
	})

	require.Equal(t, "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 7", options)
}

func TestBuildSchemaDrivenProjection_MixedBoundAndUnboundAttributesKeepCastsAliasesAndAttrIDsAligned(t *testing.T) {
	projection := buildSchemaDrivenProjection(forma.SchemaAttributeCache{
		"display_name": {
			AttributeName: "display_name",
			AttributeID:   20,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"employee_count": {
			AttributeName: "employee_count",
			AttributeID:   21,
			ValueType:     forma.ValueTypeInteger,
		},
		"is_active": {
			AttributeName: "is_active",
			AttributeID:   22,
			ValueType:     forma.ValueTypeBool,
		},
	})

	require.Contains(t, projection.mainColumns, string(forma.MainColumnText01))
	require.Len(t, projection.mainProjections, 1)
	require.Contains(t, projection.mainProjections[0], "CAST(m.text_01 AS VARCHAR)")
	require.Contains(t, projection.mainProjections[0], "AS display_name")
	require.Len(t, projection.eavAgg, 2)
	require.Contains(t, projection.eavAgg[0], "attr_id = 21")
	require.Contains(t, projection.eavAgg[0], "TRY_CAST(value_text AS INTEGER)")
	require.Contains(t, projection.eavAgg[0], "AS employee_count")
	require.Contains(t, projection.eavAgg[1], "attr_id = 22")
	require.Contains(t, projection.eavAgg[1], "lower(value_text) IN ('true','1','t','yes','y')")
	require.Contains(t, projection.eavAgg[1], "ELSE NULL END")
	require.Contains(t, projection.eavAgg[1], "AS is_active")
	require.Equal(t, []string{"e.employee_count", "e.is_active"}, projection.eavSelect)
	require.Equal(t, []int16{21, 22}, projection.eavAttrIDs)
}

func TestBuildSchemaDrivenProjection_PreservesProjectionSplitAcrossBoundAndUnboundAttributes(t *testing.T) {
	projection := buildSchemaDrivenProjection(forma.SchemaAttributeCache{
		"company_name": {
			AttributeName: "company_name",
			AttributeID:   30,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02},
		},
		"annual_revenue": {
			AttributeName: "annual_revenue",
			AttributeID:   31,
			ValueType:     forma.ValueTypeNumeric,
		},
	})

	require.Contains(t, projection.mainColumns, string(forma.MainColumnText02))
	require.Equal(t, []string{"CAST(m.text_02 AS VARCHAR) AS company_name"}, projection.mainProjections)
	require.Equal(t, []string{"MAX(CASE WHEN attr_id = 31 THEN TRY_CAST(value_text AS DOUBLE) END) AS annual_revenue"}, projection.eavAgg)
	require.Equal(t, []string{"e.annual_revenue"}, projection.eavSelect)
	require.Equal(t, []int16{31}, projection.eavAttrIDs)
}

func TestBuildEAVAggregationSQL_GroupsByRowIDWhenNoAggregatesExist(t *testing.T) {
	query := buildEAVAggregationSQL("SELECT row_id FROM eav_data", nil)

	require.Contains(t, query, "SELECT row_id FROM postgres_query")
	require.Contains(t, query, "GROUP BY row_id")
	require.NotContains(t, query, ",\n    MAX(")
}
