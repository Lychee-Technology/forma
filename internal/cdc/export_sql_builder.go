package cdc

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

type exportSQLOptions struct {
	compression      string
	compressionLevel int
	memoryLimit      string
	parquetVersion   string
}

const defaultParquetVersion = "V2"

func resolveExportSQLOptions(cfg CDCConfig, defaultMemoryLimit string) exportSQLOptions {
	compression := cfg.ParquetCompression
	if compression == "" {
		compression = DefaultParquetCompression
	}

	compressionLevel := cfg.ParquetCompressionLevel
	if compressionLevel <= 0 {
		compressionLevel = DefaultParquetCompressionLevel
	}

	memoryLimit := cfg.DuckMemLimit
	if memoryLimit == "" {
		memoryLimit = defaultMemoryLimit
	}

	return exportSQLOptions{
		compression:      compression,
		compressionLevel: compressionLevel,
		memoryLimit:      memoryLimit,
		parquetVersion:   defaultParquetVersion,
	}
}

func buildParquetCopyOptions(opts exportSQLOptions) string {
	return fmt.Sprintf(
		"FORMAT PARQUET, PARQUET_VERSION %s, COMPRESSION '%s', COMPRESSION_LEVEL %d",
		opts.parquetVersion,
		strings.ToUpper(opts.compression),
		opts.compressionLevel,
	)
}

func resolveMainAndEAVTableNames(cfg CDCConfig) (string, string) {
	entityMain := sanitizeIdentifier(cfg.EntityMainTable)
	if entityMain == "" {
		entityMain = "entity_main"
	}
	eavData := sanitizeIdentifier(cfg.EAVDataTable)
	if eavData == "" {
		eavData = "eav_data"
	}

	return entityMain, eavData
}

func buildMainEntityQuery(entityMain string, schemaID int16, mainColumns []string, mainFilter string, activeOnly bool) string {
	if activeOnly {
		return fmt.Sprintf(
			"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND ltbase_deleted_at IS NULL AND %s",
			strings.Join(mainColumns, ", "), entityMain, schemaID, mainFilter,
		)
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND %s",
		strings.Join(mainColumns, ", "), entityMain, schemaID, mainFilter,
	)
}

func buildEAVQuery(eavData string, schemaID int16, eavFilter string, eavAttrIDs []int16) string {
	attrFilter := ""
	if len(eavAttrIDs) > 0 {
		attrFilter = fmt.Sprintf(" AND attr_id IN (%s)", joinInt16(eavAttrIDs))
	}
	return fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, value_text, value_numeric FROM %s WHERE schema_id = %d AND %s%s",
		eavData, schemaID, eavFilter, attrFilter,
	)
}

type schemaDrivenProjection struct {
	mainColumns     []string
	mainProjections []string
	eavAgg          []string
	eavSelect       []string
	eavAttrIDs      []int16
}

func buildSchemaDrivenProjection(attrCache forma.SchemaAttributeCache) schemaDrivenProjection {
	mainColumns := []string{
		"ltbase_row_id",
		"ltbase_schema_id",
		"ltbase_created_at",
		"ltbase_updated_at",
		"ltbase_deleted_at",
	}
	mainColSet := map[string]struct{}{
		"ltbase_row_id":     {},
		"ltbase_schema_id":  {},
		"ltbase_created_at": {},
		"ltbase_updated_at": {},
		"ltbase_deleted_at": {},
	}

	mainProjections := make([]string, 0, len(attrCache))
	eavAgg := make([]string, 0, len(attrCache))
	eavSelect := make([]string, 0, len(attrCache))
	eavAttrIDs := make([]int16, 0, len(attrCache))

	for _, attrName := range sortedAttrKeys(attrCache) {
		meta := attrCache[attrName]
		alias := safeColumnAlias(attrName)
		if meta.ColumnBinding != nil {
			colName := string(meta.ColumnBinding.ColumnName)
			if _, ok := mainColSet[colName]; !ok {
				mainColumns = append(mainColumns, colName)
				mainColSet[colName] = struct{}{}
			}

			expr := castMainValue("m."+colName, meta)
			mainProjections = append(mainProjections, fmt.Sprintf("%s AS %s", expr, alias))
			continue
		}

		castExpr := castEAVValue(meta)
		eavAgg = append(eavAgg, fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END) AS %s", meta.AttributeID, castExpr, alias))
		eavSelect = append(eavSelect, fmt.Sprintf("e.%s", alias))
		eavAttrIDs = append(eavAttrIDs, meta.AttributeID)
	}

	return schemaDrivenProjection{
		mainColumns:     mainColumns,
		mainProjections: mainProjections,
		eavAgg:          eavAgg,
		eavSelect:       eavSelect,
		eavAttrIDs:      eavAttrIDs,
	}
}

func buildEAVAggregationSQL(eQueryEsc string, eavAgg []string) string {
	eAggCols := []string{"row_id"}
	if len(eavAgg) > 0 {
		eAggCols = append(eAggCols, eavAgg...)
	}

	return fmt.Sprintf(
		"SELECT %s FROM postgres_query('pg_db', '%s') GROUP BY row_id",
		strings.Join(eAggCols, ",\n    "),
		eQueryEsc,
	)
}
