package internal

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// SchemaProjection holds precomputed SQL fragments for the DuckDB federated query
// template, derived from a schema's attribute metadata cache.
type SchemaProjection struct {
	// S3SourceSelect is the SELECT projection for the s3_source CTE:
	//   row_id, created_at, ver_ts, deleted_ts, attr_col_1, attr_col_2, ...
	S3SourceSelect string

	// PGSourceSelect is the SELECT projection for the pg_source CTE, including
	// COALESCE expressions for entity_main columns and EAV pivot expressions.
	PGSourceSelect string

	// PGGroupBy is the GROUP BY clause for the pg_source CTE.
	PGGroupBy string

	// EAVPivotSelect is the EAV subquery SELECT for the pg_source LEFT JOIN,
	// containing MAX(CASE WHEN attr_id = X THEN value_Y END) AS attr_name lines.
	EAVPivotSelect string

	// EAVPivotAttrs is the comma-separated list of attr_ids for the EAV pivot WHERE.
	EAVPivotAttrs string

	// OuterSelect is the final outer SELECT that maps unified columns back to
	// entity_main column descriptors.
	OuterSelect string

	// UnifiedColumnNames lists the canonical column names produced by both
	// s3_source and pg_source (in order).
	UnifiedColumnNames []string

	// UnifiedColumnTypes maps column name to DuckDB type for CAST expressions.
	UnifiedColumnTypes map[string]forma.ValueType

	// AttrToMainColumn maps attribute name to entity_main column name.
	AttrToMainColumn map[string]string

	// EAVAttrs lists attribute names that are EAV-only (no column binding).
	EAVAttrs []string

	// EAVAttrsJSON is the JSON object expression for EAV attributes in the
	// outer SELECT, e.g. json_object('attr_id', '8', 'value', exchange, ...).
	EAVAttrsJSONSelect string

	// HasEAVAttrs is true if the schema has any EAV-only attributes.
	HasEAVAttrs bool

	// attrIDs maps attribute name to its attribute ID.
	attrIDs map[string]int
}

const (
	benchmarkParquetSchemaIDStart int16 = 100
	benchmarkParquetSchemaIDEnd   int16 = 102
)

// isBenchmarkSchemaID returns true for the fixed benchmark schema ID range.
func isBenchmarkSchemaID(schemaID int16) bool {
	return schemaID >= benchmarkParquetSchemaIDStart && schemaID <= benchmarkParquetSchemaIDEnd
}

// attrProjectionInfo holds the projection-relevant metadata for one schema attribute.
type attrProjectionInfo struct {
	name     string
	attrID   int
	meta     forma.AttributeMetadata
	isColumn bool
}

// BuildSchemaProjection computes SQL fragments for the DuckDB template from the
// schema attribute cache. It handles both production and benchmark parquet shapes.
func BuildSchemaProjection(schemaID int16, cache forma.SchemaAttributeCache) (*SchemaProjection, error) {
	sp := &SchemaProjection{
		UnifiedColumnNames: []string{
			"row_id", "created_at", "ver_ts", "deleted_ts",
		},
		UnifiedColumnTypes: map[string]forma.ValueType{
			"row_id":     forma.ValueTypeUUID,
			"created_at": forma.ValueTypeBigInt,
			"ver_ts":     forma.ValueTypeBigInt,
			"deleted_ts": forma.ValueTypeBigInt,
		},
		AttrToMainColumn: make(map[string]string),
		attrIDs:          make(map[string]int),
	}

	// Collect column-bound attributes and EAV-only attributes
	attrs := make([]attrProjectionInfo, 0, len(cache))
	for name, meta := range cache {
		ai := attrProjectionInfo{name: name, meta: meta}
		ai.attrID = int(meta.AttributeID)
		sp.attrIDs[name] = int(meta.AttributeID)
		if meta.ColumnBinding != nil {
			ai.isColumn = true
			sp.AttrToMainColumn[name] = string(meta.ColumnBinding.ColumnName)
		} else {
			sp.EAVAttrs = append(sp.EAVAttrs, name)
		}
		sp.UnifiedColumnNames = append(sp.UnifiedColumnNames, name)
		sp.UnifiedColumnTypes[name] = meta.ValueType
		attrs = append(attrs, ai)
	}
	sort.Strings(sp.EAVAttrs)
	sp.HasEAVAttrs = len(sp.EAVAttrs) > 0

	// Build unified column list (system + attribute names, sorted for stability)
	sortedAttrNames := make([]string, 0, len(attrs))
	for _, a := range attrs {
		sortedAttrNames = append(sortedAttrNames, a.name)
	}
	sort.Strings(sortedAttrNames)

	// Build S3 source projection
	sp.buildS3Projection(sortedAttrNames)

	// Build PG source projection
	sp.buildPGProjection(attrs)

	// Build EAV pivot
	sp.buildEAVPivot(attrs)

	// Build outer SELECT
	sp.buildOuterSelect(schemaID, sortedAttrNames)

	return sp, nil
}

func (sp *SchemaProjection) buildS3Projection(sortedAttrs []string) {
	parts := make([]string, 0, 4+len(sortedAttrs))
	// System columns from parquet
	parts = append(parts,
		"row_id",
		"changed_at AS created_at",
		"changed_at AS ver_ts",
		"deleted_at AS deleted_ts",
	)
	for _, attr := range sortedAttrs {
		parts = append(parts, attr)
	}
	sp.S3SourceSelect = strings.Join(parts, ", ")
}

func (sp *SchemaProjection) buildPGProjection(attrs []attrProjectionInfo) {
	// Build PG source projection and group-by columns
	selectParts := make([]string, 0, 4+len(attrs))
	groupParts := make([]string, 0, 4+len(attrs))

	// System columns from PG
	selectParts = append(selectParts,
		"cl.row_id::VARCHAR AS row_id",
		"m.ltbase_created_at AS created_at",
		"cl.changed_at AS ver_ts",
		"cl.deleted_at AS deleted_ts",
	)
	groupParts = append(groupParts,
		"cl.row_id",
		"m.ltbase_created_at",
		"cl.changed_at",
		"cl.deleted_at",
	)

	sort.Slice(attrs, func(i, j int) bool { return attrs[i].name < attrs[j].name })
	for _, a := range attrs {
		if a.isColumn {
			colName := string(a.meta.ColumnBinding.ColumnName)
			expr := fmt.Sprintf("COALESCE(hot_vals.%s, m.%s) AS %s",
				a.name, colName, a.name)
			selectParts = append(selectParts, expr)
			groupParts = append(groupParts, "m."+colName)
		} else {
			expr := fmt.Sprintf("hot_vals.%s AS %s", a.name, a.name)
			selectParts = append(selectParts, expr)
		}
	}
	sp.PGSourceSelect = strings.Join(selectParts, ",\n\t\t\t")
	sp.PGGroupBy = strings.Join(groupParts, ", ")
}

func (sp *SchemaProjection) buildEAVPivot(attrs []attrProjectionInfo) {
	var pivotParts []string
	var attrIDParts []string

	sort.Slice(attrs, func(i, j int) bool { return attrs[i].name < attrs[j].name })
	for _, a := range attrs {
		attrIDParts = append(attrIDParts, fmt.Sprintf("%d", a.attrID))
		pivotParts = append(pivotParts, fmt.Sprintf(
			"\t\t\t\tMAX(CASE WHEN attr_id = %d THEN %s END) AS %s",
			a.attrID, eavValueColumn(a.meta.ValueType), a.name))
	}

	if len(pivotParts) > 0 {
		sp.EAVPivotSelect = strings.Join(pivotParts, ",\n")
	}
	if len(attrIDParts) > 0 {
		sp.EAVPivotAttrs = strings.Join(attrIDParts, ", ")
	}
}

// eavValueColumn returns the appropriate eav_data column for a given value type.
func eavValueColumn(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "value_numeric"
	default:
		return "value_text"
	}
}

func (sp *SchemaProjection) buildOuterSelect(schemaID int16, sortedAttrs []string) {
	parts := make([]string, 0, 5+len(sp.AttrToMainColumn)+3)

	// System columns
	parts = append(parts,
		fmt.Sprintf("%d::SMALLINT AS ltbase_schema_id", schemaID),
		"CAST(row_id AS UUID) AS ltbase_row_id",
		"created_at AS ltbase_created_at",
		"ver_ts AS ltbase_updated_at",
		"deleted_ts AS ltbase_deleted_at",
	)

	// Attribute columns mapped to entity_main columns
	mainColToAttr := make(map[string]string)
	for attr, col := range sp.AttrToMainColumn {
		mainColToAttr[col] = attr
	}

	// Build attributes_json
	var jsonParts []string
	eavOnlySelects := make(map[string]bool)
	for _, attr := range sp.EAVAttrs {
		eavOnlySelects[attr] = true
	}

	for _, attr := range sortedAttrs {
		col, isColumn := sp.AttrToMainColumn[attr]
		if isColumn {
			// Map attribute to its entity_main column with appropriate CAST
			parts = append(parts, fmt.Sprintf("%s AS %s",
				duckDBAttrCast(attr, sp.UnifiedColumnTypes[attr]), col))
		}
	}

	// For entity_main columns not mapped to any attribute, emit NULL
	allMainCols := entityMainColumnDescriptors[len(systemColumnDescriptors):]
	mappedCols := make(map[string]bool)
	for _, col := range sp.AttrToMainColumn {
		mappedCols[col] = true
	}
	for _, desc := range allMainCols {
		if mappedCols[desc.name] {
			continue
		}
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s",
			duckDBColumnType(desc.kind), desc.name))
	}

	// Build attributes_json for EAV-only attributes
	for _, attr := range sortedAttrs {
		if !eavOnlySelects[attr] {
			continue
		}
		jsonParts = append(jsonParts, fmt.Sprintf(
			"'%d', CAST(%s AS VARCHAR)",
			sp.attrIDForName(attr), duckDBAttrCast(attr, sp.UnifiedColumnTypes[attr])))
	}

	if len(jsonParts) > 0 {
		j := "json_object(" + strings.Join(jsonParts, ", ") + ")"
		parts = append(parts, j+"::TEXT AS attributes_json")
	} else {
		parts = append(parts, "'[]'::TEXT AS attributes_json")
	}

	// Metadata columns
	parts = append(parts,
		"COUNT(DISTINCT row_id) OVER() AS total_records",
		"CEIL(COUNT(DISTINCT row_id) OVER()::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0))::BIGINT AS total_pages",
		"(FLOOR({{.OFFSET}}::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0)) + 1)::BIGINT AS current_page",
	)

	sp.OuterSelect = strings.Join(parts, ",\n\t\t\t")
}

func (sp *SchemaProjection) attrIDForName(name string) int {
	if id, ok := sp.attrIDs[name]; ok {
		return id
	}
	return 0
}

func duckDBAttrCast(attr string, vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return fmt.Sprintf("CAST(%s AS VARCHAR)", attr)
	case forma.ValueTypeSmallInt:
		return fmt.Sprintf("CAST(%s AS SMALLINT)", attr)
	case forma.ValueTypeInteger:
		return fmt.Sprintf("CAST(%s AS INTEGER)", attr)
	case forma.ValueTypeBigInt:
		return fmt.Sprintf("CAST(%s AS BIGINT)", attr)
	case forma.ValueTypeNumeric:
		return fmt.Sprintf("CAST(%s AS DOUBLE)", attr)
	case forma.ValueTypeBool:
		return fmt.Sprintf("CAST(%s AS BOOLEAN)", attr)
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return fmt.Sprintf("CAST(%s AS BIGINT)", attr)
	default:
		return fmt.Sprintf("CAST(%s AS VARCHAR)", attr)
	}
}

func duckDBColumnType(k columnKind) string {
	switch k {
	case columnKindText:
		return "VARCHAR"
	case columnKindSmallint:
		return "SMALLINT"
	case columnKindInteger:
		return "INTEGER"
	case columnKindBigint:
		return "BIGINT"
	case columnKindDouble:
		return "DOUBLE"
	case columnKindUUID:
		return "UUID"
	default:
		return "VARCHAR"
	}
}

// BuildBenchmarkS3Projection builds an S3 projection for benchmark parquet data,
// which has flat attribute columns (unlike production parquet which uses entity_main
// columns + attributes_json).
func BuildBenchmarkS3Projection(schemaID int16) string {
	parts := []string{
		"row_id",
		"changed_at AS created_at",
		"changed_at AS ver_ts",
		"deleted_at AS deleted_ts",
	}

	// Benchmark parquet has all attributes as flat columns
	switch schemaID {
	case 100: // customer
		parts = append(parts, "name", "version", "taxId", "status", "region", "email", "creditRating")
	case 101: // security
		parts = append(parts, "name", "version", "symbol", "sector", "companyName", "dividend", "marketCap")
	case 102: // trade
		parts = append(parts, "name", "version", "symbol", "exchange", "region",
			"tradeType", "tradeTime", "customerId", "quantity", "price",
			"commission", "isCash", "brokerId", "orderChannel")
	}

	return strings.Join(parts, ", ")
}

// BuildBenchmarkOuterSelect builds an outer SELECT for benchmark parquet data
// that maps flat attribute columns to entity_main column descriptors.
func BuildBenchmarkOuterSelect(schemaID int16) string {
	parts := []string{
		fmt.Sprintf("%d::SMALLINT AS ltbase_schema_id", schemaID),
		"CAST(row_id AS UUID) AS ltbase_row_id",
		"created_at AS ltbase_created_at",
		"ver_ts AS ltbase_updated_at",
		"deleted_ts AS ltbase_deleted_at",
	}

	// Map known benchmark attributes to entity_main columns
	switch schemaID {
	case 102: // trade
		// text_01 = symbol
		parts = append(parts, "CAST(symbol AS VARCHAR) AS text_01")
		// text_02 = region
		parts = append(parts, "CAST(region AS VARCHAR) AS text_02")
		// smallint_01 = tradeType
		parts = append(parts, "CAST(tradeType AS SMALLINT) AS smallint_01")
		// bigint_01 = quantity
		parts = append(parts, "CAST(quantity AS BIGINT) AS bigint_01")
		// bigint_02 = tradeTime
		parts = append(parts, "CAST(tradeTime AS BIGINT) AS bigint_02")
		// double_01 = price
		parts = append(parts, "CAST(price AS DOUBLE) AS double_01")
		// uuid_01 = customerId
		parts = append(parts, "CAST(customerId AS UUID) AS uuid_01")
		// NULL for remaining entity_main columns
		remaining := restMainColumnNames([]string{
			"text_01", "text_02", "smallint_01", "bigint_01", "bigint_02",
			"double_01", "uuid_01",
		})
		for _, col := range remaining {
			desc := getMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.kind), col))
		}
		// attributes_json from EAV-only attributes
		parts = append(parts, "json_object('8', CAST(exchange AS VARCHAR), '9', CAST(commission AS VARCHAR), '10', CAST(isCash AS VARCHAR), '11', CAST(brokerId AS VARCHAR), '12', CAST(orderChannel AS VARCHAR))::TEXT AS attributes_json")
	case 100: // customer
		parts = append(parts, "CAST(taxId AS VARCHAR) AS text_01")
		parts = append(parts, "CAST(region AS VARCHAR) AS text_02")
		parts = append(parts, "CAST(status AS SMALLINT) AS smallint_01")
		remaining := restMainColumnNames([]string{"text_01", "text_02", "smallint_01"})
		for _, col := range remaining {
			desc := getMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.kind), col))
		}
		parts = append(parts, "json_object('3', CAST(name AS VARCHAR), '5', CAST(email AS VARCHAR), '6', CAST(creditRating AS VARCHAR))::TEXT AS attributes_json")
	case 101: // security
		parts = append(parts, "CAST(symbol AS VARCHAR) AS text_01")
		parts = append(parts, "CAST(sector AS SMALLINT) AS smallint_01")
		remaining := restMainColumnNames([]string{"text_01", "smallint_01"})
		for _, col := range remaining {
			desc := getMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.kind), col))
		}
		parts = append(parts, "json_object('3', CAST(companyName AS VARCHAR), '4', CAST(dividend AS VARCHAR), '5', CAST(marketCap AS VARCHAR))::TEXT AS attributes_json")
	}

	parts = append(parts,
		"COUNT(DISTINCT row_id) OVER() AS total_records",
		"CEIL(COUNT(DISTINCT row_id) OVER()::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0))::BIGINT AS total_pages",
		"(FLOOR({{.OFFSET}}::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0)) + 1)::BIGINT AS current_page",
	)

	return strings.Join(parts, ",\n\t\t\t")
}

// restMainColumnNames returns all entity_main EAV column names except those listed in exclude.
func restMainColumnNames(exclude []string) []string {
	excludeSet := make(map[string]bool)
	for _, col := range exclude {
		excludeSet[col] = true
	}
	var result []string
	for _, desc := range entityMainColumnDescriptors[len(systemColumnDescriptors):] {
		if !excludeSet[desc.name] {
			result = append(result, desc.name)
		}
	}
	return result
}

// BuildSchemaDrivenTemplateParams computes template parameters from a SchemaProjection.
func BuildSchemaDrivenTemplateParams(sp *SchemaProjection, schemaID int16) map[string]any {
	return map[string]any{
		"S3SourceSelect": sp.S3SourceSelect,
		"PGSourceSelect": sp.PGSourceSelect,
		"PGGroupBy":      sp.PGGroupBy,
		"EAVPivotSelect": sp.EAVPivotSelect,
		"EAVPivotAttrs":  sp.EAVPivotAttrs,
		"OuterSelect":    sp.OuterSelect,
		"HasEAVAttrs":    sp.HasEAVAttrs,
		"HasEAVPivot":    len(sp.EAVPivotAttrs) > 0,
	}
}
