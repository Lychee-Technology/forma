package sqlgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

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

func IsBenchmarkSchemaID(schemaID int16) bool {
	return isBenchmarkSchemaID(schemaID)
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

	// Work on a local copy: the caller's cache is a shared, read-only
	// registry snapshot and must never be mutated here (#142 phase 3).
	local := make(forma.SchemaAttributeCache, len(cache)+2)
	for name, meta := range cache {
		local[name] = meta
	}
	cache = local

	// Collect column-bound attributes and EAV-only attributes
	ensureAttr := func(name string, vt forma.ValueType) {
		if _, exists := cache[name]; exists {
			return
		}
		cache[name] = forma.AttributeMetadata{ValueType: vt}
	}
	ensureAttr("name", forma.ValueTypeText)
	ensureAttr("version", forma.ValueTypeText)

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
	parts = append(parts, sortedAttrs...)
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
			var expr string
			if a.meta.ValueType == forma.ValueTypeBool {
				// Normalize the main column to BOOLEAN so both sides of COALESCE
				// are the same type. hot_vals.<attr> is already BOOLEAN (from the
				// EAV pivot fix); m.<col> must be normalized by encoding.
				mainBoolExpr := mainColBoolExpr(colName, a.meta.ColumnBinding.Encoding)
				expr = fmt.Sprintf("COALESCE(hot_vals.%s, %s) AS %s",
					a.name, mainBoolExpr, a.name)
			} else {
				expr = fmt.Sprintf("COALESCE(hot_vals.%s, m.%s) AS %s",
					a.name, colName, a.name)
			}
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
		var pivotExpr string
		if a.meta.ValueType == forma.ValueTypeBool {
			// Wrap in <> 0 so the pivot column is BOOLEAN, not DOUBLE.
			// DuckDB WHERE uses CAST(? AS BOOLEAN); the pivot output must match.
			pivotExpr = fmt.Sprintf(
				"(MAX(CASE WHEN attr_id = %d THEN value_numeric END) <> 0)",
				a.attrID)
		} else {
			pivotExpr = fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN %s END)",
				a.attrID, eavValueColumn(a.meta.ValueType))
		}
		pivotParts = append(pivotParts, fmt.Sprintf("\t\t\t\t%s AS %s", pivotExpr, a.name))
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
		forma.ValueTypeSmallInt, forma.ValueTypeDate, forma.ValueTypeDateTime,
		forma.ValueTypeBool:
		return "value_numeric"
	default:
		return "value_text"
	}
}

// mainColBoolExpr returns a DuckDB boolean expression that normalises a
// column-bound bool main-table column to a BOOLEAN value.
// bool_text encoding stores "1"/"0" as text; bool_smallint stores 0/1 as SMALLINT.
// Both must produce a BOOLEAN so that COALESCE(hot_vals.<attr>, <expr>) is type-safe.
func mainColBoolExpr(colName string, enc forma.MainColumnEncoding) string {
	if enc == forma.MainColumnEncodingBoolText {
		return fmt.Sprintf("m.%s = '1'", colName)
	}
	// Default covers MainColumnEncodingBoolInt and any other numeric-like encoding.
	return fmt.Sprintf("m.%s <> 0", colName)
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
	allMainCols := model.EntityMainColumnDescriptors[len(model.SystemColumnDescriptors):]
	mappedCols := make(map[string]bool)
	for _, col := range sp.AttrToMainColumn {
		mappedCols[col] = true
	}
	for _, desc := range allMainCols {
		if mappedCols[desc.Name] {
			continue
		}
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s",
			duckDBColumnType(desc.Kind), desc.Name))
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

	sp.OuterSelect = strings.Join(parts, ",\n\t\t\t")
}

// BuildPGSelectNoEAV returns a PG source SELECT that uses only entity_main columns
// (no EAV pivot expressions), for use when all filter/sort attributes are column-bound.
func (sp *SchemaProjection) BuildPGSelectNoEAV() string {
	selectParts := []string{
		"cl.row_id::VARCHAR AS row_id",
		"m.ltbase_created_at AS created_at",
		"cl.changed_at AS ver_ts",
		"cl.deleted_at AS deleted_ts",
	}

	// Collect attribute names sorted for stability
	attrs := make([]string, 0, len(sp.AttrToMainColumn)+len(sp.EAVAttrs))
	for attr := range sp.AttrToMainColumn {
		attrs = append(attrs, attr)
	}
	sort.Strings(attrs)

	for _, attr := range attrs {
		col := sp.AttrToMainColumn[attr]
		selectParts = append(selectParts, fmt.Sprintf("m.%s AS %s", col, attr))
	}

	return strings.Join(selectParts, ",\n\t\t\t")
}

// BuildPGGroupByNoEAV returns the GROUP BY clause for PG source without EAV pivot.
func (sp *SchemaProjection) BuildPGGroupByNoEAV() string {
	parts := []string{
		"cl.row_id",
		"m.ltbase_created_at",
		"cl.changed_at",
		"cl.deleted_at",
	}

	attrs := make([]string, 0, len(sp.AttrToMainColumn))
	for attr := range sp.AttrToMainColumn {
		attrs = append(attrs, attr)
	}
	sort.Strings(attrs)

	for _, attr := range attrs {
		col := sp.AttrToMainColumn[attr]
		parts = append(parts, "m."+col)
	}

	return strings.Join(parts, ", ")
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

func duckDBColumnType(k model.ColumnKind) string {
	switch k {
	case model.ColumnKindText:
		return "VARCHAR"
	case model.ColumnKindSmallint:
		return "SMALLINT"
	case model.ColumnKindInteger:
		return "INTEGER"
	case model.ColumnKindBigint:
		return "BIGINT"
	case model.ColumnKindDouble:
		return "DOUBLE"
	case model.ColumnKindUUID:
		return "UUID"
	default:
		return "VARCHAR"
	}
}

// BuildBenchmarkS3Projection builds an S3 projection for benchmark parquet data.
// Column-bound attributes are read as flat columns; EAV-only attributes are
// extracted from the attributes_json column to model production cold-tier costs.
// Columns are emitted in alphabetical order to match PG source projection ordering.
func BuildBenchmarkS3Projection(schemaID int16) string {
	parts := []string{
		"row_id",
		"changed_at AS created_at",
		"changed_at AS ver_ts",
		"deleted_at AS deleted_ts",
	}

	// Benchmark parquet: column-bound attrs are flat, EAV-only attrs are in attributes_json
	switch schemaID {
	case 100: // customer
		parts = append(parts,
			"try_cast(json_extract_string(attributes_json, '$.creditRating') AS DOUBLE) as creditRating",
			"try_cast(json_extract_string(attributes_json, '$.email') AS VARCHAR) as email",
			"name",
			"region",
			"status",
			"taxId",
			"version",
		)
	case 101: // security
		parts = append(parts,
			"try_cast(json_extract_string(attributes_json, '$.companyName') AS VARCHAR) as companyName",
			"try_cast(json_extract_string(attributes_json, '$.dividend') AS DOUBLE) as dividend",
			"try_cast(json_extract_string(attributes_json, '$.marketCap') AS DOUBLE) as marketCap",
			"name",
			"sector",
			"symbol",
			"version",
		)
	case 102: // trade
		parts = append(parts,
			"try_cast(json_extract_string(attributes_json, '$.brokerId') AS VARCHAR) as brokerId",
			"try_cast(json_extract_string(attributes_json, '$.commission') AS DOUBLE) as commission",
			"customerId",
			"try_cast(json_extract_string(attributes_json, '$.exchange') AS VARCHAR) as exchange",
			"try_cast(json_extract_string(attributes_json, '$.isCash') AS BOOLEAN) as isCash",
			"try_cast(json_extract_string(attributes_json, '$.orderChannel') AS VARCHAR) as orderChannel",
			"price",
			"quantity",
			"region",
			"symbol",
			"tradeTime",
			"tradeType",
			"version",
		)
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
			desc := model.GetMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.Kind), col))
		}
		// attributes_json from EAV-only attributes
		parts = append(parts, fmt.Sprintf("%s::TEXT AS attributes_json",
			benchmarkEAVJSONArray(schemaID, 102, "",
				eavJSONAttr{id: 8, name: "exchange", type_: "text"},
				eavJSONAttr{id: 9, name: "commission", type_: "numeric"},
				eavJSONAttr{id: 10, name: "isCash", type_: "numeric"},
				eavJSONAttr{id: 11, name: "brokerId", type_: "text"},
				eavJSONAttr{id: 12, name: "orderChannel", type_: "text"},
			)))
	case 100: // customer
		parts = append(parts, "CAST(taxId AS VARCHAR) AS text_01")
		parts = append(parts, "CAST(region AS VARCHAR) AS text_02")
		parts = append(parts, "CAST(status AS SMALLINT) AS smallint_01")
		remaining := restMainColumnNames([]string{"text_01", "text_02", "smallint_01"})
		for _, col := range remaining {
			desc := model.GetMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.Kind), col))
		}
		parts = append(parts, fmt.Sprintf("%s::TEXT AS attributes_json",
			benchmarkEAVJSONArray(schemaID, 100, "",
				eavJSONAttr{id: 5, name: "email", type_: "text"},
				eavJSONAttr{id: 6, name: "creditRating", type_: "numeric"},
			)))
	case 101: // security
		parts = append(parts, "CAST(symbol AS VARCHAR) AS text_01")
		parts = append(parts, "CAST(sector AS SMALLINT) AS smallint_01")
		remaining := restMainColumnNames([]string{"text_01", "smallint_01"})
		for _, col := range remaining {
			desc := model.GetMainColumnDescriptor(col)
			if desc == nil {
				parts = append(parts, fmt.Sprintf("NULL::VARCHAR AS %s", col))
				continue
			}
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.Kind), col))
		}
		parts = append(parts, fmt.Sprintf("%s::TEXT AS attributes_json",
			benchmarkEAVJSONArray(schemaID, 101, "",
				eavJSONAttr{id: 3, name: "companyName", type_: "text"},
				eavJSONAttr{id: 4, name: "dividend", type_: "numeric"},
				eavJSONAttr{id: 5, name: "marketCap", type_: "numeric"},
			)))
	}

	return strings.Join(parts, ",\n\t\t\t")
}

// restMainColumnNames returns all entity_main EAV column names except those listed in exclude.
func restMainColumnNames(exclude []string) []string {
	excludeSet := make(map[string]bool)
	for _, col := range exclude {
		excludeSet[col] = true
	}
	var result []string
	for _, desc := range model.EntityMainColumnDescriptors[len(model.SystemColumnDescriptors):] {
		if !excludeSet[desc.Name] {
			result = append(result, desc.Name)
		}
	}
	return result
}

// benchmarkAttr describes a benchmark attribute used to build matching S3 and PG projections.
type benchmarkAttr struct {
	name    string
	colName string // entity_main column name, empty if EAV-only
	attrID  int
	eavJSON bool // true if extracted from attributes_json in S3 parquet
	s3Expr  string
}

// BuildBenchmarkProjections builds a SchemaProjection with matching S3 and PG sources
// for a benchmark schema. Both sources produce the same columns in the same order.
func BuildBenchmarkProjections(schemaID int16) *SchemaProjection {
	sp := &SchemaProjection{
		UnifiedColumnNames: []string{"row_id", "created_at", "ver_ts", "deleted_ts"},
		AttrToMainColumn:   make(map[string]string),
		attrIDs:            make(map[string]int),
	}

	// Define benchmark attributes in sorted order (matching PG projection order)
	var allAttrs []benchmarkAttr
	switch schemaID {
	case 100: // customer
		allAttrs = []benchmarkAttr{
			{name: "creditRating", eavJSON: false, attrID: 6, s3Expr: "creditRating"},
			{name: "email", eavJSON: false, attrID: 5, s3Expr: "email"},
			{name: "region", colName: "text_02", eavJSON: false, attrID: 3, s3Expr: "region"},
			{name: "status", colName: "smallint_01", eavJSON: false, attrID: 2, s3Expr: "status"},
			{name: "taxId", colName: "text_01", eavJSON: false, attrID: 1, s3Expr: "taxId"},
		}
	case 101: // security
		allAttrs = []benchmarkAttr{
			{name: "companyName", eavJSON: false, attrID: 3, s3Expr: "companyName"},
			{name: "dividend", eavJSON: false, attrID: 4, s3Expr: "dividend"},
			{name: "marketCap", eavJSON: false, attrID: 5, s3Expr: "marketCap"},
			{name: "sector", colName: "smallint_01", eavJSON: false, attrID: 2, s3Expr: "sector"},
			{name: "symbol", colName: "text_01", eavJSON: false, attrID: 1, s3Expr: "symbol"},
		}
	case 102: // trade
		allAttrs = []benchmarkAttr{
			{name: "brokerId", eavJSON: true, attrID: 11, s3Expr: "brokerId"},
			{name: "commission", eavJSON: true, attrID: 9, s3Expr: "commission"},
			{name: "customerId", colName: "uuid_01", eavJSON: false, attrID: 6, s3Expr: "customerId"},
			{name: "exchange", eavJSON: true, attrID: 8, s3Expr: "exchange"},
			{name: "isCash", eavJSON: true, attrID: 10, s3Expr: "isCash"},
			{name: "orderChannel", eavJSON: true, attrID: 12, s3Expr: "orderChannel"},
			{name: "price", colName: "double_01", eavJSON: false, attrID: 4, s3Expr: "price"},
			{name: "quantity", colName: "bigint_01", eavJSON: false, attrID: 3, s3Expr: "quantity"},
			{name: "region", colName: "text_02", eavJSON: false, attrID: 7, s3Expr: "region"},
			{name: "symbol", colName: "text_01", eavJSON: false, attrID: 1, s3Expr: "symbol"},
			{name: "tradeTime", colName: "bigint_02", eavJSON: false, attrID: 5, s3Expr: "epoch_ms(try_cast(tradeTime AS TIMESTAMP)) as tradeTime"},
			{name: "tradeType", colName: "smallint_01", eavJSON: false, attrID: 2, s3Expr: "tradeType"},
		}
	}

	// Benchmark parquet files expose changed_at/deleted_at directly rather than the
	// production ltbase_* columns used by exported entity_main projections.
	s3Parts := []string{"row_id", "changed_at AS created_at", "changed_at AS ver_ts", "deleted_at AS deleted_ts"}
	for _, a := range allAttrs {
		s3Parts = append(s3Parts, a.s3Expr)
	}
	sp.S3SourceSelect = strings.Join(s3Parts, ", ")

	// Build PG source projection and GROUP BY
	pgSelectParts := []string{
		"cl.row_id::VARCHAR AS row_id",
		"m.ltbase_created_at AS created_at",
		"cl.changed_at AS ver_ts",
		"cl.deleted_at AS deleted_ts",
	}
	pgGroupParts := []string{"cl.row_id", "m.ltbase_created_at", "cl.changed_at", "cl.deleted_at"}

	// Build EAV pivot
	var eavPivotParts []string
	var eavAttrIDParts []string

	for _, a := range allAttrs {
		if a.colName != "" {
			pgSelectParts = append(pgSelectParts,
				fmt.Sprintf("COALESCE(ANY_VALUE(hot_vals.%s), CAST(m.%s AS VARCHAR)) AS %s", a.name, a.colName, a.name))
			pgGroupParts = append(pgGroupParts, "m."+a.colName)
		} else {
			pgSelectParts = append(pgSelectParts, fmt.Sprintf("ANY_VALUE(hot_vals.%s) AS %s", a.name, a.name))
		}
		sp.attrIDs[a.name] = a.attrID
		sp.UnifiedColumnNames = append(sp.UnifiedColumnNames, a.name)

		if a.attrID > 0 {
			eavPivotParts = append(eavPivotParts,
				fmt.Sprintf("\t\t\t\tMAX(CASE WHEN attr_id = %d THEN value_text END) AS %s", a.attrID, a.name))
			eavAttrIDParts = append(eavAttrIDParts, fmt.Sprintf("%d", a.attrID))
		}
	}

	sp.PGSourceSelect = strings.Join(pgSelectParts, ",\n\t\t\t")
	sp.PGGroupBy = strings.Join(pgGroupParts, ", ")
	if len(eavPivotParts) > 0 {
		sp.EAVPivotSelect = strings.Join(eavPivotParts, ",\n")
	}
	if len(eavAttrIDParts) > 0 {
		sp.EAVPivotAttrs = strings.Join(eavAttrIDParts, ", ")
	}

	return sp
}

// eavJSONAttr describes an EAV attribute for JSON array generation.
type eavJSONAttr struct {
	id    int
	name  string
	type_ string // "text" or "numeric"
}

// benchmarkEAVJSONArray generates a DuckDB json_array of json_objects for EAV attributes.
// Each object has: schema_id, row_id, attr_id, array_indices, value_text, value_numeric.
func benchmarkEAVJSONArray(schemaID, targetSchemaID int16, extra string, attrs ...eavJSONAttr) string {
	var parts []string
	for _, a := range attrs {
		valColumn := "value_text"
		nullColumn := "value_numeric"
		valueExpr := a.name
		if a.type_ == "numeric" {
			valColumn = "value_numeric"
			nullColumn = "value_text"
			if a.name == "isCash" {
				valueExpr = "CASE WHEN isCash THEN 1 ELSE 0 END"
			}
		}
		parts = append(parts, fmt.Sprintf(
			`json_object('schema_id', %d, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', %d, 'array_indices', '', '%s', CAST(%s AS VARCHAR), '%s', NULL)`,
			targetSchemaID, a.id, valColumn, valueExpr, nullColumn))
	}
	if len(parts) == 0 {
		return "'[]'"
	}
	return "json_array(" + strings.Join(parts, ", ") + ")"
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
