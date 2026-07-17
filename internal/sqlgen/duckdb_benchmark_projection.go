package sqlgen

// Benchmark-schema (IDs 100-102) projections: hardcoded parquet shapes for
// the benchmark harness, split from duckdb_schema_projection.go along the
// seam recorded in #220. The production schema-driven projections live in
// duckdb_schema_projection.go.

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"
)

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

	// Map known benchmark attributes to entity_main column cast expressions.
	var boundCols map[string]string
	var eavJSON string
	switch schemaID {
	case 102: // trade
		boundCols = map[string]string{
			"text_01":     "CAST(symbol AS VARCHAR)",
			"text_02":     "CAST(region AS VARCHAR)",
			"smallint_01": "CAST(tradeType AS SMALLINT)",
			"bigint_01":   "CAST(quantity AS BIGINT)",
			"bigint_02":   "CAST(tradeTime AS BIGINT)",
			"double_01":   "CAST(price AS DOUBLE)",
			"uuid_01":     "CAST(customerId AS UUID)",
		}
		eavJSON = benchmarkEAVJSONArray(schemaID, 102, "",
			eavJSONAttr{id: 8, name: "exchange", type_: "text"},
			eavJSONAttr{id: 9, name: "commission", type_: "numeric"},
			eavJSONAttr{id: 10, name: "isCash", type_: "numeric"},
			eavJSONAttr{id: 11, name: "brokerId", type_: "text"},
			eavJSONAttr{id: 12, name: "orderChannel", type_: "text"},
		)
	case 100: // customer
		boundCols = map[string]string{
			"text_01":     "CAST(taxId AS VARCHAR)",
			"text_02":     "CAST(region AS VARCHAR)",
			"smallint_01": "CAST(status AS SMALLINT)",
		}
		eavJSON = benchmarkEAVJSONArray(schemaID, 100, "",
			eavJSONAttr{id: 5, name: "email", type_: "text"},
			eavJSONAttr{id: 6, name: "creditRating", type_: "numeric"},
		)
	case 101: // security
		boundCols = map[string]string{
			"text_01":     "CAST(symbol AS VARCHAR)",
			"smallint_01": "CAST(sector AS SMALLINT)",
		}
		eavJSON = benchmarkEAVJSONArray(schemaID, 101, "",
			eavJSONAttr{id: 3, name: "companyName", type_: "text"},
			eavJSONAttr{id: 4, name: "dividend", type_: "numeric"},
			eavJSONAttr{id: 5, name: "marketCap", type_: "numeric"},
		)
	}

	// Emit EAV columns in descriptor order: streamDuckDBRows scans rows
	// positionally against model.EntityMainColumnDescriptors (#147).
	for _, desc := range model.EntityMainColumnDescriptors[len(model.SystemColumnDescriptors):] {
		if expr, ok := boundCols[desc.Name]; ok {
			parts = append(parts, fmt.Sprintf("%s AS %s", expr, desc.Name))
			continue
		}
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBColumnType(desc.Kind), desc.Name))
	}
	if eavJSON == "" {
		eavJSON = "'[]'"
	}
	parts = append(parts, fmt.Sprintf("%s::TEXT AS attributes_json", eavJSON))

	return strings.Join(parts, ",\n\t\t\t")
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
			{name: "tradeTime", colName: "bigint_02", eavJSON: false, attrID: 5, s3Expr: "tradeTime"},
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
