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

	// OuterSelect is the final outer SELECT that maps unified columns back to
	// entity_main column descriptors.
	OuterSelect string

	// UnifiedColumnNames lists the canonical column names s3_source produces
	// (in order).
	UnifiedColumnNames []string

	// UnifiedColumnTypes maps column name to DuckDB type for CAST expressions.
	UnifiedColumnTypes map[string]forma.ValueType

	// AttrToMainColumn maps attribute name to entity_main column name.
	AttrToMainColumn map[string]string

	// EAVAttrs lists attribute names that are EAV-only (no column binding).
	EAVAttrs []string

	// HasEAVAttrs is true if the schema has any EAV-only attributes.
	HasEAVAttrs bool

	// attrIDs maps attribute name to its attribute ID.
	attrIDs map[string]int

	// itemsTypes maps a list attribute's name to its effective element type.
	itemsTypes map[string]forma.ValueType
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
		itemsTypes:       make(map[string]forma.ValueType),
	}

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
		if meta.ValueType == forma.ValueTypeList {
			sp.itemsTypes[name] = meta.EffectiveItemsType()
		}
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

	// Folded names must not collide with each other or with the system
	// columns this projection emits itself; registration already rejects
	// such schemas, this re-check is defense in depth (plain read-path
	// error).
	if err := ValidateParquetAttrColumns(cache); err != nil {
		return nil, fmt.Errorf("schema %d: %w", schemaID, err)
	}

	// Build S3 source projection
	sp.buildS3Projection(sortedAttrNames)

	// Build outer SELECT
	sp.buildOuterSelect(schemaID, sortedAttrNames)

	return sp, nil
}

// buildS3Projection renders the parquet leg's system columns plus one column
// per attribute. created_at and ver_ts are DIFFERENT quantities and come from
// different parquet columns (#460): ltbase_created_at is the creation time
// every generation carries (cdc.exportModeSpec.baseSelectColumns), changed_at
// is the LWW version stamp. Aliasing changed_at into the created_at slot put a
// different quantity here than the hot leg's m.ltbase_created_at puts in the
// same UNION ALL column, so created_at was wrong for parquet-winning rows and
// the default sort key changed value at the flush boundary. No COALESCE
// fallback: only hard-delete tombstones carry a NULL ltbase_created_at (the
// delta export LEFT JOINs entity_main, #173) and they always stamp
// deleted_at > 0, so `visible` drops them before any ORDER BY.
func (sp *SchemaProjection) buildS3Projection(sortedAttrs []string) {
	parts := make([]string, 0, 4+len(sortedAttrs))
	// System columns from parquet
	parts = append(parts,
		"row_id",
		"ltbase_created_at AS created_at",
		"changed_at AS ver_ts",
		"deleted_at AS deleted_ts",
	)
	for _, attr := range sortedAttrs {
		parts = append(parts, ParquetAttrColumn(attr))
	}
	sp.S3SourceSelect = strings.Join(parts, ", ")
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

	eavOnlySelects := make(map[string]bool)
	for _, attr := range sp.EAVAttrs {
		eavOnlySelects[attr] = true
	}

	// Emit EAV columns in descriptor order: streamDuckDBRows scans rows
	// positionally against model.EntityMainColumnDescriptors, so any other
	// order lands attribute values in the wrong record fields (#147).
	allMainCols := model.EntityMainColumnDescriptors[len(model.SystemColumnDescriptors):]
	for _, desc := range allMainCols {
		if attr, ok := mainColToAttr[desc.Name]; ok {
			parts = append(parts, fmt.Sprintf("%s AS %s",
				duckDBMainColCast(ParquetAttrColumn(attr), sp.UnifiedColumnTypes[attr], desc.Kind), desc.Name))
			continue
		}
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s",
			duckDBColumnType(desc.Kind), desc.Name))
	}

	// Build attributes_json for EAV-only attributes; the shape contract and
	// the per-element list expansion live in duckdb_schema_projection_json.go.
	parts = append(parts, sp.buildAttributesJSONExpr(schemaID, sortedAttrs, eavOnlySelects))

	sp.OuterSelect = strings.Join(parts, ",\n\t\t\t")
}

func (sp *SchemaProjection) attrIDForName(name string) int {
	if id, ok := sp.attrIDs[name]; ok {
		return id
	}
	return 0
}

// duckDBMainColCast renders the outer-select cast for an attribute that lands
// in an entity_main column. The alias is the physical column and the reader
// scans it by the column's kind (federated.duckDBScanBuffers), so the cast
// must produce that kind, not the logical value type. The two diverge for
// bool and for an rfc3339 date/datetime, and the stored image is re-derived
// from the unified verdict in both cases:
//   - bool: every leg derives a BOOLEAN (mainColBoolExpr, the parquet
//     column), and CAST(attr AS BOOLEAN) then failed the SMALLINT scan
//     ("converting driver.Value type bool") and put "true" into the text
//     slot a bool_text reader expects "1"/"0" in. 1/0 for bool_smallint,
//     '1'/'0' for bool_text, NULL kept.
//   - date/datetime in a text column (rfc3339): every leg carries epoch-ms
//     BIGINT, and CAST(attr AS BIGINT) then landed "1704164645000" in the
//     text slot transform.readWithEncoding parses as RFC3339 (#555). The
//     write path's image — RFC 3339, UTC, three fractional digits (#842) —
//     is re-derived (rfc3339ImageFromEpochMs), with an offset only for an
//     instant past the four-digit UTC years.
func duckDBMainColCast(attr string, vt forma.ValueType, kind model.ColumnKind) string {
	switch vt {
	case forma.ValueTypeBool:
		switch kind {
		case model.ColumnKindSmallint:
			return fmt.Sprintf("CAST(%s AS SMALLINT)", attr)
		case model.ColumnKindText:
			return fmt.Sprintf("CAST(CAST(%s AS TINYINT) AS VARCHAR)", attr)
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		if kind == model.ColumnKindText {
			return rfc3339ImageFromEpochMs(attr)
		}
	}
	return duckDBAttrCast(attr, vt)
}

func duckDBAttrCast(attr string, vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeList:
		// LIST columns are consumed by attributes_json reconstruction;
		// CAST(... AS VARCHAR) would stringify them, so pass through (#204).
		return attr
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

// BuildSchemaDrivenTemplateParams computes template parameters from a SchemaProjection.
func BuildSchemaDrivenTemplateParams(sp *SchemaProjection, schemaID int16) map[string]any {
	return map[string]any{
		"S3SourceSelect": sp.S3SourceSelect,
		"OuterSelect":    sp.OuterSelect,
		"HasEAVAttrs":    sp.HasEAVAttrs,
	}
}
