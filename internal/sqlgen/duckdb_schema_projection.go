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
		parts = append(parts, ParquetAttrColumn(attr))
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

	// hot_vals references are wrapped in ANY_VALUE(): pg_source groups by
	// (row_id, timestamps, bound columns) and the hot_vals pivot joins 1:1
	// per row, but DuckDB does not infer that functional dependency, so a
	// bare hot_vals.<attr> is rejected as ungrouped. This mirrors the
	// benchmark projection, which already aggregates the same way (#173).
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].name < attrs[j].name })
	for _, a := range attrs {
		unified := ParquetAttrColumn(a.name)
		if a.isColumn {
			colName := string(a.meta.ColumnBinding.ColumnName)
			var expr string
			if a.meta.ValueType == forma.ValueTypeBool {
				// Normalize the main column to BOOLEAN so both sides of COALESCE
				// are the same type. hot_vals.<attr> is already BOOLEAN (from the
				// EAV pivot fix); m.<col> must be normalized by encoding.
				mainBoolExpr := mainColBoolExpr(colName, a.meta.ColumnBinding.Encoding)
				expr = fmt.Sprintf("COALESCE(ANY_VALUE(hot_vals.%s), %s) AS %s",
					unified, mainBoolExpr, unified)
			} else if a.meta.ValueType == forma.ValueTypeUUID {
				// hot_vals pivots uuid attributes out of value_text (VARCHAR);
				// the UUID main column must be cast explicitly because DuckDB
				// refuses to mix VARCHAR and UUID inside COALESCE.
				expr = fmt.Sprintf("COALESCE(ANY_VALUE(hot_vals.%s), CAST(m.%s AS VARCHAR)) AS %s",
					unified, colName, unified)
			} else {
				expr = fmt.Sprintf("COALESCE(ANY_VALUE(hot_vals.%s), m.%s) AS %s",
					unified, colName, unified)
			}
			selectParts = append(selectParts, expr)
			groupParts = append(groupParts, "m."+colName)
		} else {
			expr := fmt.Sprintf("ANY_VALUE(hot_vals.%s) AS %s", unified, unified)
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
		pivotParts = append(pivotParts, fmt.Sprintf("\t\t\t\t%s AS %s",
			buildEAVPivotExpr(a), ParquetAttrColumn(a.name)))
	}

	if len(pivotParts) > 0 {
		sp.EAVPivotSelect = strings.Join(pivotParts, ",\n")
	}
	if len(attrIDParts) > 0 {
		sp.EAVPivotAttrs = strings.Join(attrIDParts, ", ")
	}
}

// buildEAVPivotExpr renders the hot-tier EAV pivot expression for one attribute.
// The output type must mirror the CDC export side (cdc.castEAVValue) so the
// pg_source leg type-unifies with the parquet legs through COALESCE and
// UNION ALL instead of widening to DOUBLE — which silently rounded bound
// bigint values above 2^53 and crashed CAST-back at a stored MaxInt64 (#205).
// TRY_CAST (not CAST) matches export semantics: an out-of-range NUMERIC in
// eav_data becomes NULL on every tier alike instead of a read-path crash.
// EAV-only integer/smallint cast by storage width (DOUBLE — the write funnel
// narrows everything through float64), not declared width, so a stored value
// the declared type cannot hold answers the same on hot Postgres and every
// DuckDB tier (#384); the write funnel now rejects new ones
// (transform.checkDeclaredIntegerFit).
func buildEAVPivotExpr(a attrProjectionInfo) string {
	if a.meta.ValueType == forma.ValueTypeList {
		// One eav_data row per element: aggregate into a LIST in element-index
		// order instead of MAX-collapsing, mirroring the CDC export side
		// (cdc.castEAVValue with the items-typed meta) so the hot leg
		// type-unifies with the parquet LIST column. The empty-list marker row
		// (array_indices '') is excluded from the element aggregate but
		// detected by the presence count: explicit [] stays distinguishable
		// from an absent attribute (NULL) on every tier (#204).
		return fmt.Sprintf(
			"CASE WHEN count(*) FILTER (WHERE attr_id = %d) > 0 THEN coalesce(list(%s ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = %d AND array_indices <> ''), []) END",
			a.attrID, eavElementCastExpr(a.meta.EffectiveItemsType()), a.attrID)
	}
	if a.meta.ValueType == forma.ValueTypeBool {
		// Wrap in <> 0 so the pivot column is BOOLEAN, not DOUBLE (#182).
		return fmt.Sprintf("(MAX(CASE WHEN attr_id = %d THEN value_numeric END) <> 0)", a.attrID)
	}
	base := fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END)",
		a.attrID, eavValueColumn(a.meta.ValueType))
	switch a.meta.ValueType {
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return fmt.Sprintf("TRY_CAST(%s AS BIGINT)", base)
	case forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		if a.isColumn {
			// Bound storage is physically int4/int2: declared width IS the
			// storage width, and the COALESCE partner m.<col> is just as
			// narrow, so nothing wider can ever round-trip.
			if a.meta.ValueType == forma.ValueTypeInteger {
				return fmt.Sprintf("TRY_CAST(%s AS INTEGER)", base)
			}
			return fmt.Sprintf("TRY_CAST(%s AS SMALLINT)", base)
		}
		// EAV-only storage width is DOUBLE, like the numeric class: the write
		// funnel narrows everything through float64 (numutil.Float64), so
		// whatever value_numeric holds — including out-of-declared-range and
		// non-integral history — is a float64 image, and DOUBLE reproduces it
		// exactly on every tier. A narrower integer cast either NULLed the
		// value (BIGINT-overflow) or rounded it (1.5 → 2) only on the DuckDB
		// legs while hot Postgres compared the raw NUMERIC (#384).
		return fmt.Sprintf("TRY_CAST(%s AS DOUBLE)", base)
	default:
		// numeric 保持 DOUBLE;text/uuid 走 value_text,无 cast
		return base
	}
}

// eavElementCastExpr renders the raw-column cast for one list element of the
// given items type. Must mirror cdc.castEAVValue applied to the same type so
// hot LIST elements type-unify with the parquet LIST column (#204, cf. #205).
func eavElementCastExpr(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeBool:
		return "(value_numeric <> 0)"
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "TRY_CAST(value_numeric AS BIGINT)"
	case forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		// List storage is one float64-funneled NUMERIC eav row per element
		// and lists are always EAV-only: storage width DOUBLE, as in the
		// scalar EAV-only arm of buildEAVPivotExpr (#384).
		return "TRY_CAST(value_numeric AS DOUBLE)"
	case forma.ValueTypeNumeric:
		return "TRY_CAST(value_numeric AS DOUBLE)"
	default: // text / uuid
		return "value_text"
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
				duckDBAttrCast(ParquetAttrColumn(attr), sp.UnifiedColumnTypes[attr]), desc.Name))
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
		selectParts = append(selectParts, fmt.Sprintf("m.%s AS %s", col, ParquetAttrColumn(attr)))
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
		"PGSourceSelect": sp.PGSourceSelect,
		"PGGroupBy":      sp.PGGroupBy,
		"EAVPivotSelect": sp.EAVPivotSelect,
		"EAVPivotAttrs":  sp.EAVPivotAttrs,
		"OuterSelect":    sp.OuterSelect,
		"HasEAVAttrs":    sp.HasEAVAttrs,
		"HasEAVPivot":    len(sp.EAVPivotAttrs) > 0,
	}
}
