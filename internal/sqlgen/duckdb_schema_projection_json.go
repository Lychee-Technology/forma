package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// This file renders the attributes_json expression of the outer SELECT.
//
// The shape must match model.ParseAttributesJSON (the same contract the
// Postgres template's JSON_AGG(JSON_BUILD_OBJECT(...)) emits): a JSON array
// of objects with schema_id/row_id/attr_id/array_indices and the value in
// the storage column for the attribute's type (value_numeric for the numeric
// family including bool as 1/0 and dates as epoch millis; value_text
// otherwise). NULL (absent) attributes are filtered out, mirroring the PG
// INNER JOIN which only aggregates rows that exist in eav_data (#173).
//
// Scalar attributes emit one object with array_indices ''. List attributes
// are stored as a LIST column (one element per original eav_data row, #204);
// they expand back into one object per element with positional
// array_indices, so the downstream merge key (AttrID, ArrayIndices) keeps
// every element distinct.

// buildAttributesJSONExpr renders the full "... AS attributes_json" part of
// the outer SELECT. Schemas without list attributes keep the historical
// single-level list_filter([...]) expression byte-identical; the flatten
// form is only introduced when a list attribute needs per-element expansion.
func (sp *SchemaProjection) buildAttributesJSONExpr(schemaID int16, sortedAttrs []string, eavOnly map[string]bool) string {
	hasList := false
	eavAttrs := make([]string, 0, len(sortedAttrs))
	for _, attr := range sortedAttrs {
		if !eavOnly[attr] {
			continue
		}
		eavAttrs = append(eavAttrs, attr)
		if sp.UnifiedColumnTypes[attr] == forma.ValueTypeList {
			hasList = true
		}
	}

	if len(eavAttrs) == 0 {
		return "'[]'::TEXT AS attributes_json"
	}

	if !hasList {
		parts := make([]string, 0, len(eavAttrs))
		for _, attr := range eavAttrs {
			parts = append(parts, sp.scalarEAVJSONObject(schemaID, attr))
		}
		return "to_json(list_filter([" + strings.Join(parts, ", ") + "], x -> x IS NOT NULL))::TEXT AS attributes_json"
	}

	parts := make([]string, 0, len(eavAttrs))
	for _, attr := range eavAttrs {
		if sp.UnifiedColumnTypes[attr] == forma.ValueTypeList {
			parts = append(parts, sp.listEAVJSONPart(schemaID, attr))
			continue
		}
		parts = append(parts, "["+sp.scalarEAVJSONObject(schemaID, attr)+"]")
	}
	return "to_json(list_filter(flatten([" + strings.Join(parts, ", ") + "]), x -> x IS NOT NULL))::TEXT AS attributes_json"
}

// scalarEAVJSONObject renders the single CASE ... END object for one scalar
// EAV attribute (array_indices is '' — scalars have no element position).
func (sp *SchemaProjection) scalarEAVJSONObject(schemaID int16, attr string) string {
	unified := ParquetAttrColumn(attr)
	valueText, valueNumeric := "NULL", "NULL"
	vt := sp.UnifiedColumnTypes[attr]
	if eavValueColumn(vt) == "value_numeric" {
		if vt == forma.ValueTypeBool {
			valueNumeric = fmt.Sprintf("CAST(CAST(%s AS INTEGER) AS DOUBLE)", unified)
		} else {
			valueNumeric = fmt.Sprintf("CAST(%s AS DOUBLE)", unified)
		}
	} else {
		valueText = fmt.Sprintf("CAST(%s AS VARCHAR)", unified)
	}
	return fmt.Sprintf(
		"CASE WHEN %s IS NOT NULL THEN {'schema_id': %d, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': %d, 'array_indices': '', 'value_text': %s, 'value_numeric': %s} END",
		unified, schemaID, sp.attrIDForName(attr), valueText, valueNumeric)
}

// listEAVJSONPart expands a LIST-typed attribute column into one object per
// element. The 1-based lambda index i becomes the 0-based array_indices, so
// the parquet LIST position round-trips the original element index. An
// explicit empty list ([] column, non-NULL) emits the marker object —
// array_indices '' with both value columns NULL — which the transform layer
// materializes back into an empty array; NULL (absent) emits nothing (#204).
func (sp *SchemaProjection) listEAVJSONPart(schemaID int16, attr string) string {
	unified := ParquetAttrColumn(attr)
	itemsVT := sp.itemsTypes[attr]
	valueText, valueNumeric := "NULL", "NULL"
	if eavValueColumn(itemsVT) == "value_numeric" {
		if itemsVT == forma.ValueTypeBool {
			valueNumeric = "CAST(CAST(x AS INTEGER) AS DOUBLE)"
		} else {
			valueNumeric = "CAST(x AS DOUBLE)"
		}
	} else {
		valueText = "CAST(x AS VARCHAR)"
	}
	rowExpr := "CAST(row_id AS VARCHAR)"
	return fmt.Sprintf(
		"CASE WHEN %s IS NOT NULL AND len(%s) = 0 THEN [{'schema_id': %d, 'row_id': %s, 'attr_id': %d, 'array_indices': '', 'value_text': NULL, 'value_numeric': NULL}] WHEN %s IS NOT NULL THEN list_transform(%s, (x, i) -> {'schema_id': %d, 'row_id': %s, 'attr_id': %d, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': %s, 'value_numeric': %s}) ELSE [] END",
		unified, unified, schemaID, rowExpr, sp.attrIDForName(attr),
		unified, unified, schemaID, rowExpr, sp.attrIDForName(attr), valueText, valueNumeric)
}
