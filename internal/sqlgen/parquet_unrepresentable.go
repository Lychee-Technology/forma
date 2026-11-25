package sqlgen

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/lychee-technology/forma"
)

// ParquetUnrepresentableReason says why the parquet tiers cannot hold a
// schema's rows, or returns "" when they can (#704). They cannot hold an
// attribute inside an array of objects: the CDC export and the DuckDB
// hot-tier pivot both reduce a row's attribute to one value, and such an
// attribute has one value per element, so a row with two elements would read
// back as one object holding the larger of each member. A list inside the
// array fares no better, since its elements' indices ("i,j") do not order as
// one list. The first such attribute in name order is named.
//
// It reads ArrayAncestors, which only the schema registry derives (the
// metadata loader's cache leaves them nil), so callers pass the registry's
// cache. A schema whose registry did not derive them (#705) reports "".
//
// An ancestor declared as an object or an array of objects
// (UnionArrayAncestors) does not count: a row writing it in its object form
// holds one value of the attribute, as the schema's rows did before #696
// derived the union's array form. A row writing the array form is what the
// tiers cannot hold, so the CDC export refuses that row instead
// (UnionArrayFormCondition, #696 review R4-F1).
func ParquetUnrepresentableReason(cache forma.SchemaAttributeCache) string {
	names := make([]string, 0, len(cache))
	for name, meta := range cache {
		if firstDeclaredArray(meta) != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	attr := names[0]
	return fmt.Sprintf("attribute %q lies inside the array of objects %q, and the parquet tiers hold one value of it per row, not one per element (#704)",
		attr, firstDeclaredArray(cache[attr]))
}

// firstDeclaredArray answers the outermost of meta's array ancestors that
// is not an object-or-array union, "" when there is none.
func firstDeclaredArray(meta forma.AttributeMetadata) string {
	for _, ancestor := range meta.ArrayAncestors {
		if !slices.Contains(meta.UnionArrayAncestors, ancestor) {
			return ancestor
		}
	}
	return ""
}

// UnionArrayFormCondition answers a SQL condition over one eav_data record
// (attr_id, array_indices, value_text, value_numeric) that holds when the
// record is meta's written inside the array form of an object-or-array
// union, "" when meta has no union ancestor or is column-bound. A
// representable schema's attribute has only union ancestors, so a record
// of the object form carries no index but a list's own: a list element
// carries one ("j"), and the empty-list marker, a record with no value,
// none. Any further index is the union's array form, which the parquet
// tiers cannot hold one value of per row (#704, #696 review R4-F1).
func UnionArrayFormCondition(meta forma.AttributeMetadata) string {
	if len(meta.UnionArrayAncestors) == 0 || meta.ColumnBinding != nil {
		return ""
	}
	if meta.ValueType == forma.ValueTypeList {
		return fmt.Sprintf("attr_id = %d AND array_indices <> '' AND (array_indices LIKE '%%,%%' OR (value_text IS NULL AND value_numeric IS NULL))",
			meta.AttributeID)
	}
	return fmt.Sprintf("attr_id = %d AND array_indices <> ''", meta.AttributeID)
}

// ValidateParquetRepresentable refuses a schema ParquetUnrepresentableReason
// names. The CDC export calls it as defense in depth: the CDC run skips such
// a schema before the export is built. Plain operator error, never
// forma.ErrInvalidInput.
func ValidateParquetRepresentable(cache forma.SchemaAttributeCache) error {
	if reason := ParquetUnrepresentableReason(cache); reason != "" {
		return errors.New(reason)
	}
	return nil
}
