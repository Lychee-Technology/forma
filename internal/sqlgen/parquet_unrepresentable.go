package sqlgen

import (
	"errors"
	"fmt"
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
// cache. A schema with an attribute whose ancestry was not derived cannot be
// held either (#937): nil ArrayAncestors says nothing about whether the
// attribute lies inside an array of objects, so the schema fails closed. That
// holds back a schema whose unresolved attribute is in fact flat too, such as
// a cross-file $ref to a flat object, until its ancestry is derived (#705).
func ParquetUnrepresentableReason(cache forma.SchemaAttributeCache) string {
	if reason := objectArrayMemberReason(cache); reason != "" {
		return reason
	}
	return underivedAncestryReason(cache)
}

// objectArrayMemberReason names the first attribute in name order that the
// registry placed inside an array of objects (#704).
func objectArrayMemberReason(cache forma.SchemaAttributeCache) string {
	names := make([]string, 0, len(cache))
	for name, meta := range cache {
		if len(meta.ArrayAncestors) > 0 {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	attr := names[0]
	return fmt.Sprintf("attribute %q lies inside the array of objects %q, and the parquet tiers hold one value of it per row, not one per element (#704)",
		attr, cache[attr].ArrayAncestors[0])
}

// underivedAncestryReason names, for a schema holding an attribute without
// derived ancestry, the first attribute in name order the registry marked
// ArrayAncestryUnresolved, or else the first without ancestry: a registry
// that derives none marks none (#937).
func underivedAncestryReason(cache forma.SchemaAttributeCache) string {
	unresolved, underived := "", ""
	for name, meta := range cache {
		if meta.ArrayAncestors != nil {
			continue
		}
		if underived == "" || name < underived {
			underived = name
		}
		if meta.ArrayAncestryUnresolved && (unresolved == "" || name < unresolved) {
			unresolved = name
		}
	}
	switch {
	case unresolved != "":
		return fmt.Sprintf("the schema registry could not place attribute %q: the schema document does not resolve its ancestors, "+
			"so the schema's array ancestry was not derived and the parquet tiers cannot tell whether an attribute lies inside an array of objects (#937)",
			unresolved)
	case underived != "":
		return fmt.Sprintf("the schema registry did not derive the array ancestry of attribute %q, "+
			"so the parquet tiers cannot tell whether it lies inside an array of objects (#937)", underived)
	}
	return ""
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
