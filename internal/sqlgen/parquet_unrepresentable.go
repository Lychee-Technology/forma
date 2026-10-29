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
// cache. A schema whose registry did not derive them (#705) reports "".
func ParquetUnrepresentableReason(cache forma.SchemaAttributeCache) string {
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
