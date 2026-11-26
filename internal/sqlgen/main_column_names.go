package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// entityMainColumnNames is the physical entity_main column set, keyed the way
// the registration guard looks names up (DuckDBFoldIdentifier), derived from
// model.EntityMainColumnDescriptors rather than restated: that list is the one
// statement of the column set (#585), so a column added there is reserved here
// by construction.
//
// The logical attribute namespace and the physical storage namespace must not
// overlap (#470). The PostgreSQL renderers resolve a bare name to the physical
// column before they consult the schema cache (hybridMainColumn and
// hasMainTableCondition on the Query and CrossSchemaSearch paths), so an
// attribute named after a column is stored and read back correctly but
// filtered on a different value. The retired attach template had the same
// fault in its outer ORDER BY, where the outer select's per-column alias
// (NULL::VARCHAR AS text_01 when unbound) won over the visible CTE column. The
// parquet-only statement that replaced it (#858) qualifies every named sort
// key with visible. (parquetOnlyOrderTerms), so it does not rebind; the rule
// keeps the two namespaces disjoint instead of relying on every renderer to
// consult the cache first.
var entityMainColumnNames = buildEntityMainColumnNames(model.EntityMainColumnDescriptors)

func buildEntityMainColumnNames(descs []model.ColumnDescriptor) map[string]struct{} {
	cols := make(map[string]struct{}, len(descs))
	for _, d := range descs {
		cols[DuckDBFoldIdentifier(d.Name)] = struct{}{}
	}
	return cols
}

// mainColumnNameError explains why an attribute named after an entity_main
// column is refused. It names the attribute as the caller spelled it and the
// physical column it collides with; when the two differ (a case variant, or a
// name the fold transforms, such as "text.01"), it also names the folded form
// the renderers emit, so the operator can see how the name reaches the column.
func mainColumnNameError(name, col, key string) error {
	const why = "conditions and sorts resolve that name to the physical column, not the attribute, so they would silently filter and order on a different value; rename the attribute"
	if name == key {
		return fmt.Errorf("attribute %q is named after entity_main column %q: %s", name, key, why)
	}
	return fmt.Errorf("attribute %q folds to %q, which resolves onto entity_main column %q: %s", name, col, key, why)
}

// ValidateEntityMainColumnNames refuses an active attribute whose folded name
// is an entity_main column. It is the #470 half of ValidateParquetAttrColumns
// on its own, for caches that never went through admission: the attribute
// caches a caller-supplied forma.SchemaRegistry serves
// (schemameta.GuardEntityMainColumnNames). Those are read on every request,
// so it is a single pass with no sort and no map of its own; when several
// names collide, the error names the smallest, so it stays deterministic. Retired entries are exempt for the same reasons as there.
// Plain operator error, never forma.ErrInvalidInput.
func ValidateEntityMainColumnNames(cache forma.SchemaAttributeCache) error {
	first := ""
	found := false
	for name, meta := range cache {
		if meta.Retired {
			continue
		}
		if _, ok := entityMainColumnNames[DuckDBFoldIdentifier(ParquetAttrColumn(name))]; !ok {
			continue
		}
		if !found || name < first {
			first, found = name, true
		}
	}
	if !found {
		return nil
	}
	col := ParquetAttrColumn(first)
	return mainColumnNameError(first, col, DuckDBFoldIdentifier(col))
}
