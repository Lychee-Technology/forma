package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// entityMainColumnNames is the physical entity_main column set, keyed the way
// the registration guard looks names up (DuckDBFoldIdentifier), derived from
// model.EntityMainColumnDescriptors rather than restated: that list is the one
// statement of the column set (#585), so a column added there is reserved here
// by construction.
//
// The logical attribute namespace and the physical storage namespace must not
// overlap (#470). Several renderers resolve a bare name to the physical column
// before they consult the schema cache: hybridMainColumn and
// hasMainTableCondition on the PostgreSQL Query and CrossSchemaSearch paths,
// and the federated outer ORDER BY, where the outer select's per-column alias
// (NULL::VARCHAR AS text_01 when unbound) wins over the visible CTE column. An
// attribute named after a column is therefore stored and read back correctly
// but filtered and sorted on a different value.
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
