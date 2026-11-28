package main

import (
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/schemameta"
)

// checkColumnBindings reports every active binding whose col_name is not a
// column entity_main has (#557), that declares a smallint or integer over a
// system timestamp column (#745), or whose valueType cannot round-trip
// through its main column (#459), each under its own category. Registration
// refuses all three for new schemas, but a schema deployed before those
// guards still carries them — and with them a write path that fails every
// write with "unsupported column", answers a redacted 500 (text→uuid column)
// or silently drops the value (text→numeric column), or, for a #745 alias, a
// read that answered a wrapped value in place of the stamp and a runtime
// that now refuses to load the schema. The tool loads through
// DeferColumnBindingCheck so the whole set is listed at once.
func (v schemaConsistencyValidator) checkColumnBindings(cache *schemameta.MetadataCache) []validationIssue {
	var issues []validationIssue
	for _, schemaName := range cache.ListSchemas() {
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		for attrName, meta := range schemaCache {
			err := schemameta.ValidateColumnBinding(attrName, meta)
			if err == nil {
				continue
			}
			category := "valueType/column-encoding binding mismatches"
			switch {
			case errors.Is(err, schemameta.ErrUnknownMainColumn):
				category = "column bindings to unknown entity_main columns"
			case errors.Is(err, schemameta.ErrNarrowSystemAlias):
				category = "smallint/integer aliases of system timestamp columns"
			}
			issues = append(issues, validationIssue{
				category: category,
				details:  fmt.Sprintf("schema=%s %v", schemaName, err),
			})
		}
	}
	return issues
}
