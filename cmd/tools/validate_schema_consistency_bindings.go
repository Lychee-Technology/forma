package main

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/schemameta"
)

// checkColumnBindings reports every active binding whose valueType cannot
// round-trip through its main column (#459). Registration refuses such a
// binding for new schemas, but a schema deployed before that guard still
// carries it — and with it a write path that answers a redacted 500 (text→
// uuid column) or silently drops the value (text→numeric column). The tool
// loads through DeferColumnBindingCheck so the whole set is listed at once.
func (v schemaConsistencyValidator) checkColumnBindings(cache *schemameta.MetadataCache) []validationIssue {
	var issues []validationIssue
	for _, schemaName := range cache.ListSchemas() {
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		for attrName, meta := range schemaCache {
			if err := schemameta.ValidateColumnBinding(attrName, meta); err != nil {
				issues = append(issues, validationIssue{
					category: "valueType/column-encoding binding mismatches",
					details:  fmt.Sprintf("schema=%s %v", schemaName, err),
				})
			}
		}
	}
	return issues
}
