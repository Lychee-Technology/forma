package main

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// listAttrIndex maps schema_id → attr_id → attribute name for every attribute
// whose current metadata declares valueType list.
type listAttrIndex map[int16]map[int16]string

// schemaNames maps schema_id → schema name for the report line.
type schemaNames map[int16]string

func buildListAttrIndex(cache *schemameta.MetadataCache) (listAttrIndex, schemaNames) {
	lists := make(listAttrIndex)
	names := make(schemaNames)
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		names[schemaID] = schemaName
		for _, meta := range schemaCache {
			if meta.ValueType != forma.ValueTypeList {
				continue
			}
			if lists[schemaID] == nil {
				lists[schemaID] = make(map[int16]string)
			}
			lists[schemaID][meta.AttributeID] = meta.AttributeName
		}
	}
	return lists, names
}

// checkScalarRowsUnderListAttrs reports eav_data rows that store a scalar
// (array_indices ” with a value) under an attribute whose metadata now
// declares a list (#372). Such rows predate the write path's array
// enforcement (#314): the OLTP read still returns the scalar, while the
// parquet export and the DuckDB pivot read the row as a one-element list. The
// operator resolves the drift by rewriting the row to element index '0'
// (docs/schema-consistency-migration.md). The empty-list marker (#204) is the
// same key with both value columns NULL and is excluded by the query itself.
func (v schemaConsistencyValidator) checkScalarRowsUnderListAttrs(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	lists, names := buildListAttrIndex(cache)

	query := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, COUNT(*) AS record_count
FROM %s AS e
WHERE e.array_indices = '' AND (e.value_text IS NOT NULL OR e.value_numeric IS NOT NULL)
GROUP BY e.schema_id, e.attr_id
ORDER BY e.schema_id, e.attr_id`, quoteIdentifier(v.eavTable))

	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query scalar rows under list attributes: %w", err)
	}
	defer rows.Close()

	var issues []validationIssue
	for rows.Next() {
		var schemaID, attrID int16
		var count int64
		if err := rows.Scan(&schemaID, &attrID, &count); err != nil {
			return nil, fmt.Errorf("scan scalar rows under list attributes: %w", err)
		}
		// Every scalar attribute stores array_indices '', and rows under an
		// unknown schema or attr_id are the attr_id census's finding: only a
		// hit in the list index is this check's.
		attrName, ok := lists[schemaID][attrID]
		if !ok {
			continue
		}
		issues = append(issues, validationIssue{
			category: "scalar rows stored under list attributes in " + v.eavTable,
			details: fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s rows=%d",
				names[schemaID], schemaID, attrID, attrName, count),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scalar rows under list attributes: %w", err)
	}
	return issues, nil
}
