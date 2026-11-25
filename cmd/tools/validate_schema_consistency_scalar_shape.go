package main

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// The scalar-shape census (#696). Before #696 a report-only update could give
// a scalar attribute an array, ["a"], and the write stored its elements as
// records carrying array indices of their own. The census finds such records
// by reading what the schema document says a scalar's record may carry: one
// index per array of objects around it, and one more where the document
// declares the attribute itself a primitive array whose attributes file still
// records its element type (before #204), which the write and the read store
// that way. Indexed records alone are no anomaly; a member of an array of
// objects carries the indices of its element.
//
// A record is reported as a failure only where the document settles the
// attribute's array positions (schemameta.DeriveArrayPositions): every node
// on its path has a type that fixes it as an object, an array of objects, a
// single value or a primitive array. Elsewhere, a node without a type, a
// $ref to another file, or a type admitting both an object and an array, an
// index may belong to an array the document admits as well as to the
// attribute's own array, so the census reports the records as informational,
// unverified, rather than counting them as clean or as corrupt. No record is
// repaired here, and no value is printed: each finding names one record by
// row ID and indices.

// scalarShapeAttr is an active scalar attribute and its array positions,
// what the census judges its records against.
type scalarShapeAttr struct {
	schema    string
	meta      forma.AttributeMetadata
	positions schemameta.ArrayPositions
}

// scalarShapeAttrs indexes every active attribute that holds a single value,
// lists excluded, by schema ID and attribute ID, with the array positions its
// schema's document settles.
func scalarShapeAttrs(cache *schemameta.MetadataCache, schemaDir string) (map[int16]map[int16]scalarShapeAttr, error) {
	attrs := make(map[int16]map[int16]scalarShapeAttr)
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		positions, err := schemameta.DeriveArrayPositions(schemaDir, schemaName, schemaCache)
		if err != nil {
			return nil, fmt.Errorf("derive the array positions of schema %s: %w", schemaName, err)
		}
		for name, meta := range schemaCache {
			if meta.ValueType == forma.ValueTypeList {
				continue
			}
			if attrs[schemaID] == nil {
				attrs[schemaID] = make(map[int16]scalarShapeAttr)
			}
			meta.AttributeName = name
			attrs[schemaID][meta.AttributeID] = scalarShapeAttr{schema: schemaName, meta: meta, positions: positions[name]}
		}
	}
	return attrs, nil
}

// indexDepthSQL is the number of indices in a non-empty array_indices.
const indexDepthSQL = "LENGTH(e.array_indices) - LENGTH(REPLACE(e.array_indices, ',', '')) + 1"

// scalarShapeGroup is the indexed records of one attribute with one number
// of indices.
type scalarShapeGroup struct {
	schemaID, attrID int16
	depth            int
	count            int64
}

// checkIndexedRowsUnderScalarAttrs reports the indexed records of scalar
// attributes that carry more indices than their schema document admits, and,
// as informational, those whose array positions the document does not settle.
func (v schemaConsistencyValidator) checkIndexedRowsUnderScalarAttrs(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	attrs, err := scalarShapeAttrs(cache, v.schemaDir)
	if err != nil {
		return nil, err
	}
	groups, err := v.indexedRecordGroups(ctx)
	if err != nil {
		return nil, err
	}
	var issues []validationIssue
	for _, group := range groups {
		attr, ok := attrs[group.schemaID][group.attrID]
		if !ok {
			// A list's elements, or an attribute the attr_id census reports.
			continue
		}
		issue, found := classifyScalarShape(v.eavTable, attr, group)
		if !found {
			continue
		}
		example, err := v.indexedRecordExample(ctx, group)
		if err != nil {
			return nil, err
		}
		issue.details += " example=" + example
		issues = append(issues, issue)
	}
	return issues, nil
}

// classifyScalarShape judges one group of an attribute's indexed records.
func classifyScalarShape(eavTable string, attr scalarShapeAttr, group scalarShapeGroup) (validationIssue, bool) {
	meta, positions := attr.meta, attr.positions
	details := fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s type=%s indices=%d",
		attr.schema, group.schemaID, group.attrID, meta.AttributeName, meta.ValueType, group.depth)
	if !positions.Verified {
		return validationIssue{
			category: "indexed scalar records whose array positions the schema document does not settle in " + eavTable,
			details:  fmt.Sprintf("%s array_positions=unverified rows=%d", details, group.count),
			severity: severityInfo,
		}, true
	}
	if group.depth <= positions.MaxIndices() {
		return validationIssue{}, false
	}
	declared := ""
	if positions.DeclaredArray {
		declared = " declared_array=true"
	}
	return validationIssue{
		category: "array elements stored under scalar attributes in " + eavTable,
		details:  fmt.Sprintf("%s array_ancestors=%d%s rows=%d", details, len(positions.Ancestors), declared, group.count),
	}, true
}

// indexedRecordGroups counts the indexed records of eav_data by attribute
// and number of indices.
func (v schemaConsistencyValidator) indexedRecordGroups(ctx context.Context) ([]scalarShapeGroup, error) {
	query := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, %s AS depth, COUNT(*) AS record_count
FROM %s AS e
WHERE e.array_indices <> ''
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3`, indexDepthSQL, quoteIdentifier(v.eavTable))
	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query indexed records by attribute: %w", err)
	}
	defer rows.Close()
	var groups []scalarShapeGroup
	for rows.Next() {
		var group scalarShapeGroup
		if err := rows.Scan(&group.schemaID, &group.attrID, &group.depth, &group.count); err != nil {
			return nil, fmt.Errorf("scan indexed records by attribute: %w", err)
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexed records by attribute: %w", err)
	}
	return groups, nil
}

// indexedRecordExample names one record of group, its row ID and indices
// read from the same record, never its value.
func (v schemaConsistencyValidator) indexedRecordExample(ctx context.Context, group scalarShapeGroup) (string, error) {
	query := fmt.Sprintf(`
SELECT e.row_id::text, e.array_indices
FROM %s AS e
WHERE e.schema_id = $1 AND e.attr_id = $2 AND e.array_indices <> '' AND %s = $3
ORDER BY e.row_id, e.array_indices
LIMIT 1`, quoteIdentifier(v.eavTable), indexDepthSQL)
	rows, err := v.pool.Query(ctx, query, group.schemaID, group.attrID, group.depth)
	if err != nil {
		return "", fmt.Errorf("query an example record of schema %d attribute %d: %w", group.schemaID, group.attrID, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("read an example record of schema %d attribute %d: %w", group.schemaID, group.attrID, err)
		}
		return "none (the records changed during the census)", nil
	}
	var rowID, indices string
	if err := rows.Scan(&rowID, &indices); err != nil {
		return "", fmt.Errorf("scan an example record of schema %d attribute %d: %w", group.schemaID, group.attrID, err)
	}
	return fmt.Sprintf("row_id=%s array_indices=%q", rowID, indices), nil
}
