package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// checkIndexedScalarRecords is the read-only census of #696: eav_data
// records of an attribute that is not a list, stored inside more arrays, or
// other arrays, than its schema declares. Before #696 a report-only update
// stored {"note": ["a"]} as element 0 of a scalar, and
// {"propertyInterests": [[{...}]]} as members at two indices where the
// schema declares one array of objects.
//
// array_indices alone is no evidence: a member of an array of objects, and an
// element of a primitive array the schema declares under an element-typed
// (pre-#204) ledger entry, are stored at indices too. So each group of
// records, by attribute and index count, is judged against the schema's
// container skeleton (schemavalidate.ShapeSchema.AdmitsIndices), the same
// skeleton an update's intake judges by: if no arrangement of that many
// arrays along the attribute's path is admitted, no write the schema admits
// stores such a record, and the group is a failure, naming one record by row
// and indices, never a value. A group the census cannot judge, because the
// schema has no skeleton or its document cannot be read, or because there
// are too many arrangements to try, is informational: unverified, not
// clean. A list's records are #372's census. Nothing is repaired; a
// confirmed finding needs a separately tracked remediation.
func (v schemaConsistencyValidator) checkIndexedScalarRecords(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	shapes, unverified := censusShapes(cache, v.schemaDir)
	query := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id,
	length(e.array_indices) - length(replace(e.array_indices, ',', '')) + 1 AS depth,
	COUNT(*) AS record_count,
	MIN(e.row_id::text || ' ' || e.array_indices) AS example
FROM %s AS e
WHERE e.array_indices <> ''
GROUP BY e.schema_id, e.attr_id, length(e.array_indices) - length(replace(e.array_indices, ',', '')) + 1
ORDER BY e.schema_id, e.attr_id, depth`, quoteIdentifier(v.eavTable))

	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query indexed records under scalar attributes: %w", err)
	}
	defer rows.Close()

	var issues []validationIssue
	for rows.Next() {
		var group indexedGroup
		if err := rows.Scan(&group.schemaID, &group.attrID, &group.depth, &group.count, &group.example); err != nil {
			return nil, fmt.Errorf("scan indexed records under scalar attributes: %w", err)
		}
		if issue, found := v.classifyIndexedGroup(cache, shapes, unverified, group); found {
			issues = append(issues, issue)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexed records under scalar attributes: %w", err)
	}
	return issues, nil
}

// indexedGroup is one census row: the records of one attribute at one index
// count, and the smallest "row_id array_indices" among them.
type indexedGroup struct {
	schemaID, attrID int16
	depth            int
	count            int64
	example          string
}

// classifyIndexedGroup judges one group. A record under an unknown schema
// or attribute is the attr_id census's finding, and a list's is #372's.
func (v schemaConsistencyValidator) classifyIndexedGroup(cache *schemameta.MetadataCache, shapes map[int16]*schemavalidate.ShapeSchema, unverified string, group indexedGroup) (validationIssue, bool) {
	schemaName, ok := cache.GetSchemaName(group.schemaID)
	if !ok {
		return validationIssue{}, false
	}
	meta, ok := attributeByID(cache, schemaName, group.attrID)
	if !ok || meta.ValueType == forma.ValueTypeList {
		return validationIssue{}, false
	}
	rowID, indices, _ := strings.Cut(group.example, " ")
	details := fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s type=%s indices=%d rows=%d example=row_id=%s array_indices=%q",
		schemaName, group.schemaID, group.attrID, meta.AttributeName, meta.ValueType, group.depth, group.count, rowID, indices)

	shape := shapes[group.schemaID]
	reason := unverified
	if shape != nil {
		admitted, decided := shape.AdmitsIndices(meta.AttributeName, group.depth)
		if admitted {
			return validationIssue{}, false
		}
		if decided {
			return validationIssue{
				category: "array elements stored where the schema declares no such arrays, under scalar attributes in " + v.eavTable,
				details:  details,
			}, true
		}
		reason = "too many arrangements of arrays along the attribute's path to try"
	}
	if reason == "" {
		reason = "the schema has no container skeleton"
	}
	return validationIssue{
		category: "indexed records under scalar attributes the census could not judge in " + v.eavTable,
		details:  details + " unverified=" + reason,
		severity: severityInfo,
	}, true
}

// attributeByID answers schemaName's active attribute with attrID.
func attributeByID(cache *schemameta.MetadataCache, schemaName string, attrID int16) (forma.AttributeMetadata, bool) {
	attrs, ok := cache.GetSchemaCache(schemaName)
	if !ok {
		return forma.AttributeMetadata{}, false
	}
	for _, meta := range attrs {
		if meta.AttributeID == attrID {
			return meta, true
		}
	}
	return forma.AttributeMetadata{}, false
}

// censusShapes builds every registered schema's container skeleton from its
// document in schemaDir, as the server's validator does. When that fails,
// none is built and the reason is answered, so every group is reported
// unverified rather than clean.
func censusShapes(cache *schemameta.MetadataCache, schemaDir string) (map[int16]*schemavalidate.ShapeSchema, string) {
	validator, err := schemavalidate.New(censusDocuments{cache: cache, dir: schemaDir}, schemaDir)
	if err != nil {
		return nil, fmt.Sprintf("the schema documents did not resolve (%v)", err)
	}
	shapes := make(map[int16]*schemavalidate.ShapeSchema)
	for _, name := range cache.ListSchemas() {
		if id, ok := cache.GetSchemaID(name); ok && validator.Shape(id) != nil {
			shapes[id] = validator.Shape(id)
		}
	}
	return shapes, ""
}

// censusDocuments serves schemavalidate.New the registered schemas'
// documents, read from schemaDir under the registry's schema ids.
type censusDocuments struct {
	cache *schemameta.MetadataCache
	dir   string
}

func (d censusDocuments) ListSchemas() []string { return d.cache.ListSchemas() }

func (d censusDocuments) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	id, ok := d.cache.GetSchemaID(name)
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema %q is not registered", name)
	}
	raw, err := os.ReadFile(filepath.Join(d.dir, name+".json"))
	if err != nil {
		return 0, forma.JSONSchema{}, fmt.Errorf("read the document of schema %q: %w", name, err)
	}
	return id, forma.JSONSchema{ID: id, Name: name, Schema: string(raw)}, nil
}

func (d censusDocuments) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	name, ok := d.cache.GetSchemaName(id)
	if !ok {
		return "", forma.JSONSchema{}, fmt.Errorf("schema id %d is not registered", id)
	}
	_, document, err := d.GetSchemaByName(name)
	return name, document, err
}

func (d censusDocuments) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, ok := d.cache.GetSchemaID(name)
	attrs, cached := d.cache.GetSchemaCache(name)
	if !ok || !cached {
		return 0, nil, fmt.Errorf("schema %q is not registered", name)
	}
	return id, attrs, nil
}

func (d censusDocuments) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, ok := d.cache.GetSchemaName(id)
	if !ok {
		return "", nil, fmt.Errorf("schema id %d is not registered", id)
	}
	_, attrs, err := d.GetSchemaAttributeCacheByName(name)
	return name, attrs, err
}
