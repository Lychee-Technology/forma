package transform

// Error handling strategy:
//
// Write-path validation errors wrap forma.ErrInvalidInput so callers can map
// them to user-facing 4xx responses.
//
// Read-path consistency failures, such as metadata drift or storage-column
// mismatches, return plain errors because they indicate system state problems
// rather than invalid caller input.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

type transformer struct {
	registry  forma.SchemaRegistry
	converter *AttributeConverter
}

// NewTransformer creates a new Transformer instance backed by the provided schema registry.
func NewTransformer(registry forma.SchemaRegistry) *transformer {
	return &transformer{
		registry:  registry,
		converter: NewAttributeConverter(registry),
	}
}

// SetRelationRoots forwards the relation-root lookup to the converter that
// performs the required-policy check (#314/#315).
func (t *transformer) SetRelationRoots(lookup RelationRootsLookup) {
	t.converter.SetRelationRoots(lookup)
}

func (t *transformer) ToAttributes(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) ([]model.EntityAttribute, error) {
	if jsonData == nil {
		return []model.EntityAttribute{}, nil
	}

	cache, _, err := schemameta.GetSchemaMetadata(t.registry, schemaID)
	if err != nil {
		return nil, err
	}

	var data map[string]any
	switch v := jsonData.(type) {
	case map[string]any:
		data = v
	case []byte:
		if err := json.Unmarshal(v, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	case string:
		if err := json.Unmarshal([]byte(v), &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	default:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON data: %w", err)
		}
		if err := json.Unmarshal(jsonBytes, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	}

	// The relation roots are resolved once here and handed to both required
	// checks on this write, the walk's and the converter's below, so they
	// carve the same names out of the same registry read (#389).
	relationRoots, err := t.converter.relationRootsFor(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve relation roots for required-attribute check: %w", err)
	}

	entries, err := walkDocument(schemaID, rowID, data, cache)
	if err != nil {
		return nil, err
	}
	// The required policy is judged on what the write stores, before any
	// value is converted, so a missing attribute is reported ahead of a
	// refused value as it always has been.
	if err := requireWrittenAttributes(entries, cache, relationRoots); err != nil {
		return nil, err
	}
	flattened, err := flattenToAttributes(entries)
	if err != nil {
		return nil, err
	}

	// Dotted attribute names let one payload spell the same attribute twice;
	// the last spelling wins, whole attribute at a time (#312).
	eavRecords := dedupeEAVRecords(flattened)

	// Convert EAVRecords to EntityAttributes, reusing the relation roots
	// resolved above rather than reading the registry a second time.
	attributes, err := t.converter.fromEAVRecords(eavRecords, relationRoots)
	if err != nil {
		return nil, fmt.Errorf("convert to model.EntityAttribute: %w", err)
	}

	return attributes, nil
}

// FromAttributes rebuilds a row's document from its attributes, in whatever
// order they arrive (rebuildDocument, #619). A record with no value is
// dropped, except a list's empty-list marker (no indices, no value), which
// rebuilds as an explicit [] (#204).
func (t *transformer) FromAttributes(ctx context.Context, attributes []model.EntityAttribute) (map[string]any, error) {
	records := make([]rebuildRecord, 0, len(attributes))
	for _, attr := range attributes {
		cache, idToName, err := schemameta.GetSchemaMetadata(t.registry, attr.SchemaID)
		if err != nil {
			return nil, fmt.Errorf("load metadata of schema %d: %w", attr.SchemaID, err)
		}
		attrName, ok := idToName[attr.AttrID]
		if !ok {
			return nil, fmt.Errorf("attribute id %d not found for schema %d", attr.AttrID, attr.SchemaID)
		}
		list := cache[attrName].ValueType == forma.ValueTypeList
		value := attr.Value
		if value == nil {
			if !list || attr.ArrayIndices != "" {
				continue
			}
			value = []any{}
		}
		indices, err := parseIndices(attr.ArrayIndices)
		if err != nil {
			return nil, fmt.Errorf("parse array indices for attribute '%s': %w", attrName, err)
		}
		records = append(records, rebuildRecord{
			segments: strings.Split(attrName, "."), list: list, indices: indices, value: value,
		})
	}
	return rebuildDocument(records), nil
}

func (t *transformer) BatchToAttributes(ctx context.Context, schemaID int16, jsonObjects []any) ([]model.EntityAttribute, error) {
	attributes := make([]model.EntityAttribute, 0)

	for _, obj := range jsonObjects {
		var rowID uuid.UUID
		if objMap, ok := obj.(map[string]any); ok {
			if idVal, exists := objMap["id"]; exists {
				if idStr, ok := idVal.(string); ok {
					parsedID, err := uuid.Parse(idStr)
					if err == nil {
						rowID = parsedID
					}
				}
			}
		}

		if rowID == (uuid.UUID{}) {
			rowID = uuid.Must(uuid.NewV7())
		}

		attrs, err := t.ToAttributes(ctx, schemaID, rowID, obj)
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, attrs...)
	}

	return attributes, nil
}

func (t *transformer) BatchFromAttributes(ctx context.Context, attributes []model.EntityAttribute) ([]map[string]any, error) {
	if len(attributes) == 0 {
		return []map[string]any{}, nil
	}

	// Group by RowID directly from model.EntityAttribute
	groupedByRowID := make(map[uuid.UUID][]model.EntityAttribute)
	for _, attr := range attributes {
		groupedByRowID[attr.RowID] = append(groupedByRowID[attr.RowID], attr)
	}

	// Convert each group back to JSON
	results := make([]map[string]any, 0, len(groupedByRowID))
	for _, attrs := range groupedByRowID {
		result, err := t.FromAttributes(ctx, attrs)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

func (t *transformer) ValidateAgainstSchema(ctx context.Context, jsonSchema any, jsonData any) error {
	var schemaMap map[string]any
	switch s := jsonSchema.(type) {
	case map[string]any:
		schemaMap = s
	case []byte:
		if err := json.Unmarshal(s, &schemaMap); err != nil {
			return fmt.Errorf("failed to unmarshal JSON schema: %w", err)
		}
	case string:
		if err := json.Unmarshal([]byte(s), &schemaMap); err != nil {
			return fmt.Errorf("failed to unmarshal JSON schema: %w", err)
		}
	default:
		jsonBytes, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("failed to marshal schema: %w", err)
		}
		if err := json.Unmarshal(jsonBytes, &schemaMap); err != nil {
			return fmt.Errorf("failed to unmarshal schema: %w", err)
		}
	}

	var dataToValidate any
	switch d := jsonData.(type) {
	case []byte:
		if err := json.Unmarshal(d, &dataToValidate); err != nil {
			return fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	case string:
		if err := json.Unmarshal([]byte(d), &dataToValidate); err != nil {
			return fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	default:
		dataToValidate = d
	}

	var schema jsonschema.Schema
	schemaBytes, err := json.Marshal(schemaMap)
	if err != nil {
		return fmt.Errorf("failed to marshal schema for validation: %w", err)
	}
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return fmt.Errorf("failed to unmarshal into jsonschema.Schema: %w", err)
	}

	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		return fmt.Errorf("failed to resolve JSON schema: %w", err)
	}

	if err := resolved.Validate(dataToValidate); err != nil {
		return fmt.Errorf("JSON validation failed: %w", err)
	}

	return nil
}

// isKnownAttributeOrParent returns true if name is a leaf attribute in the cache
// or is a parent path prefix of at least one cached attribute.
func isKnownAttributeOrParent(name string, cache forma.SchemaAttributeCache) bool {
	if _, ok := cache[name]; ok {
		return true
	}
	prefix := name + "."
	for attrName := range cache {
		if strings.HasPrefix(attrName, prefix) {
			return true
		}
	}
	return false
}

// The document walk that ToAttributes reads (walkDocument, flattenToAttributes,
// requireWrittenAttributes) lives in document_walk.go; the rebuild that
// FromAttributes runs (rebuildDocument) in document_rebuild.go; index and
// array helpers (joinIndices, parseIndices, setArrayValueRecursive, etc.) in
// array_paths.go.
