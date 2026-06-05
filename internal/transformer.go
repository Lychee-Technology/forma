package internal

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
	"sort"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type transformer struct {
	*schemaMetadataCache
	converter *AttributeConverter
}

// NewTransformer creates a new Transformer instance backed by the provided schema registry.
func NewTransformer(registry forma.SchemaRegistry) Transformer {
	return &transformer{
		schemaMetadataCache: newSchemaMetadataCache(registry),
		converter:           NewAttributeConverter(registry),
	}
}

func (t *transformer) ToAttributes(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) ([]EntityAttribute, error) {
	if jsonData == nil {
		return []EntityAttribute{}, nil
	}

	cache, _, err := t.getSchemaMetadata(schemaID)
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

	// Validate required attributes with parent-aware semantics before flattening.
	if err := validateRequiredAttributesFromInput(data, cache); err != nil {
		return nil, err
	}

	// First convert to EAVRecords internally
	eavRecords := make([]EAVRecord, 0)
	if err := t.flattenToAttributes(schemaID, rowID, nil, data, nil, cache, &eavRecords); err != nil {
		return nil, err
	}

	// Convert EAVRecords to EntityAttributes
	attributes, err := t.converter.FromEAVRecords(eavRecords)
	if err != nil {
		return nil, fmt.Errorf("convert to EntityAttribute: %w", err)
	}

	return attributes, nil
}

func (t *transformer) FromAttributes(ctx context.Context, attributes []EntityAttribute) (map[string]any, error) {
	if len(attributes) == 0 {
		return make(map[string]any), nil
	}

	result := make(map[string]any)

	for _, attr := range attributes {
		_, idToName, err := t.getSchemaMetadata(attr.SchemaID)
		if err != nil {
			return nil, err
		}

		attrName, ok := idToName[attr.AttrID]
		if !ok {
			return nil, fmt.Errorf("attribute id %d not found for schema %d", attr.AttrID, attr.SchemaID)
		}

		if attr.Value == nil {
			continue
		}

		indices, err := parseIndices(attr.ArrayIndices)
		if err != nil {
			return nil, fmt.Errorf("parse array indices for attribute '%s': %w", attrName, err)
		}

		segments := strings.Split(attrName, ".")
		if err := setValueAtPath(result, segments, indices, attr.Value); err != nil {
			return nil, fmt.Errorf("set value for attribute '%s': %w", attrName, err)
		}
	}

	return result, nil
}

func (t *transformer) BatchToAttributes(ctx context.Context, schemaID int16, jsonObjects []any) ([]EntityAttribute, error) {
	attributes := make([]EntityAttribute, 0)

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

func (t *transformer) BatchFromAttributes(ctx context.Context, attributes []EntityAttribute) ([]map[string]any, error) {
	if len(attributes) == 0 {
		return []map[string]any{}, nil
	}

	// Group by RowID directly from EntityAttribute
	groupedByRowID := make(map[uuid.UUID][]EntityAttribute)
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

func (t *transformer) flattenToAttributes(
	schemaID int16,
	rowID uuid.UUID,
	path []string,
	data any,
	indices []int,
	cache forma.SchemaAttributeCache,
	result *[]EAVRecord,
) error {
	switch v := data.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := v[key]
			if value == nil {
				candidateName := strings.Join(append(path, key), ".")
				if isKnownAttributeOrParent(candidateName, cache) {
					return fmt.Errorf("attribute '%s' cannot be set to null; omit the key to preserve its current value: %w", candidateName, forma.ErrInvalidInput)
				}
				continue
			}
			newPath := append(path, key)
			if err := t.flattenToAttributes(schemaID, rowID, newPath, value, indices, cache, result); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range v {
			if item == nil {
				attrName := strings.Join(path, ".")
				if isKnownAttributeOrParent(attrName, cache) {
					return fmt.Errorf("attribute '%s' cannot be set to null (array index %d); omit the element to preserve its current value: %w", attrName, i, forma.ErrInvalidInput)
				}
				continue
			}
			newIndices := append(indices, i)
			if err := t.flattenToAttributes(schemaID, rowID, path, item, newIndices, cache, result); err != nil {
				return err
			}
		}
	default:
		attrName := strings.Join(path, ".")
		meta, ok := cache[attrName]
		if !ok {
			return fmt.Errorf("attribute '%s' is not defined for schema %d: %w", attrName, schemaID, forma.ErrInvalidInput)
		}

		attr := EAVRecord{
			SchemaID:     schemaID,
			RowID:        rowID,
			AttrID:       meta.AttributeID,
			ArrayIndices: joinIndices(indices),
		}

		set, err := populateTypedValue(&attr, attrName, v, meta)
		if err != nil {
			return fmt.Errorf("convert value for attribute '%s': %w", attrName, err)
		}

		if set {
			*result = append(*result, attr)
		}
	}
	return nil
}

func populateTypedValue(attr *EAVRecord, attrName string, value any, meta forma.AttributeMetadata) (bool, error) {
	handleConversionError := func(err error) (bool, error) {
		return false, fmt.Errorf(
			"invalid value for attribute '%s' (attrID=%d): %w",
			attrName,
			meta.AttributeID,
			fmt.Errorf("%w: %v", forma.ErrInvalidInput, err),
		)
	}

	switch meta.ValueType {
	case forma.ValueTypeUUID:
		uuidVal, isUUID := toUUID(value)
		if !isUUID {
			return handleConversionError(fmt.Errorf("invalid UUID value: %v", value))
		}
		strVal := uuidVal.String()
		attr.ValueText = &strVal
	case forma.ValueTypeText:
		strVal, err := toString(value)
		if err != nil {
			return handleConversionError(err)
		}
		attr.ValueText = &strVal
	case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		numVal, err := toFloat64(value)
		if err != nil {
			return handleConversionError(err)
		}
		attr.ValueNumeric = &numVal
	case forma.ValueTypeDate:
		timeVal, err := toTime(value)
		if err != nil {
			return handleConversionError(err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		attr.ValueNumeric = &unixMillis
	case forma.ValueTypeBool:
		boolVal, err := toBool(value)
		if err != nil {
			return handleConversionError(err)
		}
		floatBool := boolToFloat64(boolVal)
		attr.ValueNumeric = &floatBool
	default:
		return handleConversionError(fmt.Errorf("unsupported value type '%s'", meta.ValueType))
	}

	return true, nil
}

func toString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

func validateRequiredAttributesFromInput(data map[string]any, cache forma.SchemaAttributeCache) error {
	if len(cache) == 0 {
		return nil
	}

	requiredNames := make([]string, 0, len(cache))
	for attrName, meta := range cache {
		switch meta.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent:
			requiredNames = append(requiredNames, attrName)
		}
	}
	sort.Strings(requiredNames)

	for _, attrName := range requiredNames {
		meta := cache[attrName]
		missing := false
		switch meta.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			missing = isRequiredAttributeMissingInInput(data, attrName, true)
		case forma.RequiredPolicyIfParentPresent:
			missing = isRequiredAttributeMissingInInput(data, attrName, false)
		default:
			missing = false
		}
		if missing {
			return fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records", attrName, meta.AttributeID)
		}
	}

	return nil
}

func isRequiredAttributeMissingInInput(root map[string]any, attrPath string, enforceWhenParentMissing bool) bool {
	segments := strings.Split(attrPath, ".")
	if len(segments) == 0 {
		return false
	}

	if len(segments) == 1 {
		value, exists := root[segments[0]]
		return !exists || value == nil
	}

	parentContexts := findExistingParentContexts(root, segments[:len(segments)-1])
	if len(parentContexts) == 0 {
		return enforceWhenParentMissing
	}

	leaf := segments[len(segments)-1]
	for _, parent := range parentContexts {
		parentMap, ok := parent.(map[string]any)
		if !ok {
			return true
		}

		value, exists := parentMap[leaf]
		if !exists || value == nil {
			return true
		}
	}

	return false
}

func findExistingParentContexts(root map[string]any, parentSegments []string) []any {
	contexts := []any{root}

	for _, segment := range parentSegments {
		nextContexts := make([]any, 0)
		for _, ctx := range contexts {
			obj, ok := ctx.(map[string]any)
			if !ok || obj == nil {
				continue
			}

			value, exists := obj[segment]
			if !exists || value == nil {
				continue
			}

			appendChildContexts(&nextContexts, value)
		}
		contexts = nextContexts
		if len(contexts) == 0 {
			return nil
		}
	}

	return contexts
}

func appendChildContexts(contexts *[]any, value any) {
	if arrayValue, ok := value.([]any); ok {
		for _, item := range arrayValue {
			if item != nil {
				*contexts = append(*contexts, item)
			}
		}
		return
	}
	*contexts = append(*contexts, value)
}

// Array handling functions (joinIndices, parseIndices, setValueAtPath, etc.)
// are implemented in transformer_array.go
