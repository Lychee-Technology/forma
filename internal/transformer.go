package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
				continue
			}
			newPath := append(path, key)
			if err := t.flattenToAttributes(schemaID, rowID, newPath, value, indices, cache, result); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range v {
			newIndices := append(indices, i)
			if err := t.flattenToAttributes(schemaID, rowID, path, item, newIndices, cache, result); err != nil {
				return err
			}
		}
	default:
		attrName := strings.Join(path, ".")
		meta, ok := cache[attrName]
		if !ok {
			return fmt.Errorf("attribute '%s' not defined for schema %d", attrName, schemaID)
		}

		attr := EAVRecord{
			SchemaID:     schemaID,
			RowID:        rowID,
			AttrID:       meta.AttributeID,
			ArrayIndices: joinIndices(indices),
		}

		if err := populateTypedValue(&attr, v, meta.ValueType); err != nil {
			return fmt.Errorf("convert value for attribute '%s': %w", attrName, err)
		}

		*result = append(*result, attr)
	}
	return nil
}

func populateTypedValue(attr *EAVRecord, value any, valueType forma.ValueType) error {
	switch valueType {
	case forma.ValueTypeUUID:
		uuidVal, isUUID := toUUID(value)
		if !isUUID {
			return fmt.Errorf("invalid UUID value: %v", value)
		}
		strVal := uuidVal.String()
		attr.ValueText = &strVal
	case forma.ValueTypeText:
		strVal, err := toString(value)
		if err != nil {
			return err
		}
		attr.ValueText = &strVal
	case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		numVal, err := toFloat64(value)
		if err != nil {
			return err
		}
		attr.ValueNumeric = &numVal
	case forma.ValueTypeDate:
		timeVal, err := toTime(value)
		if err != nil {
			return err
		}
		unixMillis := float64(timeVal.UnixMilli())
		attr.ValueNumeric = &unixMillis
	case forma.ValueTypeBool:
		boolVal, err := toBool(value)
		if err != nil {
			return err
		}
		var floatBool float64
		if boolVal {
			floatBool = 1.0
		} else {
			floatBool = 0.0
		}
		attr.ValueNumeric = &floatBool
	default:
		return fmt.Errorf("unsupported value type '%s'", valueType)
	}
	return nil
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

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		epoch, err := strconv.ParseInt(value.(string), 10, 64)
		if err == nil {
			return time.UnixMilli(epoch), nil
		}

		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02",
			"2006-01",
		}
		for _, format := range formats {
			if parsed, err := time.Parse(format, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func joinIndices(indices []int) string {
	if len(indices) == 0 {
		return ""
	}
	parts := make([]string, len(indices))
	for i, idx := range indices {
		parts[i] = strconv.Itoa(idx)
	}
	return strings.Join(parts, ",")
}

func parseIndices(indices string) ([]int, error) {
	if indices == "" {
		return nil, nil
	}
	parts := strings.Split(indices, ",")
	result := make([]int, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty index")
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid index '%s'", part)
		}
		result[i] = value
	}
	return result, nil
}

func setValueAtPath(target map[string]any, segments []string, indices []int, value any) error {
	if len(segments) == 0 {
		return fmt.Errorf("empty attribute path")
	}

	if len(indices) == 0 {
		// No indices - simple nested object path
		current := target
		for i := 0; i < len(segments)-1; i++ {
			segment := segments[i]
			next, ok := current[segment].(map[string]any)
			if !ok || next == nil {
				next = make(map[string]any)
				current[segment] = next
			}
			current = next
		}
		current[segments[len(segments)-1]] = value
		return nil
	}

	// Has indices - need to handle arrays
	if len(segments) == 1 {
		// Simple array: e.g., tags[0] = "value"
		segment := segments[0]
		arr := ensureArray(target, segment)
		arr = setArrayValueRecursive(arr, indices, value)
		target[segment] = arr
		return nil
	}

	// Decide whether to keep the parent as an object (array of primitives under a field)
	// or to build an array of objects.
	containerParent := target
	for i := 0; i < len(segments)-2; i++ {
		segment := segments[i]
		next, ok := containerParent[segment].(map[string]any)
		if !ok || next == nil {
			next = make(map[string]any)
			containerParent[segment] = next
		}
		containerParent = next
	}

	arraySegment := segments[len(segments)-2]
	lastSegment := segments[len(segments)-1]

	if existingMap, ok := containerParent[arraySegment].(map[string]any); ok && existingMap != nil {
		// Parent already a map (e.g., contact) – attach the array to the field instead of
		// turning the parent into an array of objects.
		arr := ensureArray(existingMap, lastSegment)
		arr = setArrayValueRecursive(arr, indices, value)
		existingMap[lastSegment] = arr
		containerParent[arraySegment] = existingMap
		return nil
	}

	// Default: array of objects
	arr := ensureArray(containerParent, arraySegment)
	arr = setObjectArrayValue(arr, indices, lastSegment, value)
	containerParent[arraySegment] = arr

	return nil
}

func ensureArray(target map[string]any, key string) []any {
	existing := target[key]
	if existing == nil {
		return []any{}
	}
	if arr, ok := existing.([]any); ok {
		return arr
	}
	return []any{}
}

func setObjectArrayValue(arr []any, indices []int, fieldName string, value any) []any {
	if len(indices) == 0 {
		return arr
	}

	idx := indices[0]
	if idx < 0 {
		return arr
	}

	// Expand array if needed
	for len(arr) <= idx {
		arr = append(arr, nil)
	}

	if len(indices) == 1 {
		// Last index - set the field in the object at this index
		var obj map[string]any
		if arr[idx] != nil {
			if existing, ok := arr[idx].(map[string]any); ok {
				obj = existing
			} else {
				// If it's not an object, we have a conflict - replace with object
				obj = make(map[string]any)
			}
		} else {
			obj = make(map[string]any)
		}
		obj[fieldName] = value
		arr[idx] = obj
	} else {
		// More indices - need nested array within the object
		var obj map[string]any
		if arr[idx] != nil {
			if existing, ok := arr[idx].(map[string]any); ok {
				obj = existing
			} else {
				obj = make(map[string]any)
			}
		} else {
			obj = make(map[string]any)
		}

		// Get or create nested array
		var nestedArr []any
		if existingNested := obj[fieldName]; existingNested != nil {
			if nested, ok := existingNested.([]any); ok {
				nestedArr = nested
			}
		}
		if nestedArr == nil {
			nestedArr = []any{}
		}

		// Recursively set in nested array
		nestedArr = setObjectArrayValue(nestedArr, indices[1:], fieldName, value)
		obj[fieldName] = nestedArr
		arr[idx] = obj
	}

	return arr
}

func setArrayValueRecursive(arr []any, indices []int, value any) []any {
	if len(indices) == 0 {
		return arr
	}

	idx := indices[0]
	if idx < 0 {
		return arr
	}

	// Expand array if needed
	for len(arr) <= idx {
		arr = append(arr, nil)
	}

	if len(indices) == 1 {
		// Last index - set the value directly
		arr[idx] = value
	} else {
		// More indices - need nested array
		var nestedArr []any
		if arr[idx] != nil {
			if existing, ok := arr[idx].([]any); ok {
				nestedArr = existing
			}
		}
		if nestedArr == nil {
			nestedArr = []any{}
		}
		arr[idx] = setArrayValueRecursive(nestedArr, indices[1:], value)
	}

	return arr
}
