package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

type transformer struct {
	registry SchemaRegistry

	cacheMu     sync.RWMutex
	attrCache   map[int16]forma.SchemaAttributeCache // schemaID -> attrName -> meta
	idNameCache map[int16]map[int16]string           // schemaID -> attrID -> attrName
}

// NewTransformer creates a new Transformer instance backed by the provided schema registry.
func NewTransformer(registry SchemaRegistry) Transformer {
	return &transformer{
		registry:    registry,
		attrCache:   make(map[int16]forma.SchemaAttributeCache),
		idNameCache: make(map[int16]map[int16]string),
	}
}

func (t *transformer) ToAttributes(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) ([]Attribute, error) {
	if jsonData == nil {
		return []Attribute{}, nil
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

	attributes := make([]Attribute, 0)
	if err := t.flattenToAttributes(schemaID, rowID, nil, data, nil, cache, &attributes); err != nil {
		return nil, err
	}
	return attributes, nil
}

func (t *transformer) FromAttributes(ctx context.Context, attributes []Attribute) (map[string]any, error) {
	if len(attributes) == 0 {
		return make(map[string]any), nil
	}

	result := make(map[string]any)

	for _, attr := range attributes {
		cache, idToName, err := t.getSchemaMetadata(attr.SchemaID)
		if err != nil {
			return nil, err
		}

		attrName, ok := idToName[attr.AttrID]
		if !ok {
			return nil, fmt.Errorf("attribute id %d not found for schema %d", attr.AttrID, attr.SchemaID)
		}

		meta := cache[attrName]
		value, err := attributeValue(attr, meta.ValueType)
		if err != nil {
			return nil, fmt.Errorf("read value for attribute '%s': %w", attrName, err)
		}

		if value == nil {
			continue
		}

		indices, err := parseIndices(attr.ArrayIndices)
		if err != nil {
			return nil, fmt.Errorf("parse array indices for attribute '%s': %w", attrName, err)
		}

		segments := strings.Split(attrName, ".")
		if err := setValueAtPath(result, segments, indices, value); err != nil {
			return nil, fmt.Errorf("set value for attribute '%s': %w", attrName, err)
		}
	}

	return result, nil
}

func (t *transformer) BatchToAttributes(ctx context.Context, schemaID int16, jsonObjects []any) ([]Attribute, error) {
	attributes := make([]Attribute, 0)

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

func (t *transformer) BatchFromAttributes(ctx context.Context, attributes []Attribute) ([]map[string]any, error) {
	groupedByRowID := make(map[uuid.UUID][]Attribute)
	for _, attr := range attributes {
		groupedByRowID[attr.RowID] = append(groupedByRowID[attr.RowID], attr)
	}

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

func (t *transformer) getSchemaMetadata(schemaID int16) (forma.SchemaAttributeCache, map[int16]string, error) {
	t.cacheMu.RLock()
	cache, okCache := t.attrCache[schemaID]
	idMap, okMap := t.idNameCache[schemaID]
	t.cacheMu.RUnlock()

	if okCache && okMap {
		return cache, idMap, nil
	}

	if t.registry == nil {
		return nil, nil, fmt.Errorf("schema registry is not configured")
	}

	_, schemaCache, err := t.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load schema metadata for id %d: %w", schemaID, err)
	}

	idToName := make(map[int16]string, len(schemaCache))
	for name, meta := range schemaCache {
		idToName[meta.AttributeID] = name
	}

	t.cacheMu.Lock()
	t.attrCache[schemaID] = schemaCache
	t.idNameCache[schemaID] = idToName
	t.cacheMu.Unlock()

	return schemaCache, idToName, nil
}

func (t *transformer) flattenToAttributes(
	schemaID int16,
	rowID uuid.UUID,
	path []string,
	data any,
	indices []int,
	cache forma.SchemaAttributeCache,
	result *[]Attribute,
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

		attr := Attribute{
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

func populateTypedValue(attr *Attribute, value any, valueType forma.ValueType) error {
	switch valueType {
	case forma.ValueTypeText:
		strVal, err := toString(value)
		if err != nil {
			return err
		}
		attr.ValueText = &strVal
	case forma.ValueTypeNumeric:
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
		attr.ValueDate = &timeVal
	case forma.ValueTypeBool:
		boolVal, err := toBool(value)
		if err != nil {
			return err
		}
		attr.ValueBool = &boolVal
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

func attributeValue(attr Attribute, valueType forma.ValueType) (any, error) {
	switch valueType {
	case forma.ValueTypeText:
		if attr.ValueText == nil {
			return nil, nil
		}
		return *attr.ValueText, nil
	case forma.ValueTypeNumeric:
		if attr.ValueNumeric == nil {
			return nil, nil
		}
		return *attr.ValueNumeric, nil
	case forma.ValueTypeDate:
		if attr.ValueDate == nil {
			return nil, nil
		}
		return *attr.ValueDate, nil
	case forma.ValueTypeBool:
		if attr.ValueBool == nil {
			return nil, nil
		}
		return *attr.ValueBool, nil
	default:
		if attr.ValueText != nil {
			return *attr.ValueText, nil
		}
		if attr.ValueNumeric != nil {
			return *attr.ValueNumeric, nil
		}
		if attr.ValueDate != nil {
			return *attr.ValueDate, nil
		}
		if attr.ValueBool != nil {
			return *attr.ValueBool, nil
		}
		return nil, nil
	}
}

func setValueAtPath(target map[string]any, segments []string, indices []int, value any) error {
	if len(segments) == 0 {
		return fmt.Errorf("empty attribute path")
	}

	// Navigate to the parent of the last segment (all segments are objects until the last one)
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

	// Handle the last segment - this is where arrays are used
	lastSegment := segments[len(segments)-1]

	if len(indices) == 0 {
		// Simple scalar value
		current[lastSegment] = value
		return nil
	}

	// Value is inside an array (or nested arrays)
	// Get or create the array
	existing := current[lastSegment]
	var arr []any
	if existing != nil {
		var ok bool
		arr, ok = existing.([]any)
		if !ok {
			return fmt.Errorf("expected array at %s but found %T", strings.Join(segments, "."), existing)
		}
	}
	if arr == nil {
		arr = []any{}
	}

	// Set the value in the nested array structure
	arr = setArrayValueRecursive(arr, indices, value)
	current[lastSegment] = arr

	return nil
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
