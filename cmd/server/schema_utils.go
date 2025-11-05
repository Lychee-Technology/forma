package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lychee.technology/ltbase/forma"
)

type rawSchema struct {
	ID          int16          `json:"id"`
	Name        string         `json:"name"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	SchemaURI   string         `json:"$schema"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Schema      map[string]any `json:"schema"`
}

func loadTransformerSchemaFromFile(path string) (*forma.JSONSchema, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(data, &schemaMap); err != nil {
		return nil, "", err
	}

	raw := rawSchema{
		SchemaURI: extractString(schemaMap["$schema"]),
		Schema:    schemaMap,
	}

	if idVal, ok := schemaMap["id"]; ok {
		if id, err := extractInt16(idVal); err == nil {
			raw.ID = id
		}
	}
	if name := extractString(schemaMap["name"]); name != "" {
		raw.Name = name
	}
	if title := extractString(schemaMap["title"]); title != "" {
		raw.Title = title
	}
	if desc := extractString(schemaMap["description"]); desc != "" {
		raw.Description = desc
	}
	if createdAt := extractTime(schemaMap["created_at"]); !createdAt.IsZero() {
		raw.CreatedAt = createdAt
	}
	if updatedAt := extractTime(schemaMap["updated_at"]); !updatedAt.IsZero() {
		raw.UpdatedAt = updatedAt
	}

	if raw.ID == 0 {
		raw.ID = generateStableSchemaID(path)
	}
	if raw.Name == "" {
		raw.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}

	jsonSchema, err := buildTransformerSchema(raw.ID, raw.Name, schemaMap)
	if err != nil {
		return nil, "", err
	}

	return jsonSchema, raw.Name, nil
}

func buildTransformerSchema(schemaID int16, schemaName string, raw map[string]any) (*forma.JSONSchema, error) {
	if raw == nil {
		return nil, fmt.Errorf("schema definition is nil")
	}

	props, ok := raw["properties"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema missing properties definition")
	}

	required := extractStringSlice(raw["required"])
	parsedProps, err := parsePropertyMap(props, buildRequiredSet(required))
	if err != nil {
		return nil, err
	}

	return &forma.JSONSchema{
		ID:         schemaID,
		Name:       schemaName,
		Version:    1,
		Schema:     extractString(raw["$schema"]),
		Properties: parsedProps,
		Required:   required,
		CreatedAt:  time.Now().Unix(),
	}, nil
}

func parsePropertyMap(props map[string]any, required map[string]struct{}) (map[string]*forma.PropertySchema, error) {
	result := make(map[string]*forma.PropertySchema, len(props))
	for key, value := range props {
		propMap, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("property %q is not an object", key)
		}
		prop, err := buildPropertySchema(key, propMap, required)
		if err != nil {
			return nil, err
		}
		result[key] = prop
	}
	return result, nil
}

func buildPropertySchema(name string, raw map[string]any, requiredSet map[string]struct{}) (*forma.PropertySchema, error) {
	prop := &forma.PropertySchema{
		Name:     name,
		Type:     extractString(raw["type"]),
		Format:   extractString(raw["format"]),
		Pattern:  extractString(raw["pattern"]),
		Required: hasKey(requiredSet, name),
		Default:  raw["default"],
		Enum:     extractInterfaceSlice(raw["enum"]),
	}

	if prop.Type == "" {
		prop.Type = "string"
	}

	if min, ok := extractFloat64(raw["minimum"]); ok {
		prop.Minimum = &min
	}
	if max, ok := extractFloat64(raw["maximum"]); ok {
		prop.Maximum = &max
	}
	if minLength, ok := extractInt(raw["minLength"]); ok {
		prop.MinLength = &minLength
	}
	if maxLength, ok := extractInt(raw["maxLength"]); ok {
		prop.MaxLength = &maxLength
	}

	if items, ok := raw["items"].(map[string]any); ok {
		itemSchema, err := buildPropertySchema(name+"_item", items, nil)
		if err != nil {
			return nil, err
		}
		prop.Items = itemSchema
	}

	if prop.Type == "object" {
		childRequired := extractStringSlice(raw["required"])
		childPropsRaw, _ := raw["properties"].(map[string]any)
		childProps, err := parsePropertyMap(childPropsRaw, buildRequiredSet(childRequired))
		if err != nil {
			return nil, err
		}
		prop.Properties = childProps
		if storage := extractString(raw["x-storage"]); storage != "" {
			prop.Storage = storage
		}
	}

	if relationRaw, ok := raw["x-relation"].(map[string]any); ok {
		prop.Relation = &forma.RelationSchema{
			Target: extractString(relationRaw["target"]),
			Type:   extractString(relationRaw["type"]),
		}
	}

	return prop, nil
}

func extractString(value any) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return strings.TrimSpace(str)
	}
	return ""
}

func extractStringSlice(value any) []string {
	switch v := value.(type) {
	case []any:
		items := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				items = append(items, s)
			}
		}
		return items
	case []string:
		return v
	default:
		return nil
	}
}

func extractInterfaceSlice(value any) []any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []any:
		return v
	case []string:
		items := make([]any, len(v))
		for i, s := range v {
			items[i] = s
		}
		return items
	default:
		return nil
	}
}

func extractInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func extractFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

func extractInt16(value any) (int16, error) {
	switch v := value.(type) {
	case float64:
		return int16(v), nil
	case int:
		return int16(v), nil
	case int32:
		return int16(v), nil
	case int64:
		return int16(v), nil
	case json.Number:
		num, err := v.Int64()
		if err != nil {
			return 0, err
		}
		return int16(num), nil
	}
	return 0, fmt.Errorf("unsupported id type %T", value)
}

func extractTime(value any) time.Time {
	if str, ok := value.(string); ok {
		if ts, err := time.Parse(time.RFC3339, str); err == nil {
			return ts
		}
	}
	return time.Time{}
}

func buildRequiredSet(required []string) map[string]struct{} {
	if len(required) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(required))
	for _, key := range required {
		if key != "" {
			set[key] = struct{}{}
		}
	}
	return set
}

func hasKey(set map[string]struct{}, key string) bool {
	if set == nil {
		return false
	}
	_, ok := set[key]
	return ok
}

func generateStableSchemaID(path string) int16 {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	hash := fnv32a(strings.ToLower(base))
	return int16(hash%32000 + 1)
}

func fnv32a(text string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	hash := uint32(offset32)
	for i := 0; i < len(text); i++ {
		hash ^= uint32(text[i])
		hash *= prime32
	}
	return hash
}
