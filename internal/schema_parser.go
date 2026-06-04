package internal

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma"
)

// parseJSONSchemaFile parses a JSON Schema file and converts it to forma.JSONSchema structure
func parseJSONSchemaFile(data []byte, schemaID int16, schemaName string) (forma.JSONSchema, error) {
	var rawSchema map[string]any
	if err := json.Unmarshal(data, &rawSchema); err != nil {
		return forma.JSONSchema{}, fmt.Errorf("failed to unmarshal JSON schema: %w", err)
	}

	jsonSchema := forma.JSONSchema{
		ID:         schemaID,
		Name:       schemaName,
		Schema:     string(data),
		Properties: make(map[string]*forma.PropertySchema),
	}

	// Parse required fields
	jsonSchema.Required = parseRequiredFields(rawSchema)

	// Parse properties
	if properties, ok := rawSchema["properties"].(map[string]any); ok {
		defs := extractDefinitions(rawSchema)
		for propName, propValue := range properties {
			if propMap, ok := propValue.(map[string]any); ok {
				propSchema := parsePropertySchema(propName, propMap, defs, jsonSchema.Required)
				jsonSchema.Properties[propName] = propSchema
			}
		}
	}

	return jsonSchema, nil
}

// parseRequiredFields extracts the required field list from a raw schema
func parseRequiredFields(rawSchema map[string]any) []string {
	var result []string
	if required, ok := rawSchema["required"].([]any); ok {
		for _, r := range required {
			if s, ok := r.(string); ok {
				result = append(result, s)
			}
		}
	}
	return result
}

// extractDefinitions extracts $defs from a raw schema
func extractDefinitions(rawSchema map[string]any) map[string]any {
	if d, ok := rawSchema["$defs"].(map[string]any); ok {
		return d
	}
	return make(map[string]any)
}

// parsePropertySchema parses a single property from JSON Schema
func parsePropertySchema(name string, prop map[string]any, defs map[string]any, requiredFields []string) *forma.PropertySchema {
	// Handle $ref
	prop = resolvePropertyRef(prop, defs)

	schema := &forma.PropertySchema{
		Name:     name,
		Required: isFieldRequired(name, requiredFields),
	}

	// Parse basic fields
	parseBasicPropertyFields(schema, prop)

	// Parse constraints
	parsePropertyConstraints(schema, prop)

	// Parse extensions
	parsePropertyExtensions(schema, prop)

	// Parse items (for arrays)
	if items, ok := prop["items"].(map[string]any); ok {
		schema.Items = parsePropertySchema("", items, defs, nil)
	}

	// Parse nested properties (for objects)
	parseNestedProperties(schema, prop, defs)

	return schema
}

// resolvePropertyRef resolves $ref if present
func resolvePropertyRef(prop map[string]any, defs map[string]any) map[string]any {
	if ref, ok := prop["$ref"].(string); ok {
		if resolved := resolveRef(ref, defs); resolved != nil {
			return resolved
		}
	}
	return prop
}

// isFieldRequired checks if a field is in the required list
func isFieldRequired(name string, requiredFields []string) bool {
	return slices.Contains(requiredFields, name)
}

// parseBasicPropertyFields extracts type, format, and pattern
func parseBasicPropertyFields(schema *forma.PropertySchema, prop map[string]any) {
	if t, ok := prop["type"].(string); ok {
		schema.Type = t
	}
	if f, ok := prop["format"].(string); ok {
		schema.Format = f
	}
	if p, ok := prop["pattern"].(string); ok {
		schema.Pattern = p
	}
	if e, ok := prop["enum"].([]any); ok {
		schema.Enum = e
	}
	if d, exists := prop["default"]; exists {
		schema.Default = d
	}
}

// parsePropertyConstraints extracts min/max constraints
func parsePropertyConstraints(schema *forma.PropertySchema, prop map[string]any) {
	if min, ok := prop["minimum"].(float64); ok {
		schema.Minimum = &min
	}
	if max, ok := prop["maximum"].(float64); ok {
		schema.Maximum = &max
	}
	if minLen, ok := prop["minLength"].(float64); ok {
		v := int(minLen)
		schema.MinLength = &v
	}
	if maxLen, ok := prop["maxLength"].(float64); ok {
		v := int(maxLen)
		schema.MaxLength = &v
	}
}

// parsePropertyExtensions extracts x-* extension fields
func parsePropertyExtensions(schema *forma.PropertySchema, prop map[string]any) {
	// Parse x-relation
	if relation, ok := prop["x-relation"].(map[string]any); ok {
		schema.Relation = &forma.RelationSchema{}
		if target, ok := relation["target"].(string); ok {
			schema.Relation.Target = target
		}
		if relType, ok := relation["type"].(string); ok {
			schema.Relation.Type = relType
		}
		if keyProp, ok := relation["key_property"].(string); ok {
			schema.Relation.KeyProperty = keyProp
		}
	}

	if ltBaseType, ok := prop["x-ltbase-type"].(string); ok {
		schema.LTBaseType = ltBaseType
	}

	if ltBaseNote, ok := prop["x-ltbase-note-prop"].(string); ok {
		schema.LTBaseNote = ltBaseNote
	}
}

// parseNestedProperties extracts nested properties for object types
func parseNestedProperties(schema *forma.PropertySchema, prop map[string]any, defs map[string]any) {
	nestedProps, ok := prop["properties"].(map[string]any)
	if !ok {
		return
	}

	schema.Properties = make(map[string]*forma.PropertySchema)
	nestedRequired := extractNestedRequired(prop)

	for nestedName, nestedValue := range nestedProps {
		if nestedMap, ok := nestedValue.(map[string]any); ok {
			schema.Properties[nestedName] = parsePropertySchema(nestedName, nestedMap, defs, nestedRequired)
		}
	}
}

// extractNestedRequired extracts required fields from a nested object
func extractNestedRequired(prop map[string]any) []string {
	var nestedRequired []string
	if nr, ok := prop["required"].([]any); ok {
		for _, r := range nr {
			if s, ok := r.(string); ok {
				nestedRequired = append(nestedRequired, s)
			}
		}
	}
	return nestedRequired
}

// resolveRef resolves a JSON Schema $ref reference
func resolveRef(ref string, defs map[string]any) map[string]any {
	// Handle "#/$defs/xxx" format
	if len(ref) > 8 && ref[:8] == "#/$defs/" {
		defName := ref[8:]
		if def, ok := defs[defName].(map[string]any); ok {
			return def
		}
	}
	return nil
}

// parseFileAttributeMetadata converts raw JSON metadata into AttributeMetadata structs
func parseFileAttributeMetadata(attrName string, attrData map[string]any, source string) (forma.AttributeMetadata, error) {
	meta := forma.AttributeMetadata{AttributeName: attrName}

	// Parse attributeID
	attrID, err := parseAttributeID(attrData["attributeID"], attrName, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}
	meta.AttributeID = attrID

	// Parse valueType
	valueTypeStr, ok := attrData["valueType"].(string)
	if !ok {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing valueType for attribute %s in %s", attrName, source)
	}
	meta.ValueType = forma.ValueType(valueTypeStr)

	requiredPolicy, _, err := parseRequiredPolicy(attrName, attrData, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}
	meta.RequiredPolicy = requiredPolicy
	policy := meta.EffectiveRequiredPolicy()
	meta.Required = policy == forma.RequiredPolicyAlways || policy == forma.RequiredPolicyIfParentPresent

	// Parse optional column_binding
	if bindingRaw, exists := attrData["column_binding"]; exists {
		binding, err := parseFileColumnBinding(bindingRaw, attrName, source)
		if err != nil {
			return forma.AttributeMetadata{}, err
		}
		meta.ColumnBinding = binding
	}

	return meta, nil
}

// parseFileColumnBinding parses column binding configuration
func parseFileColumnBinding(raw any, attrName, source string) (*forma.MainColumnBinding, error) {
	bindingMap, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid column_binding format for attribute %s in %s", attrName, source)
	}

	colNameStr, ok := bindingMap["col_name"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid or missing col_name in column_binding for attribute %s in %s", attrName, source)
	}

	binding := &forma.MainColumnBinding{
		ColumnName: forma.MainColumn(colNameStr),
	}

	if encodingStr, ok := bindingMap["encoding"].(string); ok {
		binding.Encoding = forma.MainColumnEncoding(encodingStr)
	}

	return binding, nil
}
