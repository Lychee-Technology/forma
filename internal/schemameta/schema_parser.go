package schemameta

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

	indexes, err := parseIndexDeclarations(schemaName, rawSchema)
	if err != nil {
		return forma.JSONSchema{}, fmt.Errorf("parse %s: %w", IndexDeclarationsKey, err)
	}
	jsonSchema.Indexes = indexes

	uniqueKeys, err := parseUniqueKeyDeclarations(schemaName, rawSchema)
	if err != nil {
		return forma.JSONSchema{}, fmt.Errorf("parse %s: %w", UniqueKeyDeclarationsKey, err)
	}
	jsonSchema.UniqueKeys = uniqueKeys

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
	openItems := false
	if items, ok := prop["items"].(map[string]any); ok {
		schema.Items = parsePropertySchema("", items, defs, nil)
	} else if schema.Type == "" || schema.Type == "array" {
		schema.Items, openItems = parseBranchObjectItems(prop, defs)
	}

	// Parse nested properties (for objects)
	parseNestedProperties(schema, prop, defs)
	if openItems {
		schema.Items.Properties = withMissing(schema.Items.Properties, schema.Properties)
	}

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

// branchKeywords hold the subschemas a value may match instead of, or
// beside, the property's own keywords.
var branchKeywords = []string{"anyOf", "oneOf", "allOf"}

// parseBranchObjectItems answers the elements of the arrays of objects a
// property without items of its own admits through a branch: an object or
// an array of objects ({"anyOf": [{"type": "object"}, {"type": "array",
// "items": {...}}]}), with the properties of every such branch's elements,
// or nil when no branch is one. Then and else count as branches beside an
// if. A write stores such an array's members with one index for it, so the
// read must know the property is an array of objects to place them back
// there (#696 review R3-F2): without, an update or a create of the array
// form read back with the array around each member instead. A branch
// declaring a primitive array is not one; it only matters at a leaf, which
// array ancestry does not look up. Nor is a branch whose "type" is not
// "array", or any branch of a property whose own "type" is neither unset
// nor "array": no array can match it.
//
// open reports a branch whose elements are objects that name no properties:
// they admit any member, so the property's own properties name them too.
func parseBranchObjectItems(prop map[string]any, defs map[string]any) (merged *forma.PropertySchema, open bool) {
	var branches []any
	for _, keyword := range branchKeywords {
		if list, ok := prop[keyword].([]any); ok {
			branches = append(branches, list...)
		}
	}
	if _, ok := prop["if"]; ok {
		branches = append(branches, prop["then"], prop["else"])
	}
	for _, branch := range branches {
		element := branchObjectElement(branch, defs)
		if element == nil {
			continue
		}
		if merged == nil {
			merged = &forma.PropertySchema{Type: "object"}
		}
		open = open || element.Properties == nil
		merged.Properties = withMissing(merged.Properties, element.Properties)
	}
	return merged, open
}

// branchObjectElement answers the element of the array of objects branch
// declares, nil when it declares none.
func branchObjectElement(branch any, defs map[string]any) *forma.PropertySchema {
	branchMap, ok := branch.(map[string]any)
	if !ok {
		return nil
	}
	branchMap = resolvePropertyRef(branchMap, defs)
	if t, ok := branchMap["type"].(string); ok && t != "array" {
		return nil
	}
	items, ok := branchMap["items"].(map[string]any)
	if !ok {
		return nil
	}
	element := parsePropertySchema("", items, defs, nil)
	if element.Type != "object" && element.Properties == nil {
		return nil
	}
	return element
}

// withMissing answers into with every property of from it lacks added.
func withMissing(into, from map[string]*forma.PropertySchema) map[string]*forma.PropertySchema {
	for name, member := range from {
		if into == nil {
			into = make(map[string]*forma.PropertySchema)
		}
		if _, seen := into[name]; !seen {
			into[name] = member
		}
	}
	return into
}
