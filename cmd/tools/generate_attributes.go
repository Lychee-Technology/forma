package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type attributeSpec struct {
	AttributeID    int    `json:"attributeID"`
	ValueType      string `json:"valueType"`
	ItemsType      string `json:"items_type,omitempty"`
	RequiredPolicy string `json:"required_policy,omitempty"`
}

const (
	requiredPolicyOptional        = "optional"
	requiredPolicyAlways          = "required_always"
	requiredPolicyIfParentPresent = "required_if_parent_present"
)

func runGenerateAttributes(_ context.Context, args []string) error {
	flags := flag.NewFlagSet("generate-attributes", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools generate-attributes [options]")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	schemaDir := flags.String("schema-dir", "cmd/server/schemas", "Directory containing JSON schema files")
	schemaName := flags.String("schema", "", "Schema name without extension (mutually exclusive with -schema-file)")
	schemaFile := flags.String("schema-file", "", "Path to the JSON schema file (overrides -schema and -schema-dir)")
	outputFile := flags.String("out", "", "Path to write the generated attributes JSON (defaults next to schema file)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	resolvedSchemaPath := *schemaFile
	if resolvedSchemaPath == "" {
		if *schemaName == "" {
			return fmt.Errorf("either -schema or -schema-file must be provided")
		}
		resolvedSchemaPath = filepath.Join(*schemaDir, *schemaName+".json")
	}

	resolvedOutputPath := *outputFile
	if resolvedOutputPath == "" {
		base := strings.TrimSuffix(filepath.Base(resolvedSchemaPath), filepath.Ext(resolvedSchemaPath))
		resolvedOutputPath = filepath.Join(filepath.Dir(resolvedSchemaPath), base+"_attributes.json")
	}

	if err := generateAttributesJSON(resolvedSchemaPath, resolvedOutputPath); err != nil {
		return err
	}

	fmt.Printf("Generated attributes, schemaPath: %s, outputPath: %s\n", resolvedSchemaPath, resolvedOutputPath)
	return nil
}

func generateAttributesJSON(schemaPath, outputPath string) error {
	inliner := NewSchemaInliner(filepath.Dir(schemaPath))
	schema, err := inliner.InlineFile(schemaPath)
	if err != nil {
		// Adapt inliner error messages to match legacy test expectations
		errMsg := err.Error()
		if strings.Contains(errMsg, "read file") {
			return fmt.Errorf("read schema file: %w", err)
		}
		if strings.Contains(errMsg, "parse JSON") {
			return fmt.Errorf("parse schema JSON: %w", err)
		}
		return fmt.Errorf("inline schema: %w", err)
	}

	// Extract attributes from the schema
	newAttributes := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	// Load existing attributes file if it exists
	existingAttrs, err := loadExistingAttributes(outputPath)
	if err != nil {
		return fmt.Errorf("load existing attributes: %w", err)
	}

	// Find the maximum existing attributeID
	maxID := 0
	for _, attrData := range existingAttrs {
		if id, ok := attrData["attributeID"].(float64); ok {
			if int(id) > maxID {
				maxID = int(id)
			}
		}
	}

	// Merge attributes: preserve existing IDs, assign new IDs for new attributes
	result := make(map[string]map[string]any, len(newAttributes))

	// Collect new attribute names that need new IDs
	var newAttrNames []string
	for name := range newAttributes {
		if _, exists := existingAttrs[name]; !exists {
			newAttrNames = append(newAttrNames, name)
		}
	}
	// Sort new attribute names for deterministic ID assignment
	sort.Strings(newAttrNames)

	// Create a map for quick lookup of new ID assignments
	newIDMap := make(map[string]int)
	for i, name := range newAttrNames {
		newIDMap[name] = maxID + i + 1
	}

	// Build the result
	// First, preserve ALL existing attributes (even if they are removed from schema)
	for name, existingData := range existingAttrs {
		if spec, exists := newAttributes[name]; exists {
			// Attribute still exists in schema: update valueType if changed
			existingData["valueType"] = spec.ValueType
			if spec.ItemsType != "" {
				existingData["items_type"] = spec.ItemsType
			} else {
				delete(existingData, "items_type")
			}
			applyRequiredPolicy(existingData, spec.RequiredPolicy)
		} else {
			// Attribute no longer exists in schema path; it must not stay required.
			applyRequiredPolicy(existingData, requiredPolicyOptional)
		}
		// Keep the attribute regardless of whether it exists in the new schema
		result[name] = existingData
	}

	// Then, add new attributes from the schema
	for name, spec := range newAttributes {
		if _, exists := existingAttrs[name]; !exists {
			// New attribute: assign new ID
			result[name] = map[string]any{
				"attributeID": newIDMap[name],
				"valueType":   spec.ValueType,
			}
			if spec.ItemsType != "" {
				result[name]["items_type"] = spec.ItemsType
			}
			applyRequiredPolicy(result[name], spec.RequiredPolicy)
		}
	}

	if err := writeAttributesMap(outputPath, result); err != nil {
		return err
	}

	fmt.Printf("Generated attributes total: %d, new: %d, maxID: %d\n", len(result), len(newAttrNames), maxID+len(newAttrNames))
	return nil
}

// loadExistingAttributes reads an existing attributes file and returns its contents.
// Returns nil if the file does not exist.
func loadExistingAttributes(path string) (map[string]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]map[string]any), nil
		}
		return nil, fmt.Errorf("read existing attributes file: %w", err)
	}

	var attrs map[string]map[string]any
	if err := json.Unmarshal(data, &attrs); err != nil {
		return nil, fmt.Errorf("parse existing attributes JSON: %w", err)
	}

	return attrs, nil
}

// writeAttributesMap writes the attributes map to a JSON file with sorted keys
func writeAttributesMap(path string, attributes map[string]map[string]any) error {
	// Sort keys for consistent output
	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build ordered output using json.RawMessage to preserve key order
	var buf strings.Builder
	buf.WriteString("{\n")
	for i, key := range keys {
		attrData := attributes[key]
		// Ensure attributeID comes first, then valueType, then other fields
		buf.WriteString(fmt.Sprintf("  %q: {\n", key))

		// Write attributeID first
		if id, ok := attrData["attributeID"]; ok {
			buf.WriteString(fmt.Sprintf("    \"attributeID\": %v", formatJSONValue(id)))
		}

		// Write valueType second
		if vt, ok := attrData["valueType"]; ok {
			buf.WriteString(fmt.Sprintf(",\n    \"valueType\": %q", vt))
		}

		// Write other fields in sorted order
		var otherKeys []string
		for k := range attrData {
			if k != "attributeID" && k != "valueType" {
				otherKeys = append(otherKeys, k)
			}
		}
		sort.Strings(otherKeys)

		for _, k := range otherKeys {
			v := attrData[k]
			encoded, _ := json.Marshal(v)
			buf.WriteString(fmt.Sprintf(",\n    %q: %s", k, string(encoded)))
		}

		buf.WriteString("\n  }")
		if i < len(keys)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString("}")

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	if err := os.WriteFile(path, []byte(buf.String()), 0o644); err != nil {
		return fmt.Errorf("write attributes file: %w", err)
	}

	return nil
}

func formatJSONValue(v any) string {
	switch val := v.(type) {
	case float64:
		// Check if it's a whole number
		if val == float64(int(val)) {
			return fmt.Sprintf("%d", int(val))
		}
		return fmt.Sprintf("%v", val)
	case int:
		return fmt.Sprintf("%d", val)
	default:
		encoded, _ := json.Marshal(v)
		return string(encoded)
	}
}

func traverseSchema(schema map[string]any, path string, insideArray bool, attributes map[string]attributeSpec, pathRequired bool) map[string]attributeSpec {
	return traverseSchemaWithRequiredState(schema, path, insideArray, attributes, pathRequired, true)
}

func traverseSchemaWithRequiredState(
	schema map[string]any,
	path string,
	insideArray bool,
	attributes map[string]attributeSpec,
	pathRequired bool,
	currentRequired bool,
) map[string]attributeSpec {
	if properties, ok := schema["properties"].(map[string]any); ok {
		requiredProps := getRequiredProperties(schema)
		for key, raw := range properties {
			child, ok := raw.(map[string]any)
			if !ok {
				continue
			}

			var newPath string
			if path == "" {
				newPath = key
			} else {
				newPath = path + "." + key
			}
			childRequired := requiredProps[key]
			childPathRequired := pathRequired && childRequired
			traverseSchemaWithRequiredState(child, newPath, insideArray, attributes, childPathRequired, childRequired)
		}
		return attributes
	}

	switch schemaType := getSchemaType(schema); schemaType {
	case "array":
		if items, ok := schema["items"].(map[string]any); ok {
			switch getSchemaType(items) {
			case "object":
				if _, ok := items["properties"]; ok {
					// Arrays can be empty; item fields are only required when items exist.
					return traverseSchemaWithRequiredState(items, path, true, attributes, false, true)
				}
			case "string", "integer", "number", "boolean":
				// #204: primitive arrays are list containers; the element type
				// travels in items_type so the CDC export can aggregate one
				// row per element instead of collapsing to a scalar.
				attributes[path] = attributeSpec{
					ValueType:      "list",
					ItemsType:      getValueType(items),
					RequiredPolicy: resolveRequiredPolicy(pathRequired, currentRequired),
				}
				return attributes
			}
		}
	default:
		attributes[path] = attributeSpec{
			ValueType:      getValueType(schema),
			RequiredPolicy: resolveRequiredPolicy(pathRequired, currentRequired),
		}
	}

	return attributes
}

func getRequiredProperties(schema map[string]any) map[string]bool {
	raw, ok := schema["required"]
	if !ok {
		return nil
	}

	result := make(map[string]bool)

	switch vals := raw.(type) {
	case []any:
		for _, v := range vals {
			if key, ok := v.(string); ok {
				result[key] = true
			}
		}
	case []string:
		for _, key := range vals {
			result[key] = true
		}
	}

	return result
}

func applyRequiredPolicy(attrData map[string]any, policy string) {
	// Keep output in the new format.
	delete(attrData, "required")

	switch policy {
	case "", requiredPolicyOptional:
		delete(attrData, "required_policy")
	default:
		attrData["required_policy"] = policy
	}
}

func resolveRequiredPolicy(pathRequired bool, currentRequired bool) string {
	if !currentRequired {
		return requiredPolicyOptional
	}
	if pathRequired {
		return requiredPolicyAlways
	}
	return requiredPolicyIfParentPresent
}

func getSchemaType(node map[string]any) string {
	switch t := node["type"].(type) {
	case string:
		return t
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func getValueType(schema map[string]any) string {
	schemaType := getSchemaType(schema)
	formatType, _ := schema["format"].(string)

	switch schemaType {
	case "string":
		if formatType == "date" || formatType == "date-time" {
			return "date"
		}
		return "text"
	case "integer", "number":
		return "numeric"
	case "boolean":
		return "bool"
	default:
		return "text"
	}
}
