package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
)

type attributeSpec struct {
	AttributeID int    `json:"attributeID"`
	ValueType   string `json:"valueType"`
}

func runGenerateAttributes(args []string) error {
	flags := flag.NewFlagSet("generate-attributes", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		zap.S().Info("Usage: forma-tools generate-attributes [options]")
		zap.S().Info("")
		zap.S().Info("Options:")
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

	zap.S().Infow("Generated attributes", "schemaPath", resolvedSchemaPath, "outputPath", resolvedOutputPath)
	return nil
}

func generateAttributesJSON(schemaPath, outputPath string) error {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema file: %w", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("parse schema JSON: %w", err)
	}

	// Extract attributes from the schema
	newAttributes := traverseSchema(schema, "", false, make(map[string]attributeSpec))

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
		}
	}

	if err := writeAttributesMap(outputPath, result); err != nil {
		return err
	}

	zap.S().Infow("Generated attributes", "total", len(result), "new", len(newAttrNames), "maxID", maxID+len(newAttrNames))
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

func traverseSchema(schema map[string]any, path string, insideArray bool, attributes map[string]attributeSpec) map[string]attributeSpec {
	if properties, ok := schema["properties"].(map[string]any); ok {
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
			traverseSchema(child, newPath, insideArray, attributes)
		}
		return attributes
	}

	switch schemaType := getSchemaType(schema); schemaType {
	case "array":
		if items, ok := schema["items"].(map[string]any); ok {
			switch getSchemaType(items) {
			case "object":
				if _, ok := items["properties"]; ok {
					return traverseSchema(items, path, true, attributes)
				}
			case "string", "integer", "number", "boolean":
				attributes[path] = attributeSpec{
					ValueType: getValueType(items),
				}
				return attributes
			}
		}
	default:
		attributes[path] = attributeSpec{
			ValueType: getValueType(schema),
		}
	}

	return attributes
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

func writeAttributes(path string, attributes map[string]attributeSpec) error {
	encoded, err := json.MarshalIndent(attributes, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal attributes: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write attributes file: %w", err)
	}

	return nil
}
