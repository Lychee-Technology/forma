package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RelationDescriptor captures how a child schema derives fields from a parent schema.
type RelationDescriptor struct {
	ChildSchema        string
	ChildPath          string
	ParentSchema       string
	ParentPath         string
	ForeignKeyAttr     string
	ParentIDAttr       string
	ForeignKeyRequired bool
}

// RelationIndex stores parent-child relations keyed by child schema name.
type RelationIndex struct {
	bySchema map[string][]RelationDescriptor
}

// LoadRelationIndex parses JSON schema files in schemaDir and builds a relation index.
// If the directory is missing or no relations are found, it returns an empty index.
func LoadRelationIndex(schemaDir string) (*RelationIndex, error) {
	idx := &RelationIndex{bySchema: make(map[string][]RelationDescriptor)}
	if schemaDir == "" {
		return idx, nil
	}

	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return idx, fmt.Errorf("read schema dir: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, "_attributes.json") {
			continue
		}
		schemaName := strings.TrimSuffix(name, ".json")
		if err := idx.loadSchemaRelations(schemaDir, schemaName); err != nil {
			return nil, err
		}
	}

	return idx, nil
}

func (idx *RelationIndex) loadSchemaRelations(schemaDir, schemaName string) error {
	filePath := filepath.Join(schemaDir, schemaName+".json")
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read schema %s: %w", filePath, err)
	}

	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return fmt.Errorf("parse schema %s: %w", filePath, err)
	}

	requiredSet := make(map[string]struct{})
	if reqRaw, ok := schema["required"].([]any); ok {
		for _, r := range reqRaw {
			if s, ok := r.(string); ok {
				requiredSet[s] = struct{}{}
			}
		}
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil
	}

	var relations []RelationDescriptor
	for childProp, rawProp := range props {
		propMap, ok := rawProp.(map[string]any)
		if !ok {
			continue
		}

		refStr, _ := propMap["$ref"].(string)
		if refStr == "" || !strings.Contains(refStr, ".json") {
			continue
		}

		var fkPointer string
		if relMap, ok := propMap["x-relation"].(map[string]any); ok {
			fkPointer = firstNonEmpty(
				stringValue(relMap["key_property"]),
			)
		}
		// Backward compatibility: support legacy marker if present.
		if fkPointer == "" {
			continue
		}

		parentSchema, parentPath := parseRef(refStr)
		if parentSchema == "" {
			continue
		}

		fkAttr := pointerToAttrName(fkPointer)
		if fkAttr == "" {
			continue
		}

		parentIDAttr := "id"
		if tail := pointerToAttrName(extractFragment(refStr)); tail == "id" {
			parentIDAttr = tail
		}

		_, fkRequired := requiredSet[fkAttr]

		relations = append(relations, RelationDescriptor{
			ChildSchema:        schemaName,
			ChildPath:          childProp,
			ParentSchema:       parentSchema,
			ParentPath:         parentPath,
			ForeignKeyAttr:     fkAttr,
			ParentIDAttr:       parentIDAttr,
			ForeignKeyRequired: fkRequired,
		})
	}

	if len(relations) > 0 {
		idx.bySchema[schemaName] = append(idx.bySchema[schemaName], relations...)
	}
	return nil
}

// Relations returns descriptors for a child schema.
func (idx *RelationIndex) Relations(schema string) []RelationDescriptor {
	if idx == nil {
		return nil
	}
	return idx.bySchema[schema]
}

// StripComputedFields removes relation-backed attributes from the payload before persistence.
func (idx *RelationIndex) StripComputedFields(schema string, data map[string]any) map[string]any {
	if idx == nil || len(idx.bySchema) == 0 || data == nil {
		return data
	}
	rels := idx.bySchema[schema]
	if len(rels) == 0 {
		return data
	}

	result := make(map[string]any, len(data))
	for k, v := range data {
		if idx.isRelationRoot(schema, k) {
			continue
		}
		result[k] = v
	}
	return result
}

func (idx *RelationIndex) isRelationRoot(schema, key string) bool {
	for _, rel := range idx.bySchema[schema] {
		if rel.ChildPath == key {
			return true
		}
	}
	return false
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if m, ok := v.(map[string]any); ok {
		if ref, ok := m["$ref"].(string); ok {
			return ref
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseRef(refStr string) (string, string) {
	parts := strings.Split(refStr, "#")
	base := parts[0]
	parentSchema := strings.TrimSuffix(filepath.Base(base), filepath.Ext(base))
	parentPath := ""
	if len(parts) > 1 {
		parentPath = pointerToAttrName("#" + parts[1])
	}
	return parentSchema, parentPath
}

func extractFragment(refStr string) string {
	idx := strings.Index(refStr, "#")
	if idx == -1 {
		return ""
	}
	return refStr[idx:]
}

// pointerToAttrName converts a JSON pointer to a dot-separated attribute path.
// Examples:
//
//	"#/properties/leadId" -> "leadId"
//	"#/$defs/contact" -> "contact"
func pointerToAttrName(ptr string) string {
	if ptr == "" {
		return ""
	}
	ptr = strings.TrimPrefix(ptr, "#/")
	parts := strings.Split(ptr, "/")
	filtered := make([]string, 0, len(parts))
	for i := 0; i < len(parts); i++ {
		p := parts[i]
		if p == "properties" || p == "$defs" {
			continue
		}
		if p != "" {
			filtered = append(filtered, p)
		}
	}
	return strings.Join(filtered, ".")
}
