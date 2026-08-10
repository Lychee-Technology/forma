package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lychee-technology/forma/internal/transform"
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

// ValidateRelationSchemas fails closed on relation declarations the write path
// cannot honour, for callers that build the manager themselves.
//
// It exists because NewEntityManager cannot enforce this: it swallows a
// LoadRelationIndex failure into a warning and continues with a nil index, which
// disables stripping altogether rather than stopping. The composition root calls
// this before it builds the manager, at a position where returning an error
// aborts startup (#318).
//
// That swallow predates the required-relation-root guard, but its blast radius
// does not: one offending schema now fails the whole directory load, so an
// unguarded caller loses stripping for every schema, not just the offender. Any
// new construction site must call this too.
func ValidateRelationSchemas(schemaDir string) error {
	if _, err := LoadRelationIndex(schemaDir); err != nil {
		return fmt.Errorf("validate schema relations in %q: %w", schemaDir, err)
	}
	return nil
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

	// Rejected here, after the loop, rather than at the x-relation marker: only a
	// property that reached the relations slice is one StripComputedFields
	// removes. A property whose $ref, key_property, or foreign-key attribute did
	// not resolve is never stripped, so requiring it is harmless and must not
	// abort startup (#318).
	for _, rel := range relations {
		if _, isRequired := requiredSet[rel.ChildPath]; isRequired {
			return fmt.Errorf(
				"schema %s lists relation root %q in \"required\": a relation root is stripped from every payload before validation, so every create fails with a missing-required error the caller cannot fix, as does every update when strict update validation is on; remove it from \"required\" or drop its x-relation marker",
				schemaName, rel.ChildPath)
		}
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

// StripComputedFields removes the relation subtree from the payload before it is
// validated and persisted: the property carrying x-relation and everything
// beneath it, in either spelling.
//
// The subtree is derived on read from the parent entity
// (entityRelationService.enrichDataRecords), which replaces it wholesale, so a
// caller-written value there is unreadable wherever enrichment applies.
// Enrichment does skip a record whose foreign key is missing or empty, whose
// parent row is not found, or whose parent fragment is nil, and a persisted
// value would survive those reads — but a caller cannot rely on that, and the
// next update deletes the value anyway. Dropping is silent — see
// TestCreateDropsDottedKeyBeneathRelationRoot.
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
		if idx.coversRelationSubtree(schema, k) {
			continue
		}
		result[k] = v
	}
	return result
}

// RelationRoots returns the property names StripComputedFields removes for
// schema, as the set transform.NormalizeDottedKeys consults so it does not
// rebuild a stripped root out of a surviving dotted descendant. A nil receiver
// or a schema with no relations answers nil, which that set reads as "no
// relation roots".
func (idx *RelationIndex) RelationRoots(schema string) transform.RelationRoots {
	if idx == nil {
		return nil
	}
	rels := idx.bySchema[schema]
	if len(rels) == 0 {
		return nil
	}
	roots := make(transform.RelationRoots, len(rels))
	for _, rel := range rels {
		roots[rel.ChildPath] = struct{}{}
	}
	return roots
}

// coversRelationSubtree reports whether key names a relation root or anything
// beneath it.
//
// The "." in root+"." is load-bearing. A bare HasPrefix would also delete a
// sibling attribute whose name merely starts with the root's — contactSnapshotX
// alongside contactSnapshot — silently dropping an ordinary caller attribute.
// Pinned by TestStripKeepsSamePrefixSibling.
//
// For dotted descendants this is the same prefix rule
// transform.RelationRoots.Covers applies on the read path for #315's
// required-policy carve-out. The two differ on the bare root, and deliberately:
// Covers excludes a name that *is* a root, while this predicate must match it —
// the root is the nested spelling the strip has always removed. #318 was this
// predicate lagging behind that prefix rule by an exact-key match, which let the
// dotted spelling persist what the nested spelling discarded.
func (idx *RelationIndex) coversRelationSubtree(schema, key string) bool {
	for _, rel := range idx.bySchema[schema] {
		if key == rel.ChildPath || strings.HasPrefix(key, rel.ChildPath+".") {
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
	for i := range parts {
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
