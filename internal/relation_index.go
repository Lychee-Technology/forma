package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lychee-technology/forma/internal/transform"
	"go.uber.org/zap"
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

// ValidateRelationSchemas fails closed on anything that stops the relation index
// from loading, for callers that build the manager themselves.
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
//
// What aborts startup, exactly:
//
//   - a schemaDir that cannot be listed, which is a misconfiguration of the
//     server rather than a fault in one file;
//   - a schema that declares a relation root the validator would demand, or
//     whose root requirements cannot be determined (see
//     checkRelationRootsWritable).
//
// A .json file that does not parse as a JSON object does not. It is skipped with
// a warning, because both call sites run schemavalidate.New first — over every
// name registry.ListSchemas returns, failing closed on any document that will
// not parse — so a document this loader cannot parse is not a registered entity
// schema, and refusing to boot over it would abort startup for a file nothing
// reads. An empty schemaDir stays a no-op, not an error.
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
		// Skipped, not fatal — see ValidateRelationSchemas for why a document this
		// loader cannot parse is never a registered entity schema. Still logged:
		// an unreadable file in SCHEMA_DIR is worth an operator's attention.
		zap.S().Warnw("skipping unparseable file in schema directory",
			"path", filePath, "error", err)
		return nil
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil
	}

	relations := collectRelations(schemaName, props, declaredRootRequired(schema))
	// Guarded here, after collection, rather than at the x-relation marker: only a
	// property that reached the relations slice is one StripComputedFields
	// removes. A property whose $ref, key_property, or foreign-key attribute did
	// not resolve is never stripped, so requiring it is harmless and must not
	// abort startup (#318).
	if len(relations) == 0 {
		return nil
	}
	if err := checkRelationRootsWritable(schemaName, schema, relations); err != nil {
		return fmt.Errorf("unhonourable relation declaration in %s: %w", filePath, err)
	}

	idx.bySchema[schemaName] = append(idx.bySchema[schemaName], relations...)
	return nil
}

// collectRelations builds a descriptor for every property of props carrying a
// usable x-relation marker, skipping the ones that do not resolve.
//
// declaredRequired is the root object's literal "required" set and feeds only
// RelationDescriptor.ForeignKeyRequired; the guard's own, wider notion of
// "required" lives in relation_required.go and deliberately differs.
func collectRelations(
	schemaName string, props map[string]any, declaredRequired map[string]struct{},
) []RelationDescriptor {
	var relations []RelationDescriptor
	for childProp, rawProp := range props {
		propMap, ok := rawProp.(map[string]any)
		if !ok {
			continue
		}
		rel, ok := relationDescriptor(schemaName, childProp, propMap, declaredRequired)
		if !ok {
			continue
		}
		relations = append(relations, rel)
	}
	return relations
}

// relationDescriptor derives one property's descriptor, answering false when the
// property is not a usable relation root. Every early return is a property
// StripComputedFields will not strip, which is why none of them is an error.
func relationDescriptor(
	schemaName, childProp string, propMap map[string]any, declaredRequired map[string]struct{},
) (RelationDescriptor, bool) {
	refStr, _ := propMap["$ref"].(string)
	if refStr == "" || !strings.Contains(refStr, ".json") {
		return RelationDescriptor{}, false
	}

	var fkPointer string
	if relMap, ok := propMap["x-relation"].(map[string]any); ok {
		fkPointer = firstNonEmpty(
			stringValue(relMap["key_property"]),
		)
	}
	if fkPointer == "" {
		return RelationDescriptor{}, false
	}

	parentSchema, parentPath := parseRef(refStr)
	if parentSchema == "" {
		return RelationDescriptor{}, false
	}

	fkAttr := pointerToAttrName(fkPointer)
	if fkAttr == "" {
		return RelationDescriptor{}, false
	}

	parentIDAttr := "id"
	if tail := pointerToAttrName(extractFragment(refStr)); tail == "id" {
		parentIDAttr = tail
	}

	_, fkRequired := declaredRequired[fkAttr]

	return RelationDescriptor{
		ChildSchema:        schemaName,
		ChildPath:          childProp,
		ParentSchema:       parentSchema,
		ParentPath:         parentPath,
		ForeignKeyAttr:     fkAttr,
		ParentIDAttr:       parentIDAttr,
		ForeignKeyRequired: fkRequired,
	}, true
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

// RelationRoots returns schema's relation root names, as the set
// transform.AttributeConverter.FromEAVRecords consults to skip required-policy
// enforcement beneath a relation root (#315). That check is not read-only —
// transform.ToAttributes reaches it on every create and update — so this is not
// read-path-only state.
//
// It answers the roots alone, which is less than StripComputedFields removes:
// the strip also takes every dotted descendant of each root, by the prefix rule
// in coversRelationSubtree. Do not read this set as the strip's reach. A caller
// that needs the reach has to apply that prefix rule itself —
// transform.RelationRoots.Covers applies it for names strictly beneath a root,
// and deliberately excludes a name that is itself a root.
//
// A nil receiver or a schema with no relations answers nil, which that set reads
// as "no relation roots".
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
// transform.RelationRoots.Covers applies for #315's required-policy carve-out.
// The two differ on the bare root, and deliberately:
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
