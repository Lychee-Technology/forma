package internal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
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

// LoadRelationIndex builds a relation index from the schema documents the
// registry serves, one per name in registry.ListSchemas(), and fails closed on a
// relation declaration the runtime cannot honour.
//
// Building and validating are one call because they are one walk: the guard's
// question — does this schema require a property the strip removes? — can only be
// asked of the descriptors the walk has just built. There is no separate
// validate-only entry point, and a caller that wants the check runs this and
// keeps the index (see WithRelationIndex for why keeping it matters).
//
// The registry, not SCHEMA_DIR, is the source, and that is the point: the
// runtime validator is built from the same pair of accessors
// (schemavalidate.New), so neither is reading a document the other cannot see.
// forma.SchemaRegistry is a public extension point whose implementations may
// load from files, a database, or anything else, and only what it registers is
// ever validated or written to.
//
// Same accessors is not the same as same bytes, and the difference is why the
// factory builds this index exactly once and hands it to NewEntityManager
// (WithRelationIndex). A registry is free to answer differently on a later call
// — it may be reading a database — so two independent loads can disagree, and
// the losing one would be the manager's, silently disabling stripping after a
// successful preflight. What this function guarantees is that one *load* sees a
// consistent view; keeping the validator and the manager on that same load is
// the caller's job.
//
// NewEntityManager cannot enforce any of this itself: it swallows a failure here
// into a warning and continues with a nil index, which disables stripping
// altogether rather than stopping (#318, #388). That swallow predates the
// required-relation-root guard, but its blast radius does not — one offending
// schema fails the whole registry load, so an unguarded caller loses stripping
// for every schema, not just the offender. Both shipped construction sites (the
// factory and the production e2e harness) therefore call this at a position
// where returning an error aborts startup, and pass the result forward.
//
// What aborts startup, exactly:
//
//   - a schema that declares a relation root the analysed fragment shows is
//     required on every document — the root's own "required", or one in an allOf
//     chain (see collectUnconditionalRootRequired for the fragment and why it is
//     that small);
//   - a schema the registry lists but cannot then serve, or whose document does
//     not decode into any (see loadSchemaRelations for the decode).
//
// Nothing is refused for being unanalysable. Outside the fragment the walk has
// no opinion, and a schema is booted whether or not it composes its root object
// out of "not", "anyOf", "oneOf", "if"/"then"/"else", the dependent* family or a
// root "$ref". Some of those really do demand a stripped relation root, and
// those writes fail at runtime with an operator-facing explanation attached
// (explainStrippedRelationRoots) rather than at startup.
//
// The second cause is fatal rather than skipped because every name walked here
// came from ListSchemas: it names a schema the runtime resolves, validates
// writes against, and strips relation subtrees for. A document that cannot be
// read at all is therefore a fault in the registry itself, not a stray file left
// beside the ones that matter.
//
// The boundary is the decode into any, stated exactly that way because the two
// looser descriptions of it are both wrong. It is not JSON *shape*: a document
// that decodes but is not an object is accepted, since it declares no properties
// and so has no relation roots, and drawing the line there made booleans fatal —
// a boolean is a legal JSON Schema. Nor is it JSON *syntax*, which is narrower
// than what actually fails:
//
//	{"properties":{"a":{"type":"string"},"b":{"const":1e999}}}
//
// is well-formed JSON — json.Valid answers true, and RFC 8259 puts no bound on a
// number's magnitude — yet decoding it into any fails with "cannot unmarshal
// number 1e999 into Go value of type float64". So the fatal class is syntax
// errors *plus* numbers outside float64's range.
//
// Observation, not justification: schemavalidate.New rejects every document this
// cause is fatal on, and both shipped call sites run it first, so in the shipped
// ordering the cause is unreachable. Measured for both halves, and it holds for
// the whole class rather than for the cases someone thought to try:
//
//   - Syntax errors are target-independent. json.Unmarshal runs checkValid over
//     the whole input before decoding anything (encoding/json/decode.go), so a
//     malformed document fails unmarshalling into any target type.
//   - Overflow is caught because jsonschema.Schema decodes the same bytes the
//     same way this loader does. Schema.UnmarshalJSON tries a bool first
//     (schema.go:379, which is why booleans are legal schemas) and otherwise
//     calls unmarshalStructWithMap, which decodes the document twice: into its
//     own struct, and into a map[string]any (util.go:367 and :372). A JSON object
//     therefore passes every value through the same any decode this loader
//     performs, so a value that fails here fails there — in the struct pass for a
//     keyword the library models ("properties", "enum"), in the map pass for one
//     it does not ("x-junk"). A document that is neither an object nor a boolean
//     is rejected outright by the struct pass, which is why "[1,2,3]" and "\"x\""
//     are refused by the validator even though this loader accepts them.
//
// The relationship is one-directional and deliberately so: this loader's fatal
// class is a strict subset of what schemavalidate.New refuses. The reason to
// refuse remains that the schema is registered.
//
// A registry that lists no schemas, and a nil registry, both yield an empty index
// and no error.
func LoadRelationIndex(registry forma.SchemaRegistry) (*RelationIndex, error) {
	idx := &RelationIndex{bySchema: make(map[string][]RelationDescriptor)}
	if registry == nil {
		return idx, nil
	}

	for _, schemaName := range registry.ListSchemas() {
		if err := idx.loadSchemaRelations(registry, schemaName); err != nil {
			return nil, fmt.Errorf("build the relation index from the schema registry: %w", err)
		}
	}

	return idx, nil
}

// loadSchemaRelations indexes one registered schema's relation roots.
//
// forma.JSONSchema.Schema is the registry's raw document text — the shipped file
// registry sets it straight from the file bytes (schemameta/schema_parser.go) —
// so this decodes exactly what schemavalidate.New unmarshals.
//
// The document is decoded into any, not straight into map[string]any, and the
// difference is load-bearing rather than stylistic. A JSON boolean is a legal
// JSON Schema: for "true" and "false", json.Unmarshal into jsonschema.Schema,
// checkSchemaSupported and Resolve all succeed, while json.Unmarshal of the same
// bytes into map[string]any answers "cannot unmarshal bool". Decoding into a map
// and calling the failure fatal therefore aborted startup for a registry
// schemavalidate.New accepts — pinned by
// TestLoadRelationIndexAcceptsNonObjectDocuments.
//
// Any document that decodes but is not an object — true, false, an array, a
// string, null — declares no properties, so it has no relation roots, nothing is
// stripped for it, and there is nothing here to guard. It takes the same exit a
// property-less object takes.
//
// "Decodes", not "is valid JSON": the two differ. "[1e999]" is well-formed JSON
// and not an object, yet it never reaches that exit, because the decode below
// fails on the number first. See LoadRelationIndex for what that costs and
// why it is safe.
//
// # Where a marker is looked for
//
// One place: this document's own top-level "properties" object. The lookup is
// the literal map read below — no applicator is composed and no reference is
// followed — so an x-relation declared inside an "allOf" branch, or in a
// document reached by a root "$ref", is not discovered at all.
//
// Nothing is stripped for such a property and nothing is derived on read: it
// behaves as an ordinary caller-written attribute. That is the feature silently
// not applying, which is a different thing from the unwritable-entity hazard the
// guard below refuses — a schema whose marker was never seen writes and reads
// perfectly well, just not as a relation. Pinned, control included, by
// TestRelationDeclarationsAreFoundOnlyInRootProperties, and written down for
// schema authors in docs/error-handling.md.
func (idx *RelationIndex) loadSchemaRelations(registry forma.SchemaRegistry, schemaName string) error {
	_, registered, err := registry.GetSchemaByName(schemaName)
	if err != nil {
		return fmt.Errorf("read registered schema %s from the schema registry: %w", schemaName, err)
	}

	var doc any
	if err := json.Unmarshal([]byte(registered.Schema), &doc); err != nil {
		return fmt.Errorf("decode the document registered for schema %s: %w", schemaName, err)
	}

	schema, ok := doc.(map[string]any)
	if !ok {
		return nil
	}

	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil
	}

	relations := collectRelations(schemaName, props, readDeclaredRootRequired(schema))
	// Guarded here, after collection, rather than at the x-relation marker: only a
	// property that reached the relations slice is one StripComputedFields
	// removes. A property whose $ref, key_property, or foreign-key attribute did
	// not resolve is never stripped, so requiring it is harmless and must not
	// abort startup (#318).
	if len(relations) == 0 {
		return nil
	}
	if err := checkRelationRootsWritable(schemaName, schema, relations); err != nil {
		return fmt.Errorf("unhonourable relation declaration in registered schema %s: %w", schemaName, err)
	}

	idx.bySchema[schemaName] = append(idx.bySchema[schemaName], relations...)
	return nil
}

// collectRelations builds a descriptor for every property of props carrying a
// usable x-relation marker, skipping the ones that do not resolve.
//
// declaredRequired is the root object's literal "required" set and feeds only
// RelationDescriptor.ForeignKeyRequired. The guard reads a slightly wider set —
// the same array plus any reached through an allOf chain — and the two are kept
// apart deliberately; see readDeclaredRootRequired for why.
func collectRelations(
	schemaName string, props map[string]any, declaredRequired map[string]struct{},
) []RelationDescriptor {
	var relations []RelationDescriptor
	for childProp, rawProp := range props {
		propMap, ok := rawProp.(map[string]any)
		if !ok {
			continue
		}
		rel, ok := deriveRelationDescriptor(schemaName, childProp, propMap, declaredRequired)
		if !ok {
			continue
		}
		relations = append(relations, rel)
	}
	return relations
}

// deriveRelationDescriptor derives one property's descriptor, answering false when the
// property is not a usable relation root. Every early return is a property
// StripComputedFields will not strip, which is why none of them is an error.
func deriveRelationDescriptor(
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

// RelationRootNames returns schema's relation root names in sorted order, as
// diagnostic input for the write path (explainStrippedRelationRoots).
//
// Sorted because bySchema is built by ranging a map of properties, so descriptor
// order is randomised per process; an operator comparing two log lines needs the
// list to be the same list.
//
// It answers the roots alone, which is less than StripComputedFields removes —
// see RelationRoots for that distinction, which applies here identically. A nil
// receiver or a schema with no relations answers nil.
func (idx *RelationIndex) RelationRootNames(schema string) []string {
	if idx == nil {
		return nil
	}
	rels := idx.bySchema[schema]
	if len(rels) == 0 {
		return nil
	}
	names := make([]string, 0, len(rels))
	for _, rel := range rels {
		names = append(names, rel.ChildPath)
	}
	slices.Sort(names)
	return names
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
