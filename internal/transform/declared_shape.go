package transform

import (
	"encoding/json"
	"net/url"
	"strings"
	"sync"

	"github.com/lychee-technology/forma"
)

// SchemaShapes answers what a schema's document declares at an attribute's
// own position and at the elements of its value (Declared). It reads the
// document text the registry serves (forma.JSONSchema.Schema) rather than
// the registry's parsed properties, which leave open a $ref to another file
// (visit's leadId and contactSnapshot) and drop a type union such as
// ["string", "null"] (#696 review). So it follows a $ref itself: a JSON
// pointer into the same document, or into a sibling file the registry serves
// as a schema ("lead.json#/$defs/lead_id" is schema lead's document). A
// registry that serves parsed properties without the text is read through
// those. Validation does not enter into it: the answer is the same whether
// JSON Schema validation is strict, report-only or not configured.
//
// The document is read from the registry once, on the first question, which
// only an array at a non-list attribute's position asks (and the census); the
// zero value knows no document.
type SchemaShapes struct {
	lookup *shapeLookup
}

type shapeLookup struct {
	mu       sync.Mutex
	loaded   bool
	registry forma.SchemaRegistry
	schemaID int16
	// root is the schema's own document, nil when the registry serves none.
	root map[string]any
	// siblings caches the documents of other schemas a $ref names, by file
	// name; a nil entry is one the registry does not serve.
	siblings map[string]map[string]any
	// typeMemo caches the types each subschema admits (types).
	typeMemo map[nodeKey]typeAnswer
}

// SchemaShapesOf answers the shapes registry's document declares for
// schemaID. A registry that cannot answer the document knows none.
func SchemaShapesOf(registry forma.SchemaRegistry, schemaID int16) SchemaShapes {
	if registry == nil {
		return SchemaShapes{}
	}
	return SchemaShapes{lookup: &shapeLookup{registry: registry, schemaID: schemaID}}
}

// DeclaredShape is what a schema document declares at a position of an
// attribute's value, as far as the write can rely on it.
type DeclaredShape uint8

const (
	// ShapeUnresolved means the document gives no evidence: there is none,
	// it does not reach the attribute (a $ref to a file the registry does
	// not serve), or it leaves the position's type open ({}, an anyOf with
	// an open branch, an array without items). The write refuses an array
	// there as at a ShapeScalar (scalarShapeRefusal); only the census and a
	// stored row's records tell the two apart.
	ShapeUnresolved DeclaredShape = iota
	// ShapeScalar is a type that is not an array: an object or a single
	// value.
	ShapeScalar
	// ShapeArray is an array. Under element-typed metadata, the attribute's
	// own array is a primitive array recorded before #204 (contact.phones
	// as text), stored at array indices of its own.
	ShapeArray
)

// Declaration is what a schema document declares for one attribute: the
// shape at each depth of its own value, for each count of arrays of objects
// the document admits around it. A parent one alternative declares an
// object and another an array of objects admits two counts (#696 review).
type Declaration struct {
	// placements has one entry per count of arrays of objects around the
	// attribute that the document admits, fewest first. Nil when the
	// document does not reach the attribute.
	placements []placement
}

// placement is the attribute's value under one count of arrays of objects.
type placement struct {
	// ancestors is how many arrays of objects lie around the attribute; a
	// member of one carries an array index for each.
	ancestors int
	// shapes[i] is the shape declared i array levels into the attribute's
	// own value: shapes[0] is the attribute's own position, and only the
	// last entry is not ShapeArray.
	shapes []DeclaredShape
}

// At answers the shape declared at a position of the attribute inside
// ancestors arrays of objects and own array levels into its own value, as
// the write walked it: the ancestry is the value's, not one an alternative
// the value does not take declares. A position past the last declared level
// has the last one's shape, which is never an array; one under a count of
// arrays of objects the document does not admit is unresolved.
func (d Declaration) At(ancestors, own int) DeclaredShape {
	for _, p := range d.placements {
		if p.ancestors == ancestors {
			return p.shapes[min(own, len(p.shapes)-1)]
		}
	}
	return ShapeUnresolved
}

// Indices answers how many array indices a stored record of the attribute
// may carry as far as the document shows. A record carries no index telling
// its ancestry from its own value's, so where the document admits several
// counts of arrays of objects, a record within one reading may be malformed
// under another: sure is the most every reading admits, most the most any
// does. exact reports whether the document shows that no record carries
// more than most: every reading declares the innermost level a non-array
// type.
func (d Declaration) Indices() (sure, most int, exact bool) {
	if len(d.placements) == 0 {
		return 0, 0, false
	}
	sure, exact = -1, true
	for _, p := range d.placements {
		last := len(p.shapes) - 1
		depth := p.ancestors + last
		if sure < 0 || depth < sure {
			sure = depth
		}
		most = max(most, depth)
		exact = exact && p.shapes[last] == ShapeScalar
	}
	return sure, most, exact
}

// Declared answers what the document declares for the attribute name.
func (s SchemaShapes) Declared(name string) Declaration {
	if s.lookup == nil {
		return Declaration{}
	}
	s.lookup.mu.Lock()
	defer s.lookup.mu.Unlock()
	if !s.lookup.loaded {
		s.lookup.loaded = true
		_, schema, err := s.lookup.registry.GetSchemaByID(s.lookup.schemaID)
		s.lookup.root = shapeDocument(schema, err)
	}
	return s.lookup.declared(name)
}

// shapeDocument answers the document a registry answer holds: its text, or, when
// the text declares no properties and the registry parsed some anyway, those
// properties; nil when it holds neither.
func shapeDocument(schema forma.JSONSchema, err error) map[string]any {
	if err != nil {
		return nil
	}
	var doc map[string]any
	if json.Unmarshal([]byte(schema.Schema), &doc) == nil && doc != nil {
		if _, declares := doc["properties"]; declares || len(schema.Properties) == 0 {
			return doc
		}
	}
	if len(schema.Properties) == 0 {
		return nil
	}
	text, err := json.Marshal(map[string]any{"type": "object", "properties": schema.Properties})
	var parsed map[string]any
	if err != nil || json.Unmarshal(text, &parsed) != nil {
		return nil
	}
	return parsed
}

// maxShapeLevels bounds the array levels one answer follows, so an array
// whose items $ref the array itself ends; maxShapeNodes bounds the
// subschemas one lookup reads, so a document of many combinators ends too.
const (
	maxShapeLevels = 32
	maxShapeNodes  = 4096
)

// schemaNode is a subschema of a document: file names the sibling document
// holding it, "" for the schema's own.
type schemaNode struct {
	value map[string]any
	file  string
}

// branches answers the subschemas of a combinator keyword of node.
func (l *shapeLookup) branches(node schemaNode, keyword string) []schemaNode {
	list, _ := node.value[keyword].([]any)
	out := make([]schemaNode, 0, len(list))
	for _, item := range list {
		if branch, ok := item.(map[string]any); ok {
			out = append(out, schemaNode{value: branch, file: node.file})
		}
	}
	return out
}

// ref answers the subschema node's $ref names: a JSON pointer into node's
// own document ("#/$defs/x") or into a sibling file the registry serves as a
// schema ("lead.json#/properties/contact"). Any other reference, a sibling
// in another directory or on the network included, is not followed.
func (l *shapeLookup) ref(node schemaNode) (schemaNode, bool) {
	ref, ok := node.value["$ref"].(string)
	if !ok {
		return schemaNode{}, false
	}
	file, pointer, _ := strings.Cut(ref, "#")
	if file == "" {
		file = node.file
	}
	doc := l.root
	if file != "" {
		doc = l.sibling(file)
	}
	target, ok := resolvePointer(doc, pointer)
	return schemaNode{value: target, file: file}, ok
}

// sibling answers the document of the schema file names, a plain file name
// such as "lead.json", or nil.
func (l *shapeLookup) sibling(file string) map[string]any {
	if doc, seen := l.siblings[file]; seen {
		return doc
	}
	var doc map[string]any
	if name, ok := strings.CutSuffix(file, ".json"); ok && name != "" && !strings.ContainsAny(name, `/\:`) {
		_, schema, err := l.registry.GetSchemaByName(name)
		doc = shapeDocument(schema, err)
	}
	if l.siblings == nil {
		l.siblings = make(map[string]map[string]any)
	}
	l.siblings[file] = doc
	return doc
}

// resolvePointer answers the object a JSON pointer ("/$defs/lead_id", "" for
// the whole document) names in doc.
func resolvePointer(doc map[string]any, pointer string) (map[string]any, bool) {
	if doc == nil {
		return nil, false
	}
	if unescaped, err := url.PathUnescape(pointer); err == nil {
		pointer = unescaped
	}
	var current any = doc
	if pointer != "" {
		if !strings.HasPrefix(pointer, "/") {
			return nil, false
		}
		for _, token := range strings.Split(pointer[1:], "/") {
			token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
			object, ok := current.(map[string]any)
			if !ok {
				return nil, false
			}
			current = object[token]
		}
	}
	object, ok := current.(map[string]any)
	return object, ok
}
