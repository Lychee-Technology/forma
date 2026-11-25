package schemameta

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lychee-technology/forma"
)

// withDeclaredArrays returns cache with DeclaredArray set on every attribute
// that is not a list but whose schema document declares it a primitive array
// (#696): the element-typed ledger entry a primitive array had before #204.
// Unlike array ancestry it is judged attribute by attribute, since it only
// widens what an update may send at that one attribute, and only where the
// document says so positively.
func withDeclaredArrays(cache forma.SchemaAttributeCache, properties map[string]*forma.PropertySchema) forma.SchemaAttributeCache {
	derived := make(forma.SchemaAttributeCache, len(cache))
	for name, meta := range cache {
		meta.DeclaredArray = meta.ValueType != forma.ValueTypeList && declaresPrimitiveArray(name, properties)
		derived[name] = meta
	}
	return derived
}

// declaresPrimitiveArray reports whether the document declares name an array
// whose items are of a primitive type, reached through objects and arrays of
// objects as arrayAncestorsOf reaches it.
func declaresPrimitiveArray(name string, properties map[string]*forma.PropertySchema) bool {
	segments := strings.Split(name, ".")
	for _, segment := range segments[:len(segments)-1] {
		members, _, ok := memberProperties(properties[segment])
		if !ok {
			return false
		}
		properties = members
	}
	leaf := properties[segments[len(segments)-1]]
	return leaf != nil && leaf.Type == "array" && leaf.Items != nil && primitiveType(leaf.Items.Type)
}

func primitiveType(t string) bool {
	switch t {
	case "string", "number", "integer", "boolean":
		return true
	}
	return false
}

// ArrayPositions is what a schema document settles about the array indices
// an attribute's records may carry: one per array of objects around it, its
// Ancestors, and one more when the attribute itself is a primitive array
// (DeclaredArray). Verified is false when the document does not settle it,
// and then the other fields say nothing.
type ArrayPositions struct {
	Ancestors     []string
	DeclaredArray bool
	Verified      bool
}

// MaxIndices is the number of array indices a verified attribute's record
// may carry.
func (p ArrayPositions) MaxIndices() int {
	if p.DeclaredArray {
		return len(p.Ancestors) + 1
	}
	return len(p.Ancestors)
}

// DeriveArrayPositions answers the ArrayPositions of every attribute of
// cache, the active attributes of schemaName, from the schema's document in
// schemaDir. validate-schema-consistency's scalar-shape census (#696) judges
// stored records by them, and a census that fails a pre-flight must not
// report a record as malformed on a reading the document does not support.
//
// So the document is read for what it fixes, not as the registry's lenient
// parser reads it (withArrayAncestors). JSON Schema is conjunctive: a node's
// own "type" holds whatever its combinators add, so a node whose type is
// object, or array with items of type object, is that whatever else it
// says. A node without a type, or one whose type admits both an object and
// an array, can be either, as an object declared only by "properties" can
// be an array under an anyOf alternative: the attribute is then unverified.
// So is one behind a $ref outside the document's $defs, a missing property,
// and a leaf whose type does not fix it as a single value or a primitive
// array. A schema with no document leaves every attribute unverified.
func DeriveArrayPositions(schemaDir, schemaName string, cache forma.SchemaAttributeCache) (map[string]ArrayPositions, error) {
	positions := make(map[string]ArrayPositions, len(cache))
	schemaFile := filepath.Join(schemaDir, schemaName+".json")
	data, err := os.ReadFile(schemaFile)
	if os.IsNotExist(err) {
		return positions, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read schema file %s: %w", schemaFile, err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse schema file %s: %w", schemaFile, err)
	}
	defs := extractDefinitions(root)
	for name := range cache {
		positions[name] = settledArrayPositions(root, defs, name)
	}
	return positions, nil
}

// settledArrayPositions walks name's segments through the raw document
// (DeriveArrayPositions).
func settledArrayPositions(root, defs map[string]any, name string) ArrayPositions {
	segments := strings.Split(name, ".")
	ancestors := []string{}
	node := root
	for i, segment := range segments {
		properties, _ := node["properties"].(map[string]any)
		child, ok := resolvedNode(properties[segment], defs)
		if !ok {
			return ArrayPositions{}
		}
		kind := fixedKind(child)
		if i == len(segments)-1 {
			return leafPositions(child, kind, defs, ancestors)
		}
		switch kind {
		case kindObject:
			node = child
		case kindArray:
			items, ok := resolvedNode(child["items"], defs)
			if !ok || fixedKind(items) != kindObject {
				return ArrayPositions{}
			}
			ancestors = append(ancestors, strings.Join(segments[:i+1], "."))
			node = items
		default:
			return ArrayPositions{}
		}
	}
	return ArrayPositions{}
}

// leafPositions settles the attribute's own node: a single value, or a
// primitive array.
func leafPositions(leaf map[string]any, kind schemaKind, defs map[string]any, ancestors []string) ArrayPositions {
	switch kind {
	case kindSingle:
		return ArrayPositions{Ancestors: ancestors, Verified: true}
	case kindArray:
		items, ok := resolvedNode(leaf["items"], defs)
		if ok && fixedKind(items) == kindSingle {
			return ArrayPositions{Ancestors: ancestors, DeclaredArray: true, Verified: true}
		}
	}
	return ArrayPositions{}
}

// resolvedNode answers value as a schema node, following a $ref into the
// document's $defs as the parser does (resolvePropertyRef), and false for
// anything else.
func resolvedNode(value any, defs map[string]any) (map[string]any, bool) {
	node, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	ref, hasRef := node["$ref"].(string)
	if !hasRef {
		return node, true
	}
	resolved := resolveRef(ref, defs)
	return resolved, resolved != nil
}

// schemaKind is what a node's "type" fixes its instance to.
type schemaKind uint8

const (
	kindOpen schemaKind = iota
	kindObject
	kindArray
	kindSingle
)

// fixedKind reads a node's "type", a string or a union. A union may add
// "null", or single-value types beside an object or an array, which hold no
// record beneath them; one with both an object and an array, or a node
// without a type, is open.
func fixedKind(node map[string]any) schemaKind {
	var types []string
	switch t := node["type"].(type) {
	case string:
		types = []string{t}
	case []any:
		for _, member := range t {
			if s, ok := member.(string); ok {
				types = append(types, s)
			}
		}
	}
	if len(types) == 0 {
		return kindOpen
	}
	object, array := false, false
	for _, t := range types {
		object = object || t == "object"
		array = array || t == "array"
	}
	switch {
	case object && array:
		return kindOpen
	case object:
		return kindObject
	case array:
		return kindArray
	}
	return kindSingle
}
