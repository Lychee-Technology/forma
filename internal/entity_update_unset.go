package internal

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// The unset operator (#938) makes an attribute absent. It is applied in three
// places, each of which a clear that only deletes the key from the merged
// document would get wrong:
//
//   - intake decides from registry metadata alone what may be unset
//     (validateUnsetOp), so a refusal sends no SQL and leaves a Tx usable;
//   - the merge removes the target from the merged document, with the stored
//     value it held undecoded, and prunes any object the removal empties, so
//     the document validated is the one stored (removeDocumentValue);
//   - the merged record carries the main columns the clear empties
//     (PersistentRecord.ClearedColumns). The UPDATE assigns only the columns a
//     record sets and prepareUpdate copies every other stored column back in,
//     so without that intent a column-bound attribute would keep its value and
//     its unique-key entry. EAV needs nothing: the write replaces every active
//     attribute of the row.

// opsSchema is what update-operator intake reads of a schema beyond its
// attribute cache: arrays, the paths the validator declares as arrays (nil
// without a validator), and the registry, which serves the schema document
// an unset target's own shape is read from (unsetContainer).
type opsSchema struct {
	arrays   schemavalidate.ArrayPaths
	registry forma.SchemaRegistry
}

// opsSchemaOf answers the operator intake schema of schemaID.
func (s *entityCRUDService) opsSchemaOf(schemaID int16) opsSchema {
	return opsSchema{arrays: s.validator.ArrayPaths(schemaID), registry: s.registry}
}

// pathSpelling reports whether attr is an indexed, wildcard or JSONPath
// spelling ("tags[0]", "items.*", "$.name", "$") or has an empty segment,
// rather than the dotted name the attributes file gives an attribute. It is
// asked only of a name the schema does not register: a registered name is
// the attribute's own, whatever characters it holds ("price$").
func pathSpelling(attr string) bool {
	return strings.ContainsAny(attr, "[]*") || strings.HasPrefix(attr, "$") ||
		strings.HasPrefix(attr, ".") || strings.HasSuffix(attr, ".") || strings.Contains(attr, "..")
}

// validateUnsetOp checks an unset operator against the schema. Only a
// registered, writable scalar or list attribute may be unset, top-level or
// nested under ordinary objects: an object or an array of objects has no one
// value to remove, an attribute inside an array of objects has one per
// element, and one whose array ancestry the registry did not derive cannot be
// placed. Crossing is decided from the metadata's ArrayAncestors, which every
// manager has, and from the validator's array paths, where there is one. The
// target's own shape is read from the registry, whose schema document every
// manager has too (unsetContainer).
func validateUnsetOp(op forma.UpdateOp, schemaName string, cache forma.SchemaAttributeCache, relations *RelationIndex, schema opsSchema) error {
	attr := op.Attribute
	if op.By != 0 || op.Offset != 0 {
		return forma.InvalidInputf("unset on attribute '%s' sets By or Offset, which unset does not take", attr)
	}
	meta, registered := cache[attr]
	if !registered && pathSpelling(attr) {
		return forma.InvalidInputf("unset names '%s', which is not an attribute's dotted name: indexed, wildcard and JSONPath spellings are not accepted; unset takes one scalar or list attribute in the spelling of the attributes file", attr)
	}
	if model.IsMainTableColumn(attr) {
		return forma.InvalidInputf("unset names '%s', an entity_main column of schema '%s', not a schema attribute; unset clears schema attributes only", attr, schemaName)
	}
	if root, ok := relationRootCovering(relations, schemaName, attr); ok {
		return forma.InvalidInputf("attribute '%s' belongs to relation '%s', which is derived on read and never written", attr, root)
	}
	if !registered {
		return unknownUnsetTarget(attr, schemaName, cache, schema)
	}
	if binding := meta.ColumnBinding; binding != nil && model.IsSystemColumn(string(binding.ColumnName)) {
		return forma.InvalidInputf("unset names %s attribute '%s', which is bound to system column '%s': the store sets that column itself and never clears it",
			meta.ValueType, attr, binding.ColumnName)
	}
	if len(meta.ArrayAncestors) > 0 {
		return unsetInsideArray(attr, meta.ArrayAncestors[0])
	}
	if array, ok := schema.arrays.CrossedArray("", attr); ok {
		return unsetInsideArray(attr, array)
	}
	if meta.ArrayAncestors == nil && strings.Contains(attr, ".") {
		return forma.InvalidInputf("unset names nested attribute '%s' of schema '%s', whose array ancestry the registry did not derive: without it the store cannot establish where the attribute is placed, so it cannot be unset",
			attr, schemaName)
	}
	// A registered attribute can still be a container: an attributes file
	// may register as a list what the document declares an array of
	// objects, and "items": [] then stores a marker an unset must not
	// delete.
	return unsetContainer(attr, schemaName, cache, schema)
}

// unknownUnsetTarget refuses attr, which the cache does not hold: an object
// or an array of objects, otherwise an attribute the schema does not define.
func unknownUnsetTarget(attr, schemaName string, cache forma.SchemaAttributeCache, schema opsSchema) error {
	if err := unsetContainer(attr, schemaName, cache, schema); err != nil {
		return err
	}
	if array, ok := schema.arrays.CrossedArray("", attr); ok {
		return unsetInsideArray(attr, array)
	}
	return forma.InvalidInputf("unset names attribute '%s', which schema '%s' does not define", attr, schemaName)
}

// unsetContainer refuses attr when it names an object or an array of
// objects, by what any source shows: an attribute registered beneath it (an
// array of objects when one counts it among its ArrayAncestors or the
// validator declares it an array), or the schema document the registry
// serves, which shows it whether or not anything beneath it is registered
// and whether or not there is a validator. A document that does not reach
// attr, such as one behind an unresolved $ref, shows nothing, and the
// registration is then all there is to go by.
func unsetContainer(attr, schemaName string, cache forma.SchemaAttributeCache, schema opsSchema) error {
	_, declaredArray := schema.arrays[attr]
	nests := false
	for name, meta := range cache {
		if !strings.HasPrefix(name, attr+".") {
			continue
		}
		nests = true
		if slices.Contains(meta.ArrayAncestors, attr) {
			declaredArray = true
		}
	}
	kind := ""
	switch {
	case nests && declaredArray:
		kind = "array"
	case nests:
		kind = "object"
	default:
		var err error
		if kind, err = declaredContainer(attr, schemaName, schema.registry); err != nil {
			return err
		}
	}
	switch kind {
	case "array":
		return forma.InvalidInputf("unset names '%s', an array of objects in schema '%s': unset removes one scalar or list attribute, not an array of objects", attr, schemaName)
	case "object":
		return forma.InvalidInputf("unset names '%s', an object in schema '%s': unset removes one scalar or list attribute, not an object subtree; unset each attribute beneath it", attr, schemaName)
	}
	return nil
}

// declaredContainer answers "object" or "array" when the schema document
// declares attr an object or an array of objects, "" when it declares
// anything else or does not reach attr through objects. A registry without
// one (nil) shows nothing.
func declaredContainer(attr, schemaName string, registry forma.SchemaRegistry) (string, error) {
	if registry == nil {
		return "", nil
	}
	_, document, err := registry.GetSchemaByName(schemaName)
	if err != nil {
		return "", fmt.Errorf("read the document of schema '%s' to check unset target '%s': %w", schemaName, attr, err)
	}
	properties := document.Properties
	segments := strings.Split(attr, ".")
	for i, segment := range segments {
		prop := properties[segment]
		switch {
		case prop == nil:
			return "", nil
		case i < len(segments)-1:
			properties = prop.Properties
		case declaresObject(prop):
			return "object", nil
		case (prop.Type == "array" || prop.Items != nil) && declaresObject(prop.Items):
			return "array", nil
		}
	}
	return "", nil
}

// declaresObject reports whether prop declares an object: by its type or,
// for a union type the parser drops, by the properties it parses.
func declaresObject(prop *forma.PropertySchema) bool {
	return prop != nil && (prop.Type == "object" || prop.Properties != nil)
}

// unsetInsideArray refuses an unset of attr, which lies inside the array of
// objects at array: the attribute has one value per element.
func unsetInsideArray(attr, array string) error {
	return forma.InvalidInputf("attribute '%s' is inside the array of objects '%s'; unset removes one value, not one per element", attr, array)
}

// removeDocumentValue removes the value at attr's dotted path from doc, the
// merged document, and every object the removal leaves empty, the root
// excepted. A path doc does not hold is a no-op. A list on the way is
// refused (documentParent), the backstop for an array the metadata does not
// show.
//
// The objects on the path are replaced by shallow copies before the value is
// deleted (documentParent with write set), so a map of the caller's own
// Updates is never written to. The value removed is held undecoded, as merge
// hands it over, so a stored value the read refuses cannot block its clear.
func removeDocumentValue(doc map[string]any, attr string) error {
	segments := strings.Split(attr, ".")
	parent, err := documentParent(doc, attr, segments, false)
	if err != nil {
		return fmt.Errorf("find the object holding '%s': %w", attr, err)
	}
	leaf := segments[len(segments)-1]
	if _, present := parent[leaf]; parent == nil || !present {
		return nil
	}
	if parent, err = documentParent(doc, attr, segments, true); err != nil {
		return fmt.Errorf("copy the objects holding '%s': %w", attr, err)
	}
	delete(parent, leaf)
	pruneEmptiedObjects(doc, segments[:len(segments)-1])
	return nil
}

// pruneEmptiedObjects removes, innermost first, each object on path that is
// left empty. The caller has just deleted a key from the innermost one, so
// an object emptied here held only the removed value: an object that was
// empty before is never on the path of a value it held.
func pruneEmptiedObjects(doc map[string]any, path []string) {
	for depth := len(path); depth > 0; depth-- {
		holder := doc
		for _, segment := range path[:depth-1] {
			next, ok := holder[segment].(map[string]any)
			if !ok {
				return
			}
			holder = next
		}
		object, isObject := holder[path[depth-1]].(map[string]any)
		if !isObject || len(object) > 0 {
			return
		}
		delete(holder, path[depth-1])
	}
}

// clearedColumns answers the main columns the unset operators in ops empty:
// the column of each target bound to one, sorted. Intake refused a target
// bound to a system column, so each is a writable column.
func clearedColumns(ops []forma.UpdateOp, cache forma.SchemaAttributeCache) []string {
	var columns []string
	for _, op := range ops {
		if op.Kind != forma.UpdateOpUnset {
			continue
		}
		if binding := cache[op.Attribute].ColumnBinding; binding != nil {
			columns = append(columns, string(binding.ColumnName))
		}
	}
	slices.Sort(columns)
	return columns
}
