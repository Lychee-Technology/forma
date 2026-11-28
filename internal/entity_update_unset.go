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

// unsetSpellingChars are the characters of an indexed, wildcard or JSONPath
// spelling ("tags[0]", "items.*", "$.name"). An attribute name in the
// attributes file is a plain dotted path, so none of them names one.
const unsetSpellingChars = "[]*$"

// validateUnsetOp checks an unset operator against the schema. Only a
// registered, writable scalar or list attribute may be unset, top-level or
// nested under ordinary objects: an object or an array of objects has no one
// value to remove, an attribute inside an array of objects has one per
// element, and one whose array ancestry the registry did not derive cannot be
// placed. Crossing is decided from the metadata's ArrayAncestors, which every
// manager has, and from arrays, the validator's array paths, where there is
// one.
func validateUnsetOp(op forma.UpdateOp, schemaName string, cache forma.SchemaAttributeCache, relations *RelationIndex, arrays schemavalidate.ArrayPaths) error {
	attr := op.Attribute
	if op.By != 0 || op.Offset != 0 {
		return forma.InvalidInputf("unset on attribute '%s' sets By or Offset, which unset does not take", attr)
	}
	if strings.ContainsAny(attr, unsetSpellingChars) || strings.HasPrefix(attr, ".") || strings.HasSuffix(attr, ".") || strings.Contains(attr, "..") {
		return forma.InvalidInputf("unset names '%s', which is not an attribute's dotted name: indexed, wildcard and JSONPath spellings are not accepted; unset takes one scalar or list attribute in the spelling of the attributes file", attr)
	}
	if model.IsMainTableColumn(attr) {
		return forma.InvalidInputf("unset names '%s', an entity_main column of schema '%s', not a schema attribute; unset clears schema attributes only", attr, schemaName)
	}
	if root, ok := relationRootCovering(relations, schemaName, attr); ok {
		return forma.InvalidInputf("attribute '%s' belongs to relation '%s', which is derived on read and never written", attr, root)
	}
	meta, ok := cache[attr]
	if !ok {
		return unknownUnsetTarget(attr, schemaName, cache, arrays)
	}
	if binding := meta.ColumnBinding; binding != nil && model.IsSystemColumn(string(binding.ColumnName)) {
		return forma.InvalidInputf("unset names %s attribute '%s', which is bound to system column '%s': the store sets that column itself and never clears it",
			meta.ValueType, attr, binding.ColumnName)
	}
	if len(meta.ArrayAncestors) > 0 {
		return unsetInsideArray(attr, meta.ArrayAncestors[0])
	}
	if array, ok := arrays.CrossedArray("", attr); ok {
		return unsetInsideArray(attr, array)
	}
	if meta.ArrayAncestors == nil && strings.Contains(attr, ".") {
		return forma.InvalidInputf("unset names nested attribute '%s' of schema '%s', whose array ancestry the registry did not derive: without it the store cannot establish where the attribute is placed, so it cannot be unset",
			attr, schemaName)
	}
	return nil
}

// unknownUnsetTarget refuses attr, which the cache does not hold: an object
// or an array of objects when some attribute nests beneath it, otherwise an
// attribute the schema does not define.
func unknownUnsetTarget(attr, schemaName string, cache forma.SchemaAttributeCache, arrays schemavalidate.ArrayPaths) error {
	_, declaredArray := arrays[attr]
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
	switch {
	case nests && declaredArray:
		return forma.InvalidInputf("unset names '%s', an array of objects in schema '%s': unset removes one scalar or list attribute, not an array of objects", attr, schemaName)
	case nests:
		return forma.InvalidInputf("unset names '%s', an object in schema '%s': unset removes one scalar or list attribute, not an object subtree; unset each attribute beneath it", attr, schemaName)
	}
	if array, ok := arrays.CrossedArray("", attr); ok {
		return unsetInsideArray(attr, array)
	}
	return forma.InvalidInputf("unset names attribute '%s', which schema '%s' does not define", attr, schemaName)
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
