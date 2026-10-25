package main

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// typeFamily names the group of valueTypes that one JSON Schema node maps to.
// getValueType infers only one member per family (numeric, text, date, bool),
// so a ledger entry that holds a sibling the inference cannot produce — a
// hand-set bigint, integer, smallint, uuid or datetime — is the same family,
// not a different type (#732). An unrecognised valueType is its own family,
// so it only matches itself.
func typeFamily(valueType string) string {
	switch forma.ValueType(valueType) {
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return "numeric"
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return "text"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "date"
	default:
		return valueType
	}
}

// resolveExistingType decides the type a ledger entry keeps when the schema
// still declares it. Within the inferred type's family the entry keeps its
// own valueType and items_type: JSON `integer`/`number` says "a number", not
// "replace bigint with numeric". Across families the generator refuses. The
// stored values were written under the old type, and D2 (#710) allows a type
// change only when they are shown to satisfy the new one, which this tool
// cannot show. An entry without a valueType takes the inferred one.
func resolveExistingType(name string, existingData map[string]any, inferred attributeSpec) (attributeSpec, error) {
	oldType, _ := existingData["valueType"].(string)
	if oldType == "" {
		return inferred, nil
	}
	if isLegacyFlattenedList(oldType, inferred) {
		// Before #204 the generator recorded a primitive array as its element
		// type. The JSON Schema always said array and the EAV rows are already
		// one per element, so recording list is the ledger catching up with the
		// stored shape, not a type change. The element keeps its width.
		resolved := inferred
		resolved.ItemsType = oldType
		return resolved, nil
	}
	if typeFamily(oldType) != typeFamily(inferred.ValueType) {
		return attributeSpec{}, typeChangeError(name, existingData, "valueType", oldType, inferred.ValueType)
	}
	resolved := inferred
	resolved.ValueType = oldType
	if oldType != string(forma.ValueTypeList) {
		return resolved, nil
	}
	oldItems, _ := existingData["items_type"].(string)
	oldItems = effectiveItemsType(oldItems)
	if typeFamily(oldItems) != typeFamily(inferred.ItemsType) {
		return attributeSpec{}, typeChangeError(name, existingData, "items_type", oldItems, inferred.ItemsType)
	}
	resolved.ItemsType = oldItems
	return resolved, nil
}

// isLegacyFlattenedList reports a pre-#204 ledger entry: a scalar valueType
// recorded for what the JSON Schema declares as an array of the same family.
func isLegacyFlattenedList(oldType string, inferred attributeSpec) bool {
	return inferred.ValueType == string(forma.ValueTypeList) &&
		oldType != string(forma.ValueTypeList) &&
		typeFamily(oldType) == typeFamily(inferred.ItemsType)
}

// typeChangeError names the attribute, both types and their families, and the
// remediation. A retired entry says so: its preserved EAV rows are why the
// re-add must keep the old type (#342).
func typeChangeError(name string, existingData map[string]any, field, oldType, inferredType string) error {
	subject := "attribute " + name
	if retired, _ := existingData["retired"].(bool); retired {
		subject = "retired attribute " + name
	}
	return fmt.Errorf(
		"%s: the attributes file declares %s %s (%s family) but the JSON Schema now maps it to %s (%s family); "+
			"generate-attributes never changes an existing attribute's type, because its stored values were written as %s (#710). "+
			"Restore the JSON Schema type, or declare the new type under a new attribute name; converting stored values is an explicit migration (#730)",
		subject, field, oldType, typeFamily(oldType), inferredType, typeFamily(inferredType), oldType)
}
