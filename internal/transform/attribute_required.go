package transform

import (
	"strings"
)

// shouldEnforceRequiredAttribute applies RequiredPolicyIfParentPresent semantics
// to an attribute using the observed EAV array-index context.
func shouldEnforceRequiredAttribute(attrName string, presentAttrIndices map[string]map[string]struct{}) bool {
	return isRequiredAttributeMissing(attrName, presentAttrIndices, false)
}

// isRequiredAttributeMissing reports whether a required attribute is missing.
//
// For nested attributes, the required check is contextual:
//   - RequiredPolicyAlways enforces the attribute even when its parent path is absent.
//   - RequiredPolicyIfParentPresent enforces the attribute only when its parent path
//     is present in the observed EAV records.
//   - Array-backed attributes must exist for every parent array index that is present.
func isRequiredAttributeMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, enforceWhenParentMissing bool) bool {
	if indices, ok := presentAttrIndices[attrName]; ok && len(indices) > 0 {
		return parentIndexMissing(attrName, presentAttrIndices, indices, enforceWhenParentMissing)
	}

	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return true
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		return enforceWhenParentMissing
	}
	// The attribute is absent entirely while its parent context exists, so the
	// required attribute is missing for every observed parent context.
	return true
}

// parentIndexMissing verifies that a child attribute is present for every parent
// context that appears in the EAV records.
func parentIndexMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, childIndices map[string]struct{}, enforceWhenParentMissing bool) bool {
	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return len(childIndices) == 0
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		// No parent context exists, so only RequiredPolicyAlways should fail here.
		return enforceWhenParentMissing && len(childIndices) == 0
	}
	if _, hasNonArrayChild := childIndices[""]; hasNonArrayChild {
		_, hasNonArrayParent := parentIndices[""]
		return !hasNonArrayParent
	}
	for idx := range parentIndices {
		if _, ok := childIndices[idx]; !ok {
			return true
		}
	}
	return false
}

// collectParentIndices gathers the array-index contexts that imply a parent path
// exists in the current EAV row. Descendant attributes contribute their observed
// indices so required children can be checked against the same contexts.
func collectParentIndices(parentPath string, presentAttrIndices map[string]map[string]struct{}) map[string]struct{} {
	parentIndices := make(map[string]struct{})
	prefix := parentPath + "."
	for presentAttrName, indexSet := range presentAttrIndices {
		if presentAttrName != parentPath && !strings.HasPrefix(presentAttrName, prefix) {
			continue
		}
		for idx := range indexSet {
			parentIndices[idx] = struct{}{}
		}
	}
	return parentIndices
}

func attributeParentPath(attrPath string) (string, bool) {
	lastDot := strings.LastIndex(attrPath, ".")
	if lastDot < 0 {
		return "", false
	}
	return attrPath[:lastDot], true
}
