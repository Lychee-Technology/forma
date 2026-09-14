package transform

// The write path's own required-policy check, run by ToAttributes against the
// caller's input before flattening. Its record-side twin, which ToAttributes
// also runs on every write and FromPersistentRecord runs on read, is
// AttributeConverter.checkRequiredAttributes (attribute_required.go).

import (
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// validateRequiredAttributesFromInput enforces each attribute's required policy
// against the caller's input, before flattening.
//
// Names strictly beneath a relation root are skipped, on the same boundary as
// AttributeConverter.checkRequiredAttributes (#315): the relation subtree is
// removed from every payload before validation (#318), so a policy there could
// only ever fail, and unfixably — sending the value gives the strip more to
// remove (#389). The root's own policy stays enforced; a nil or empty
// relationRoots leaves enforcement exactly as it was.
func validateRequiredAttributesFromInput(data map[string]any, cache forma.SchemaAttributeCache, relationRoots RelationRoots) error {
	if len(cache) == 0 {
		return nil
	}

	requiredNames := make([]string, 0, len(cache))
	for attrName, meta := range cache {
		if relationRoots.Covers(attrName) {
			continue
		}
		switch meta.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent:
			requiredNames = append(requiredNames, attrName)
		}
	}
	sort.Strings(requiredNames)

	for _, attrName := range requiredNames {
		meta := cache[attrName]
		missing := false
		switch meta.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			missing = isRequiredAttributeMissingInInput(data, attrName, true)
		case forma.RequiredPolicyIfParentPresent:
			missing = isRequiredAttributeMissingInInput(data, attrName, false)
		default:
			missing = false
		}
		if missing {
			// A client error: a create/update body that omits a required attribute
			// is caller fault, and since #301 the HTTP boundary classifies on
			// sentinel evidence alone — without this the write path would answer 500
			// with a redacted body instead of a 400 naming the attribute. The sibling
			// null-rejection errors in this file already carry the sentinel.
			return forma.InvalidInputf("missing required attribute '%s' (attrID=%d) in EAV records",
				attrName, meta.AttributeID)
		}
	}

	return nil
}

func isRequiredAttributeMissingInInput(root map[string]any, attrPath string, enforceWhenParentMissing bool) bool {
	segments := strings.Split(attrPath, ".")
	if len(segments) == 0 {
		return false
	}

	if len(segments) == 1 {
		value, exists := root[segments[0]]
		return !exists || value == nil
	}

	parentContexts := findExistingParentContexts(root, segments[:len(segments)-1])
	if len(parentContexts) == 0 {
		return enforceWhenParentMissing
	}

	leaf := segments[len(segments)-1]
	for _, parent := range parentContexts {
		parentMap, ok := parent.(map[string]any)
		if !ok {
			return true
		}

		value, exists := parentMap[leaf]
		if !exists || value == nil {
			return true
		}
	}

	return false
}

func findExistingParentContexts(root map[string]any, parentSegments []string) []any {
	contexts := []any{root}

	for _, segment := range parentSegments {
		nextContexts := make([]any, 0)
		for _, ctx := range contexts {
			obj, ok := ctx.(map[string]any)
			if !ok || obj == nil {
				continue
			}

			value, exists := obj[segment]
			if !exists || value == nil {
				continue
			}

			appendChildContexts(&nextContexts, value)
		}
		contexts = nextContexts
		if len(contexts) == 0 {
			return nil
		}
	}

	return contexts
}

func appendChildContexts(contexts *[]any, value any) {
	if arrayValue, ok := value.([]any); ok {
		for _, item := range arrayValue {
			if item != nil {
				*contexts = append(*contexts, item)
			}
		}
		return
	}
	*contexts = append(*contexts, value)
}
