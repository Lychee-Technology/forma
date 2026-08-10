package internal

import (
	"fmt"
	"strings"
)

// rootRequiredWalk collects the property names a schema document can make
// required on its own root object, and records the first reference it cannot
// follow.
//
// It exists because the #318 guard has to answer a runtime question — "will the
// validator demand this property?" — from a raw map[string]any. The validator
// resolves composition, so reading the root's "required" array alone misses a
// relation root made mandatory through allOf/oneOf/if-then/dependent* and boots
// a schema whose every create then fails.
type rootRequiredWalk struct {
	required map[string]struct{}
	// unfollowedRef is the keyword path of the first $ref/$dynamicRef found on a
	// subschema that applies to the root instance, or "" when there is none.
	unfollowedRef string
}

// sameLocationBranchKeywords are the applicators whose subschemas are asserted
// against the *same* instance location as the schema carrying them. A "required"
// inside one of them therefore constrains the root object's own properties.
//
// "if" is in this list even though a failing "if" invalidates nothing. It is
// collected deliberately, for a different reason than the others: a relation
// root is stripped from every payload, so an "if" that tests for it can never be
// satisfied, which silently makes "then" dead and "else" unconditional. That is
// a schema doing something other than what it says, and an operator should hear
// about it at startup.
var sameLocationBranchKeywords = []string{"if", "then", "else"}

// sameLocationListKeywords are the same-instance-location applicators whose
// value is an array of subschemas.
//
// anyOf/oneOf are collected even though they only apply conditionally: a schema
// that requires a stripped property on *some* documents is still unwritable for
// exactly those documents, and there is no payload the caller can send to
// satisfy the branch. Refusing the whole schema is the intended outcome — do not
// "fix" this by restricting collection to allOf.
var sameLocationListKeywords = []string{"allOf", "anyOf", "oneOf"}

// analyzeRootRequired walks schema for every property name it can make required
// on the root object.
//
// It walks only applicators that assert against the root instance, recursively.
// Two exclusions are load-bearing:
//
//   - "properties" (and patternProperties/additionalProperties/items/$defs) are
//     not walked. A "required" there constrains a *child* object's members, not
//     the root's, so collecting it would reject a schema that is perfectly
//     writable.
//   - "not" is not walked. A "required" beneath it asserts the opposite and can
//     never make a property mandatory, so ignoring it is sound and collecting it
//     would be a false rejection.
//
// The document is a decoded JSON tree, so it has no cycles and the recursion
// terminates on depth alone.
func analyzeRootRequired(schema map[string]any) rootRequiredWalk {
	w := rootRequiredWalk{required: make(map[string]struct{})}
	w.walk(schema, "")
	return w
}

// walk accumulates one subschema's contribution and recurses into its
// same-instance-location applicators. path is the keyword path of node relative
// to the document root, used only to name an unfollowable reference.
func (w *rootRequiredWalk) walk(node map[string]any, path string) {
	w.noteRef(node, path)
	collectNameList(node["required"], w.required)

	for _, kw := range sameLocationListKeywords {
		branches, ok := node[kw].([]any)
		if !ok {
			continue
		}
		for i, branch := range branches {
			if sub, ok := branch.(map[string]any); ok {
				w.walk(sub, fmt.Sprintf("%s/%s/%d", path, kw, i))
			}
		}
	}

	for _, kw := range sameLocationBranchKeywords {
		if sub, ok := node[kw].(map[string]any); ok {
			w.walk(sub, path+"/"+kw)
		}
	}

	// dependentRequired maps a trigger property to names required whenever the
	// trigger is present. Every listed name is a root requirement for some
	// document, so all of them count.
	if deps, ok := node["dependentRequired"].(map[string]any); ok {
		for _, names := range deps {
			collectNameList(names, w.required)
		}
	}

	// dependentSchemas is the schema-valued form of the same rule: each value is
	// asserted against the root instance when its trigger property is present.
	if deps, ok := node["dependentSchemas"].(map[string]any); ok {
		for trigger, sub := range deps {
			if subMap, ok := sub.(map[string]any); ok {
				w.walk(subMap, path+"/dependentSchemas/"+trigger)
			}
		}
	}
}

// noteRef records the first reference keyword found on a subschema that applies
// to the root instance. This package never resolves references, and the
// resolving library does not hand back a resolved graph to borrow either: the
// only schema accessor on jsonschema.Resolved is Schema(), and after Resolve
// that still answers the root with Ref set to the unresolved string it was
// parsed from. Measured: a root {"$ref":"#/$defs/base"} whose $defs/base carries
// required:["contactSnapshot"] comes back from Resolve with Ref unchanged and
// Required nil, while Validate rejects a document lacking contactSnapshot. So a
// reference here means the walk's answer is incomplete, and the caller must
// refuse rather than guess.
//
// A reference on a *property* is out of this walk's reach by construction:
// walk never descends into "properties". visit.json's contactSnapshot is exactly
// that shape and must keep working.
func (w *rootRequiredWalk) noteRef(node map[string]any, path string) {
	if w.unfollowedRef != "" {
		return
	}
	for _, kw := range []string{"$ref", "$dynamicRef"} {
		if ref, ok := node[kw].(string); ok && strings.TrimSpace(ref) != "" {
			w.unfollowedRef = path + "/" + kw
			return
		}
	}
}

// collectNameList adds the strings of a decoded JSON array to dst. A non-array
// value, or a member that is not a string, is not a name list any validator
// would honour and is skipped.
func collectNameList(raw any, dst map[string]struct{}) {
	list, ok := raw.([]any)
	if !ok {
		return
	}
	for _, item := range list {
		if name, ok := item.(string); ok {
			dst[name] = struct{}{}
		}
	}
}

// declaredRootRequired reads the root object's own "required" array and nothing
// else.
//
// This deliberately stays narrower than analyzeRootRequired, because it feeds
// RelationDescriptor.ForeignKeyRequired, which gates a "missing required parent
// foreign key" warning on read (entityRelationService.enrichDataRecords). A
// foreign key that is required only under a condition is legitimately absent
// from the documents that do not meet it, so widening this set would log a
// warning about a record that is entirely valid.
func declaredRootRequired(schema map[string]any) map[string]struct{} {
	required := make(map[string]struct{})
	collectNameList(schema["required"], required)
	return required
}

// checkRelationRootsWritable refuses a schema whose relation roots the validator
// would demand, and refuses a schema whose root requirements cannot be
// determined at all.
//
// Both are startup/operator errors: they are plain wrapped errors, never
// forma.InvalidInputf, because no caller can act on them and they must never
// surface as a 4xx (docs/error-handling.md).
//
// Callers must invoke this only for a schema that actually declares relations.
// A property that never reached the relations slice is never stripped, so
// requiring it — or being unable to analyse it — is harmless.
func checkRelationRootsWritable(schemaName string, schema map[string]any, relations []RelationDescriptor) error {
	analysis := analyzeRootRequired(schema)

	if analysis.unfollowedRef != "" {
		return fmt.Errorf(
			"schema %s declares a relation root and applies %q to its root object: this loader cannot follow a reference, "+
				"so it cannot tell whether the reference makes a relation root mandatory — and a mandatory relation root makes "+
				"every write fail, because the root is stripped before validation; inline the referenced constraints into %s.json "+
				"or move the reference onto a property",
			schemaName, analysis.unfollowedRef, schemaName)
	}

	for _, rel := range relations {
		if _, isRequired := analysis.required[rel.ChildPath]; !isRequired {
			continue
		}
		return fmt.Errorf(
			"schema %s requires relation root %q: a relation root is stripped from every payload before validation, so every "+
				"create fails with a missing-required error the caller cannot fix, as does every update when strict update "+
				"validation is on; remove it from \"required\" (including any allOf/anyOf/oneOf/if/then/else/dependentRequired/"+
				"dependentSchemas branch that names it) or drop its x-relation marker",
			schemaName, rel.ChildPath)
	}
	return nil
}
