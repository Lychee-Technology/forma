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
// a schema whose writes then fail — always for an unconditional requirement,
// and for the documents that take the branch for a conditional one.
type rootRequiredWalk struct {
	required map[string]struct{}
	// unfollowedRef is the keyword path of the first $ref/$dynamicRef found on a
	// subschema that applies to the root instance, or "" when there is none.
	unfollowedRef string
}

// conditionalKeywords are the "if"/"then"/"else" trio. They are walked as a
// group, and only when the group can affect validation at all — see
// walkConditional.
//
// "if" is collected along with its branches even though a failing "if"
// invalidates nothing on its own, and for a different reason than the others: a
// relation root never reaches the validator, so an "if" that turns on the
// presence of one is being decided against a property no payload can carry, and
// the branch the author meant to select may be permanently dead. That is a
// schema doing something other than what it says, and an operator should hear
// about it at startup rather than discover it as a mis-validated document.
//
// This over-approximates — an "if" could also be satisfiable by some other
// disjunct it contains — which is the intended direction: the guard refuses a
// suspect schema instead of booting a broken one. Widening it costs nothing
// today (no shipped schema uses "if" at all).
var conditionalKeywords = []string{"if", "then", "else"}

// sameLocationListKeywords are the same-instance-location applicators whose
// value is an array of subschemas.
//
// anyOf/oneOf are collected even though they only apply conditionally: a schema
// that requires a stripped property on *some* documents is still unwritable for
// exactly those documents, and there is no payload the caller can send to
// satisfy the branch. Refusing the whole schema is the intended outcome — do not
// "fix" this by restricting collection to allOf.
var sameLocationListKeywords = []string{"allOf", "anyOf", "oneOf"}

// dependentKeywords are the three spellings of "these names become required once
// this trigger property is present". Each is asserted against the same instance
// location as the schema carrying it.
//
// "dependencies" is draft-07's spelling, and including it is not optional:
// internal/schemavalidate accepts draft-07 (schema_check.go's
// supportedSchemaVersions, pinned by TestNewAcceptsSupportedSchemaVersions), and
// under draft-07 the library enforces the rule from "dependencies" alone.
// jsonschema-go@v0.4.2 splits that keyword into DependencyStrings and
// DependencySchemas while unmarshalling (schema.go:448-467), and validate.go
// reads only those two when the draft is draft7, reading
// DependentRequired/DependentSchemas only when it is draft2020
// (validate.go:569-613). Omitting it left the #318 bypass alive under another
// name.
//
// All three are collected regardless of the document's declared draft, rather
// than switching on $schema. That over-approximates by exactly one case: a
// keyword written in the draft that does not define it — "dependencies" in a
// 2020-12 document, or the 2020-12 pair in a draft-07 one — is still recognised
// and populated, and merely never consulted. UnmarshalJSON splits "dependencies"
// into DependencyStrings/DependencySchemas with no draft test at all
// (schema.go:448-467), and the 2020-12 pair has ordinary json tags; what decides
// whether either is honoured is the draft gate in validate.go, which reads the
// draft-07 fields only under draft7 (:569) and the 2020-12 fields only under
// draft2020 (:589). It is a known keyword the gate declines to read, not an
// unknown one the parser discards — which is why the warning below is about
// getting the *draft* wrong. Collecting the ungated one can refuse a schema the
// validator would not have constrained. The trade is deliberate and one-sided:
// the cost is a startup message telling an operator to delete a keyword that was
// already doing nothing, while getting the draft detection wrong in the other
// direction reinstates a silent bypass. Do not "optimise" this into a
// draft-conditional lookup without re-deriving which draft the library picks for
// a document with no $schema, which supportedSchemaVersions permits.
var dependentKeywords = []string{"dependentRequired", "dependentSchemas", "dependencies"}

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

	w.walkConditional(node, path)

	for _, kw := range dependentKeywords {
		w.walkDependent(node, path, kw)
	}
}

// walkConditional walks "if"/"then"/"else", but only when that trio has some
// validation effect.
//
// The gate is what keeps this from rejecting writable schemas. "then" and "else"
// are inert without an "if" — measured: a schema whose only conditional keyword
// is then:{"required":["contactSnapshot"]} validates {"id":"x"} as nil — and an
// "if" with neither branch is inert in turn, since there is nothing for its
// outcome to select. Collecting from an inert trio would refuse a schema the
// validator never constrains, which is the one failure mode this guard must not
// have.
func (w *rootRequiredWalk) walkConditional(node map[string]any, path string) {
	// Presence is tested on the key, not on its decoded shape, so that a boolean
	// schema ("if": true, which makes "then" unconditional) still opens the gate.
	_, hasIf := node["if"]
	_, hasThen := node["then"]
	_, hasElse := node["else"]
	if !hasIf || (!hasThen && !hasElse) {
		return
	}

	for _, kw := range conditionalKeywords {
		if sub, ok := node[kw].(map[string]any); ok {
			w.walk(sub, path+"/"+kw)
		}
	}
}

// walkDependent walks one trigger-keyed keyword. Each value is either a name
// list — draft-07's "dependencies" allows the array form, and
// "dependentRequired" only that — or a subschema asserted against the same
// instance location, which "dependencies" also allows and "dependentSchemas"
// requires. Both shapes are accepted for every keyword so that neither draft's
// spelling can slip past on a shape technicality.
func (w *rootRequiredWalk) walkDependent(node map[string]any, path, keyword string) {
	deps, ok := node[keyword].(map[string]any)
	if !ok {
		return
	}
	for trigger, value := range deps {
		switch v := value.(type) {
		case []any:
			collectNameList(v, w.required)
		case map[string]any:
			w.walk(v, path+"/"+keyword+"/"+trigger)
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
			"schema %s requires relation root %q: a relation root is stripped from every payload before validation, so a write "+
				"that has to satisfy that requirement fails with a missing-required error the caller cannot fix by sending the "+
				"field — every create and update when the requirement is unconditional (the root \"required\" array, or an allOf "+
				"branch), and every document that takes the branch when it is conditional (anyOf/oneOf/if/then/else/"+
				"dependentRequired/dependentSchemas/dependencies); remove it from every \"required\" that names it, or drop its "+
				"x-relation marker",
			schemaName, rel.ChildPath)
	}
	return nil
}
