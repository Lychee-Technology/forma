package internal

import (
	"fmt"
)

// collectUnconditionalRootRequired collects the property names schema makes
// required on its root object *unconditionally*.
//
// It exists because the #318 guard has to answer a runtime question — "will the
// validator demand this property?" — from a raw map[string]any, before any
// payload exists. In full generality that question is undecidable by inspection:
// whether a schema's logic ends up demanding a property is a satisfiability
// question, and three rounds of widening a static walk to chase it produced both
// false negatives and false positives. So the walk no longer tries. It reads one
// small fragment of JSON Schema in which the answer needs no reasoning at all,
// and says nothing whatsoever about the rest.
//
// The fragment is exactly two rules:
//
//   - the root object's own "required";
//   - a "required" reached through a chain of "allOf" branches, recursively.
//
// Both are sound for the same reason: every link on such a chain applies to the
// root instance, always, with no condition to evaluate. allOf asserts that the
// instance validates against *every* branch, so a name collected at any depth of
// an allOf chain is demanded of every document, exactly as the root array's own
// names are.
//
// Everything else is **not judged**. "not", "anyOf", "oneOf", "if"/"then"/
// "else", "dependentRequired", "dependentSchemas", draft-07's "dependencies",
// "$ref" and "$dynamicRef" contribute no name and are no reason to refuse a
// schema either — the walk steps over them in silence. That is not an
// approximation of their meaning, it is a refusal to have one.
//
// Two consequences, both deliberate:
//
//   - A name collected here is valid regardless of what sits beside it. The walk
//     never enters a negated or conditional context, so there is no position in
//     which a collected "required" means the opposite of what it says.
//     allOf: [{"required":["x"]}, {"not": …}] still yields x, because the first
//     branch is unconditional whatever the second one does.
//   - The set is incomplete, and knowingly so. {"not":{"not":{"required":["x"]}}}
//     demands x and is not collected; nor is a requirement inside oneOf, then,
//     or dependentRequired. Those are false negatives with no startup symptom.
//     The backstop for them is on the write path, where a concrete document
//     exists and the validator's real verdict is available:
//     explainStrippedRelationRoots (entity_write_validation.go) explains the
//     failure to the operator instead of leaving an unfixable 4xx unexplained.
//
// The trade is one-sided on purpose. A false negative costs an explained runtime
// failure; a false positive costs a deployment that cannot boot at all over a
// schema that would have worked. Only the second is unfixable from outside.
//
// The document is a decoded JSON tree, so it has no cycles and the recursion
// terminates on depth alone.
func collectUnconditionalRootRequired(schema map[string]any) map[string]struct{} {
	required := make(map[string]struct{})
	walkAllOfChain(schema, required)
	return required
}

// walkAllOfChain adds one subschema's own "required" to dst and recurses into
// its "allOf" branches, which are the only descent the fragment permits.
//
// A branch that is not a JSON object is skipped, and that covers every remaining
// input class rather than the ones that came to mind. A decoded JSON value there
// is one of: an object, true, false, a string, a number, null, or an array.
//
//   - true and false are legal JSON Schemas and schemavalidate.New accepts a
//     document containing either (measured). Neither declares a "required", so
//     there is nothing to collect. false makes the whole document unsatisfiable
//     rather than making any one property mandatory, which is not a question
//     this walk answers.
//   - a string, a number or an array fails schemavalidate.New at parse
//     ("cannot unmarshal … into … allOf"), and null fails it at resolve
//     ("schema at /allOf/0 is nil") — also measured. Skipping them here only
//     means this walk is not the check that reports them.
func walkAllOfChain(node map[string]any, dst map[string]struct{}) {
	collectNameList(node["required"], dst)

	branches, ok := node["allOf"].([]any)
	if !ok {
		return
	}
	for _, branch := range branches {
		if sub, ok := branch.(map[string]any); ok {
			walkAllOfChain(sub, dst)
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

// readDeclaredRootRequired reads the root object's own "required" array and
// nothing else.
//
// It feeds RelationDescriptor.ForeignKeyRequired, which gates a "missing
// required parent foreign key" warning on read
// (entityRelationService.enrichDataRecords).
//
// It stays narrower than collectUnconditionalRootRequired by exactly one thing:
// a "required" inside an allOf chain. That difference is no longer about
// soundness — since the fragment shrank, everything the guard collects is
// unconditional, so an allOf-required foreign key would be as genuinely
// mandatory as a root-required one. It is about scope: widening this set changes
// which records log that warning, and nobody has asked for that change. Do not
// fold the two functions together without deciding that question on its own
// merits.
func readDeclaredRootRequired(schema map[string]any) map[string]struct{} {
	required := make(map[string]struct{})
	collectNameList(schema["required"], required)
	return required
}

// checkRelationRootsWritable refuses a schema whose relation roots the analysed
// fragment shows the validator will demand.
//
// It refuses nothing else. A schema whose requirements this package cannot
// determine is *not* refused — being unanalysable is the normal case for
// anything outside the fragment, and aborting startup over it would refuse
// schemas that write perfectly well (an "anyOf" whose other branch is the
// boolean true, an "if" that never matches, a "$ref" to a parent that requires
// nothing — all three measured, see relation_required_conditional_test.go).
//
// The error is a startup/operator error: a plain wrapped error, never
// forma.InvalidInputf, because no caller can act on it and it must never surface
// as a 4xx (docs/error-handling.md).
//
// Callers must invoke this only for a schema that actually declares relations. A
// property that never reached the relations slice is never stripped, so
// requiring it is harmless.
func checkRelationRootsWritable(schemaName string, schema map[string]any, relations []RelationDescriptor) error {
	required := collectUnconditionalRootRequired(schema)

	for _, rel := range relations {
		if _, isRequired := required[rel.ChildPath]; !isRequired {
			continue
		}
		return fmt.Errorf(
			"schema %s requires relation root %q on every document: a relation root is stripped from every payload "+
				"before validation, so every create — and every update under strict update validation — fails with a "+
				"missing-required error the caller cannot fix by sending the field; remove %q from the \"required\" array "+
				"that names it (the root object's own, or one in an allOf branch), or drop its x-relation marker",
			schemaName, rel.ChildPath, rel.ChildPath)
	}
	return nil
}
