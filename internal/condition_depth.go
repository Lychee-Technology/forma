package internal

import "github.com/lychee-technology/forma"

// maxConditionNestingDepth bounds how deeply a condition tree may nest
// (#675). forma.CompositeCondition holds its children as []forma.Condition,
// so a Go embedder can build a tree that contains itself, and every walk over
// a condition (the precondition and guard validators, the query repository's
// main-table probe, predicate normalization, the PostgreSQL and DuckDB
// renderers) recurses without an end of its own. Without a bound such a tree
// exhausts the stack fatally, taking the whole process down instead of
// answering ErrInvalidInput.
//
// The number is the payload cap's (maxPayloadNestingDepth in
// internal/transform, #406), so one nesting limit holds across the API. It is
// not configurable: real conditions are a few levels deep, and HTTP JSON
// cannot express a cycle, so only a Go embedder's runaway tree comes near it.
//
// Depth counts composite edges from the root, as the payload cap counts
// container edges: the root is at depth 0 and each child sits one deeper
// than its composite. A tree is accepted when every node, leaf or composite,
// is at depth maxConditionNestingDepth or less, so the deepest accepted chain
// is 1000 composites over one leaf.
const maxConditionNestingDepth = 1000

// checkConditionDepth refuses cond when a node of it sits deeper than
// maxConditionNestingDepth. It is the first walk over a condition at every
// entry point that accepts one, so every later walker may rely on the bound.
// The walk is itself bounded by depth, which is what stops it on a cycle: a
// cycle is infinitely deep, so no visited set is needed. noun names the
// condition in the published message ("condition", "precondition", ...).
// nil passes.
func checkConditionDepth(noun string, cond forma.Condition) error {
	if conditionWithinDepth(cond, 0) {
		return nil
	}
	return forma.InvalidInputf(
		"%s nests composite conditions more than %d levels deep; the %s is cyclic or too deeply nested",
		noun, maxConditionNestingDepth, noun)
}

// conditionWithinDepth reports whether cond, sitting at depth, and all of its
// descendants are within the bound. It stops at the first node past it. Only
// a non-nil *forma.CompositeCondition has children; a leaf, a nil node and a
// caller's own Condition type end the walk and are left to the validators.
func conditionWithinDepth(cond forma.Condition, depth int) bool {
	if depth > maxConditionNestingDepth {
		return false
	}
	composite, ok := cond.(*forma.CompositeCondition)
	if !ok || composite == nil {
		return true
	}
	for _, child := range composite.Conditions {
		if !conditionWithinDepth(child, depth+1) {
			return false
		}
	}
	return true
}
