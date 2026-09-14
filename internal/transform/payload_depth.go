package transform

import "github.com/lychee-technology/forma"

// maxPayloadNestingDepth bounds the two recursions this package runs over the
// caller's own nesting: NormalizeDottedKeys (validator configured) and
// flattenToAttributes (always). Neither has a natural end on a cyclic Go value
// — `m := map[string]any{}; m["self"] = m` — and both sit in front of the
// layers that would catch one (json.Marshal, and the validator's own capped
// walk), so without a cap here a cyclic payload exhausts the stack fatally
// before any validation runs (#406).
//
// Only a Go embedder can hand in such a value: HTTP JSON cannot express a
// cycle, and encoding/json's decoder refuses nesting past 10000. The cap
// therefore also refuses an acyclic document deeper than 1000, which no
// shipped schema comes near.
//
// The value matches schemavalidate's maxMarshalRefusalWalkDepth on purpose,
// but the two are separate constants because they mean different things:
// the validator's cap degrades an error *message* on a payload already
// refused elsewhere, while this one is what refuses the payload.
const maxPayloadNestingDepth = 1000

// payloadPosition locates a container in the caller's document for the cap:
// how many container edges below the root map it sits — the root itself is
// depth 0 — and the root key beneath which it sits, which is what the error
// names. An array element counts as an edge exactly like an object member,
// so a cycle through []any, where the dotted name never grows, is still
// bounded.
type payloadPosition struct {
	root  string
	depth int
}

// into is the position of a child of the container at p reached through key.
// The root map's own children set the root key; below that it is inherited.
// Array elements pass "" — an array is never the root document, so the value
// is unused there.
func (p payloadPosition) into(key string) payloadPosition {
	if p.depth == 0 {
		p.root = key
	}
	p.depth++
	return p
}

// flattenPosition derives the position flattenToAttributes already carries
// implicitly: path grows by one per object member and indices by one per
// array element, so their combined length is the depth.
func flattenPosition(path []string, indices []int) payloadPosition {
	pos := payloadPosition{depth: len(path) + len(indices)}
	if len(path) > 0 {
		pos.root = path[0]
	}
	return pos
}

// checkPayloadDepth is the shared abort: a container past the cap is rejected
// as caller input, published, since a cyclic or absurdly deep document is
// something the caller sent rather than a server fault. Only the root key is
// named — at depth 1000 the full dotted path is a multi-kilobyte string, and
// the root key is enough to find the runaway branch.
func checkPayloadDepth(pos payloadPosition) error {
	if pos.depth <= maxPayloadNestingDepth {
		return nil
	}
	return forma.InvalidInputf(
		"payload nesting exceeds %d levels beneath attribute %q; the payload is cyclic or too deeply nested",
		maxPayloadNestingDepth, pos.root)
}
