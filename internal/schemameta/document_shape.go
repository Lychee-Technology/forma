package schemameta

import "strings"

// This file reads what a schema document admits at a node with its
// combinators, as JSON Schema reads them (#696 review R4): every allOf
// conjunct holds of the instance, and so does at least one branch of an
// anyOf or a oneOf. A branch the document does not resolve, such as a $ref
// to another file, admits anything; "not" and "if" are not read. Each only
// widens what a node admits, so what is read here is what a valid instance
// may be, never narrower.

// kindSet is the instance kinds a node admits: an array, an object, or a
// single value (null included).
type kindSet uint8

const (
	kindSetArray kindSet = 1 << iota
	kindSetObject
	kindSetSingle
	kindSetAll = kindSetArray | kindSetObject | kindSetSingle
)

// maxShapeDepth caps the nodes one reading passes through, so a $ref cycle
// (an array whose items refer to it) reads as admitting anything.
const maxShapeDepth = 32

// algebra is how readings of one node's keywords combine: and for
// conjuncts, or for branches, none the empty union and any what a node
// without keywords admits.
type algebra[T any] struct {
	and, or   func(T, T) T
	none, any T
}

// combined narrows own, the reading of node's own keywords, by its
// combinators, each subschema read by read.
func combined[T any](node, defs map[string]any, own T, read func(map[string]any) T, alg algebra[T]) T {
	result := own
	conjuncts, _ := node["allOf"].([]any)
	for _, conjunct := range conjuncts {
		if resolved, ok := resolvedNode(conjunct, defs); ok {
			result = alg.and(result, read(resolved))
		}
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		branches, _ := node[key].([]any)
		if len(branches) == 0 {
			continue
		}
		union := alg.none
		for _, branch := range branches {
			resolved, ok := resolvedNode(branch, defs)
			if !ok {
				union = alg.or(union, alg.any)
				continue
			}
			union = alg.or(union, read(resolved))
		}
		result = alg.and(result, union)
	}
	return result
}

var kindAlgebra = algebra[kindSet]{
	and:  func(a, b kindSet) kindSet { return a & b },
	or:   func(a, b kindSet) kindSet { return a | b },
	none: 0,
	any:  kindSetAll,
}

// typeNames reads a node's "type", a string or a union.
func typeNames(node map[string]any) []string {
	var types []string
	switch t := node["type"].(type) {
	case string:
		types = []string{t}
	case []any:
		for _, member := range t {
			if s, ok := member.(string); ok {
				types = append(types, s)
			}
		}
	}
	return types
}

// ownKinds is what a node's own "type" admits; without one, anything.
func ownKinds(node map[string]any) kindSet {
	types := typeNames(node)
	if len(types) == 0 {
		return kindSetAll
	}
	var kinds kindSet
	for _, t := range types {
		switch t {
		case "array":
			kinds |= kindSetArray
		case "object":
			kinds |= kindSetObject
		default:
			kinds |= kindSetSingle
		}
	}
	return kinds
}

// nodeKinds is what a node admits with its combinators.
func nodeKinds(node, defs map[string]any, depth int) kindSet {
	if depth > maxShapeDepth {
		return kindSetAll
	}
	return combined(node, defs, ownKinds(node), func(sub map[string]any) kindSet {
		return nodeKinds(sub, defs, depth+1)
	}, kindAlgebra)
}

// elementKinds is what an element of an array the node admits may be: its
// own items narrowed by each conjunct's, and by the union of the elements of
// the branches that admit an array. A node admitting no array has none.
func elementKinds(node, defs map[string]any, depth int) kindSet {
	if depth > maxShapeDepth {
		return kindSetAll
	}
	var own kindSet
	if ownKinds(node)&kindSetArray != 0 {
		own = kindSetAll
		if items, ok := resolvedNode(node["items"], defs); ok {
			own = nodeKinds(items, defs, depth+1)
		}
	}
	return combined(node, defs, own, func(sub map[string]any) kindSet {
		if nodeKinds(sub, defs, depth+1)&kindSetArray == 0 {
			return 0
		}
		return elementKinds(sub, defs, depth+1)
	}, kindAlgebra)
}

// declaresPrimitiveArrayNode reports whether a node admits an array of
// single values and nothing that holds members: no object, and no array
// whose elements could be objects or arrays. A union with "null" or another
// single type beside the array still declares it, and the items' single
// types may be given through combinators (anyOf string or integer, #696
// review R4-F1).
func declaresPrimitiveArrayNode(node, defs map[string]any) bool {
	kinds := nodeKinds(node, defs, 0)
	return kinds&kindSetObject == 0 && kinds&kindSetArray != 0 && elementKinds(node, defs, 0) == kindSetSingle
}

// indexBound bounds the array indices a record of the attribute may carry
// under some reading: holds is false where no instance of that reading holds
// one, and unbounded where the document does not bound them.
type indexBound struct {
	holds, unbounded bool
	n                int
}

var (
	noRecord   = indexBound{}
	anyIndices = indexBound{holds: true, unbounded: true}
)

func (b indexBound) and(o indexBound) indexBound {
	switch {
	case !b.holds || !o.holds:
		return noRecord
	case b.unbounded:
		return o
	case o.unbounded:
		return b
	}
	return indexBound{holds: true, n: min(b.n, o.n)}
}

func (b indexBound) or(o indexBound) indexBound {
	switch {
	case !b.holds:
		return o
	case !o.holds:
		return b
	case b.unbounded || o.unbounded:
		return anyIndices
	}
	return indexBound{holds: true, n: max(b.n, o.n)}
}

// inArray is b for an element of an array: one index more.
func (b indexBound) inArray() indexBound {
	if b.holds && !b.unbounded {
		b.n++
	}
	return b
}

// kindBounds is an indexBound for each kind an ancestor instance may be:
// an array, an object, a single value. Conjuncts and branches combine kind by
// kind, since one instance is of one kind.
type kindBounds [3]indexBound

var kindBoundsAlgebra = algebra[kindBounds]{
	and: func(a, b kindBounds) kindBounds {
		return kindBounds{a[0].and(b[0]), a[1].and(b[1]), a[2].and(b[2])}
	},
	or: func(a, b kindBounds) kindBounds {
		return kindBounds{a[0].or(b[0]), a[1].or(b[1]), a[2].or(b[2])}
	},
	none: kindBounds{noRecord, noRecord, noRecord},
	any:  kindBounds{anyIndices, anyIndices, anyIndices},
}

// indexBounder reads the bound of one attribute, whose own position takes
// leaf indices: one where it takes an array, none otherwise.
type indexBounder struct {
	defs map[string]any
	leaf int
}

// bound bounds the indices below node, segments the rest of the name.
func (r indexBounder) bound(node map[string]any, segments []string, depth int) indexBound {
	if len(segments) == 0 {
		return indexBound{holds: true, n: r.leaf}
	}
	b := r.ancestorBounds(node, segments, depth)
	return b[0].or(b[1]).or(b[2])
}

// ancestorBounds reads node, an ancestor of the attribute, kind by kind. An
// array's elements carry one index more, read by its items; an object's
// member is read by its properties; a single value holds no member. Items or
// a member the node leaves open admit any indices.
func (r indexBounder) ancestorBounds(node map[string]any, segments []string, depth int) kindBounds {
	if depth > maxShapeDepth {
		return kindBoundsAlgebra.any
	}
	kinds := ownKinds(node)
	own := kindBoundsAlgebra.none
	if kinds&kindSetArray != 0 {
		own[0] = anyIndices
		if items, ok := resolvedNode(node["items"], r.defs); ok {
			own[0] = r.bound(items, segments, depth+1).inArray()
		}
	}
	if kinds&kindSetObject != 0 {
		own[1] = anyIndices
		properties, _ := node["properties"].(map[string]any)
		if member, ok := resolvedNode(properties[segments[0]], r.defs); ok {
			own[1] = r.bound(member, segments[1:], depth+1)
		}
	}
	return combined(node, r.defs, own, func(sub map[string]any) kindBounds {
		return r.ancestorBounds(sub, segments, depth+1)
	}, kindBoundsAlgebra)
}

// boundedArrayIndices answers the most array indices a record of name may
// carry under any reading the document admits (forma.AttributeMetadata
// .MaxArrayIndices), arrayValued saying whether the attribute's own position
// takes an array, and false where the document does not bound them. The
// root is read as the object an entity's document is. A path no reading
// can hold a record at is left unbounded: nothing is judged there.
func boundedArrayIndices(root, defs map[string]any, name string, arrayValued bool) (int, bool) {
	if root == nil {
		return 0, false
	}
	r := indexBounder{defs: defs}
	if arrayValued {
		r.leaf = 1
	}
	b := r.ancestorBounds(root, strings.Split(name, "."), 0)[1]
	if !b.holds || b.unbounded {
		return 0, false
	}
	return b.n, true
}
