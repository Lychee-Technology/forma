package transform

import (
	"reflect"
	"strings"
)

// How SchemaShapes resolves an attribute's declaration in a schema document.
//
// A property, an array's items and the types a position admits can each be
// declared in several places at once: a subschema's own keywords, its $ref,
// and the branches of its allOf, anyOf and oneOf. Every one of them is read,
// not the first that answers. An anyOf or oneOf branch is an alternative the
// value may match: where one branch declares phones a string and another an
// array of strings, the array is valid, so the second branch's declaration
// counts as much as the first's, in either order (#696 review). An allOf
// branch constrains the value as well, so it is read the same way.
//
// A position is declared an array when any declaration reaching it admits
// one, a single value when every one says it is not an array, and
// unresolved otherwise: some declaration leaves its type open.

// nodeKey identifies a subschema the lookup has read: the object holding it
// and the file it was read from.
type nodeKey struct {
	at   uintptr
	file string
}

func keyOf(node schemaNode) nodeKey {
	return nodeKey{at: reflect.ValueOf(node.value).Pointer(), file: node.file}
}

// typeAnswer is a subschema's memoized types; a zero one is in progress, so
// a $ref cycle reads as no evidence.
type typeAnswer struct {
	types map[string]bool
	known bool
}

var combinatorKeywords = []string{"allOf", "anyOf", "oneOf"}

func (l *shapeLookup) declared(name string) Declaration {
	if l.root == nil {
		return Declaration{}
	}
	nodes := []schemaNode{{value: l.root}}
	segments := strings.Split(name, ".")
	ancestors := 0
	for _, segment := range segments[:len(segments)-1] {
		children := l.property(nodes, segment)
		if len(children) == 0 {
			return Declaration{}
		}
		nodes = children
		if l.shapeOf(children) == ShapeArray {
			// An array of objects around the attribute. Where an
			// alternative declares the parent an object instead, its
			// members are still looked up there, and the member is
			// counted under the array: the array's index is then the one
			// the write may meet.
			items := l.items(children)
			if len(items) == 0 {
				return Declaration{}
			}
			ancestors++
			nodes = append(items, children...)
		}
	}
	leaves := l.property(nodes, segments[len(segments)-1])
	if len(leaves) == 0 {
		return Declaration{}
	}
	return Declaration{ancestors: ancestors, shapes: l.levels(leaves)}
}

// levels answers the shape at each array level of the value the
// declarations nodes describe.
func (l *shapeLookup) levels(nodes []schemaNode) []DeclaredShape {
	var shapes []DeclaredShape
	for len(shapes) < maxShapeLevels {
		shape := l.shapeOf(nodes)
		shapes = append(shapes, shape)
		if shape != ShapeArray {
			return shapes
		}
		if nodes = l.items(nodes); len(nodes) == 0 {
			return append(shapes, ShapeUnresolved)
		}
	}
	return append(shapes, ShapeUnresolved)
}

// shapeOf answers the shape the declarations nodes give one position: an
// array where any of them admits one, a single value where every one says
// it is not an array, and unresolved otherwise.
func (l *shapeLookup) shapeOf(nodes []schemaNode) DeclaredShape {
	if len(nodes) == 0 {
		return ShapeUnresolved
	}
	shape := ShapeScalar
	for _, node := range nodes {
		types, known := l.types(node)
		switch {
		case types["array"]:
			return ShapeArray
		case !known:
			shape = ShapeUnresolved
		}
	}
	return shape
}

// property answers every subschema the declarations nodes give the member
// name: in their own "properties", or in those of their $ref or of a
// combinator branch.
func (l *shapeLookup) property(nodes []schemaNode, name string) []schemaNode {
	return l.collect(nodes, func(n schemaNode) (schemaNode, bool) {
		properties, _ := n.value["properties"].(map[string]any)
		child, ok := properties[name].(map[string]any)
		return schemaNode{value: child, file: n.file}, ok
	})
}

// items answers every subschema the declarations nodes give an array's
// elements.
func (l *shapeLookup) items(nodes []schemaNode) []schemaNode {
	return l.collect(nodes, func(n schemaNode) (schemaNode, bool) {
		items, ok := n.value["items"].(map[string]any)
		return schemaNode{value: items, file: n.file}, ok
	})
}

// collect answers own of each of nodes and of every subschema their $ref
// and combinator branches name, transitively, each subschema read once.
func (l *shapeLookup) collect(nodes []schemaNode, own func(schemaNode) (schemaNode, bool)) []schemaNode {
	var found []schemaNode
	seen := make(map[nodeKey]bool)
	queue := append([]schemaNode(nil), nodes...)
	for len(queue) > 0 && len(seen) < maxShapeNodes {
		node := queue[0]
		queue = queue[1:]
		key := keyOf(node)
		if seen[key] {
			continue
		}
		seen[key] = true
		if child, ok := own(node); ok {
			found = append(found, child)
		}
		if target, ok := l.ref(node); ok {
			queue = append(queue, target)
		}
		for _, keyword := range combinatorKeywords {
			queue = append(queue, l.branches(node, keyword)...)
		}
	}
	return found
}

// types answers the JSON types node admits other than null, and whether the
// document says: its "type" (a name or a union), its $ref's, its allOf
// branches', and the union of its anyOf or oneOf branches' when every branch
// says. Only when none of them says, an "items" declares an array and a
// "properties" an object, as the registry's parser reads them. Every
// constraint's types are joined, so an array is admitted wherever any of
// them names one. Each subschema's answer is read once.
func (l *shapeLookup) types(node schemaNode) (map[string]bool, bool) {
	key := keyOf(node)
	if answer, seen := l.typeMemo[key]; seen {
		return answer.types, answer.known
	}
	if l.typeMemo == nil {
		l.typeMemo = make(map[nodeKey]typeAnswer)
	}
	if len(l.typeMemo) >= maxShapeNodes {
		return nil, false
	}
	l.typeMemo[key] = typeAnswer{}
	types, known := l.ownTypes(node)
	l.typeMemo[key] = typeAnswer{types: types, known: known}
	return types, known
}

func (l *shapeLookup) ownTypes(node schemaNode) (map[string]bool, bool) {
	out, known := map[string]bool{}, false
	add := func(types map[string]bool, ok bool) {
		if ok {
			known = true
			for t := range types {
				out[t] = true
			}
		}
	}
	add(typeKeyword(node.value["type"]))
	if target, ok := l.ref(node); ok {
		add(l.types(target))
	}
	for _, branch := range l.branches(node, "allOf") {
		add(l.types(branch))
	}
	for _, keyword := range []string{"anyOf", "oneOf"} {
		add(l.unionOf(l.branches(node, keyword)))
	}
	if !known {
		_, hasItems := node.value["items"].(map[string]any)
		_, hasProperties := node.value["properties"].(map[string]any)
		add(map[string]bool{"array": true}, hasItems)
		add(map[string]bool{"object": true}, !hasItems && hasProperties)
	}
	delete(out, "null")
	return out, known && len(out) > 0
}

// unionOf joins the types of branches, which say only when every one does.
func (l *shapeLookup) unionOf(branches []schemaNode) (map[string]bool, bool) {
	if len(branches) == 0 {
		return nil, false
	}
	out := map[string]bool{}
	for _, branch := range branches {
		types, ok := l.types(branch)
		if !ok {
			return nil, false
		}
		for t := range types {
			out[t] = true
		}
	}
	return out, true
}

// typeKeyword reads a "type" keyword: a type name or an array of them.
func typeKeyword(value any) (map[string]bool, bool) {
	switch v := value.(type) {
	case string:
		return map[string]bool{v: true}, v != ""
	case []any:
		out := map[string]bool{}
		for _, item := range v {
			name, ok := item.(string)
			if !ok {
				return nil, false
			}
			out[name] = true
		}
		return out, len(out) > 0
	}
	return nil, false
}
