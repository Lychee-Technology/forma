package internal

import (
	"strings"

	"github.com/lychee-technology/forma"
)

// FilterAttributes filters a map of attributes based on the requested attribute paths.
// If attrs is nil or empty, returns the original attributes unchanged.
// Supports nested paths like "contact.name" or "contact.phone", including
// paths through arrays of objects ("items.sku"), which keep each element in
// its position (projectionNode.project), and paths a literal dotted key
// spells (projectionNode.member).
func FilterAttributes(attributes map[string]any, attrs []string) map[string]any {
	if len(attrs) == 0 {
		return attributes
	}

	root := &projectionNode{}
	for _, attrPath := range attrs {
		attrPath = strings.TrimSpace(attrPath)
		if attrPath == "" {
			continue
		}
		root.add(strings.Split(attrPath, "."))
	}
	return root.projectMembers(attributes)
}

// projectionNode is one segment of the requested attribute paths: whole when
// a path ends at it, so its value is kept in full, and children for the paths
// that continue beneath it. Every path is applied in one walk of the value,
// so paths sharing a container fill one copy of it.
type projectionNode struct {
	whole    bool
	children map[string]*projectionNode
}

func (n *projectionNode) add(segments []string) {
	for _, segment := range segments {
		if n.children == nil {
			n.children = make(map[string]*projectionNode)
		}
		child, ok := n.children[segment]
		if !ok {
			child = &projectionNode{}
			n.children[segment] = child
		}
		n = child
	}
	n.whole = true
}

// projectMembers returns the requested members of object, each projected by
// its node and kept under its own key. A member no requested path reaches,
// or that holds none of its node's paths, is left out.
func (n *projectionNode) projectMembers(object map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range object {
		child := n.member(key)
		if child == nil {
			continue
		}
		if projected, ok := child.project(value); ok {
			result[key] = projected
		}
	}
	return result
}

// member returns the node that projects the object member named key, or nil
// when no requested path reaches it. A key holding dots is a literal key the
// rebuild places beside an array of objects (#623,
// {"requirement":{"areas":[…],"areas.note":"n"}}). It stands for the path
// its segments spell, so "requirement.areas.note" selects it, and a path
// that ends partway through it, such as "requirement.areas", keeps it whole.
func (n *projectionNode) member(key string) *projectionNode {
	for {
		segment, rest, dotted := strings.Cut(key, ".")
		n = n.children[segment]
		if n == nil || n.whole || !dotted {
			return n
		}
		key = rest
	}
}

// project returns the part of value the node's paths select, and false when
// value holds none of them. An array is projected element by element in
// place, so "items.sku" over an array of objects answers each element's sku
// at that element's position. An element holding none of the paths reads as
// {}, as the rebuild reads an element no record reaches (#626), and an array
// none of whose elements holds one is left out, as an object is.
func (n *projectionNode) project(value any) (any, bool) {
	if n.whole {
		return value, true
	}
	switch v := value.(type) {
	case map[string]any:
		members := n.projectMembers(v)
		return members, len(members) > 0
	case []any:
		elements := make([]any, len(v))
		found := false
		for i, element := range v {
			projected, ok := n.project(element)
			if !ok {
				projected = map[string]any{}
			}
			elements[i] = projected
			found = found || ok
		}
		return elements, found
	default:
		return nil, false
	}
}

// FilterDataRecord applies attribute filtering to a forma.DataRecord.
// Returns a new DataRecord with filtered attributes.
func FilterDataRecord(record *forma.DataRecord, attrs []string) *forma.DataRecord {
	if record == nil {
		return nil
	}

	if len(attrs) == 0 {
		return record
	}

	return &forma.DataRecord{
		SchemaName: record.SchemaName,
		RowID:      record.RowID,
		Attributes: FilterAttributes(record.Attributes, attrs),
	}
}
