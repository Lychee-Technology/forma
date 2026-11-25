package transform

import (
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ValuesInArrays answers, for each attribute data writes a value of inside
// an array, the array indices of its first such value in the walk's order;
// nil when the walk refuses the document's nesting, which the write refuses
// at its own stage. It judges nothing: an update's intake uses it to refuse
// a unique key's attribute written inside the array form of an
// object-or-array union, whose object form the registry lets a key name
// (#696 review R4-F2), before any statement.
func ValuesInArrays(data map[string]any, cache forma.SchemaAttributeCache) map[string]string {
	entries, err := walkDocument(0, uuid.Nil, data, cache)
	if err != nil {
		return nil
	}
	var out map[string]string
	for _, entry := range entries {
		if !entry.claims() || entry.tagged.record.ArrayIndices == "" {
			continue
		}
		if _, seen := out[entry.name]; seen {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[entry.name] = entry.tagged.record.ArrayIndices
	}
	return out
}
