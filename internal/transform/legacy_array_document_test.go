package transform

import (
	"encoding/json"
	"slices"
	"strings"
)

// legacyArrayDocument answers a schema document that declares each of arrays
// an array of strings, as an attributes file before #204 recorded such a
// property under its element type, and each of objectArrays an array of
// objects; every other path it reaches is an object. The shape rule admits
// an array at a non-list attribute only where the document declares one
// (#696), so a fixture writing a pre-#204 array serves the document that
// declares it.
func legacyArrayDocument(objectArrays []string, arrays ...string) string {
	root := map[string]any{"type": "object", "properties": map[string]any{}}
	for _, path := range append(append([]string(nil), objectArrays...), arrays...) {
		node := root
		segments := strings.Split(path, ".")
		for i, segment := range segments {
			properties := node["properties"].(map[string]any)
			prefix := strings.Join(segments[:i+1], ".")
			child, _ := properties[segment].(map[string]any)
			switch {
			case slices.Contains(arrays, prefix):
				properties[segment] = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
				continue
			case child == nil && slices.Contains(objectArrays, prefix):
				child = map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{}}}
			case child == nil:
				child = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			properties[segment] = child
			if items, ok := child["items"].(map[string]any); ok {
				node = items
			} else {
				node = child
			}
		}
	}
	text, err := json.Marshal(root)
	if err != nil {
		panic(err)
	}
	return string(text)
}
