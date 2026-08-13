package schemavalidate

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
)

// nonFinitePath is one non-finite float found in a payload, with the dotted
// path it sits at.
type nonFinitePath struct {
	path  string
	value float64
}

// marshalRefusalError classifies a json.Marshal refusal of the caller's own
// payload (#322): caller input, so it carries forma.ErrInvalidInput and
// publishes.
//
// encoding/json's own text names the offending value ("json: unsupported
// value: NaN") but never where it sits, and an error the caller cannot locate
// in their payload is barely actionable. So the payload is walked here to
// derive the path. Only the first offender is named — alphabetically first, so
// the same payload always produces the same message — with a count of the
// rest, matching the precedent in transform's missing-required error.
//
// If the walk finds nothing the refusal came from something else (a cycle, a
// failing json.Marshaler, an unsupported type such as a channel or func), and
// the library's text is published unchanged: it is the only description of the
// fault that exists.
func marshalRefusalError(doc any, marshalErr error) error {
	found := nonFinitePaths(doc)
	if len(found) == 0 {
		return forma.InvalidInputf("payload cannot be encoded as JSON: %v", marshalErr)
	}
	more := ""
	if len(found) > 1 {
		more = fmt.Sprintf(" (and %d more)", len(found)-1)
	}
	return forma.InvalidInputf(
		"payload cannot be encoded as JSON: attribute %q holds non-finite number %v; a finite number is required%s",
		found[0].path, found[0].value, more)
}

// nonFinitePaths walks a document and returns every non-finite float in it,
// sorted by path. It exists for one error path — json.Marshal has just refused
// the payload — and is never on a successful write.
//
// Only the shapes a payload is made of are walked: map[string]any, []any,
// float64 and float32. Anything else either cannot hold the non-finite that
// made Marshal fail, or is exotic enough (a custom Marshaler, a struct) that
// the caller is better served by the library's own text, which the empty
// result falls back to. A non-finite at the document root is likewise left to
// the library: there is no attribute to name.
func nonFinitePaths(doc any) []nonFinitePath {
	var found []nonFinitePath
	collectNonFinite("", doc, &found)
	slices.SortFunc(found, func(a, b nonFinitePath) int {
		return strings.Compare(a.path, b.path)
	})
	return found
}

func collectNonFinite(path string, node any, found *[]nonFinitePath) {
	switch v := node.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			collectNonFinite(joinAttributePath(path, key), v[key], found)
		}
	case []any:
		for i, elem := range v {
			collectNonFinite(fmt.Sprintf("%s[%d]", path, i), elem, found)
		}
	case float64:
		appendIfNonFinite(path, v, found)
	case float32:
		appendIfNonFinite(path, float64(v), found)
	}
}

func appendIfNonFinite(path string, value float64, found *[]nonFinitePath) {
	if path == "" {
		return
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		*found = append(*found, nonFinitePath{path: path, value: value})
	}
}

func joinAttributePath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
