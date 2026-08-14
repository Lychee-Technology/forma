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
// If the walk comes back empty, the library's text is published unchanged: it
// is then the only description of the fault that exists. That covers a failing
// json.Marshaler and an unsupported type such as a channel or func — and a
// cycle, which reaches this branch because the walk is depth-capped and gives
// up on one rather than following it (see nonFinitePaths). Publishing "json:
// unsupported value: encountered a cycle" is the honest answer there: no single
// attribute is at fault.
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

// maxNonFiniteWalkDepth bounds the recursion below. Any real payload is far
// shallower — an HTTP-decoded document is bounded by encoding/json's own
// nesting limit long before this — so the cap only ever fires on a payload that
// is pathological, which on this code path means a cyclic one.
const maxNonFiniteWalkDepth = 1000

// nonFinitePaths walks a document and returns every non-finite float in it,
// sorted by path. It exists for one error path — json.Marshal has just refused
// the payload — and is never on a successful write.
//
// The walk is depth-capped, and exceeding the cap abandons it entirely: nil
// comes back, and the caller publishes the library's text. That is what makes
// a cyclic payload safe here. json.Marshal detects a cycle and refuses, so this
// walk runs precisely when a cycle is possible, and following one has no
// natural end — the cap is the end. Abandoning rather than truncating is
// deliberate: a partial walk cannot tell "no non-finite in this payload" from
// "stopped looking", and only the first answer may be published.
//
// Only the shapes a payload is made of are walked: map[string]any, []any,
// float64 and float32. Anything else either cannot hold the non-finite that
// made Marshal fail, or is exotic enough (a custom Marshaler, a struct) that
// the caller is better served by the library's own text, which the empty
// result falls back to. A non-finite at the document root is likewise left to
// the library: there is no attribute to name.
func nonFinitePaths(doc any) []nonFinitePath {
	var walker nonFiniteWalker
	walker.walk("", doc, 0)
	if walker.aborted {
		return nil
	}
	slices.SortFunc(walker.found, func(a, b nonFinitePath) int {
		return strings.Compare(a.path, b.path)
	})
	return walker.found
}

// nonFiniteWalker carries the walk's accumulated findings and its abort flag.
// Once aborted, every pending frame returns without doing further work.
type nonFiniteWalker struct {
	found   []nonFinitePath
	aborted bool
}

func (w *nonFiniteWalker) walk(path string, node any, depth int) {
	if w.aborted {
		return
	}
	if depth > maxNonFiniteWalkDepth {
		w.aborted = true
		return
	}

	switch v := node.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			w.walk(joinAttributePath(path, key), v[key], depth+1)
		}
	case []any:
		for i, elem := range v {
			w.walk(fmt.Sprintf("%s[%d]", path, i), elem, depth+1)
		}
	case float64:
		w.appendIfNonFinite(path, v)
	case float32:
		w.appendIfNonFinite(path, float64(v))
	}
}

func (w *nonFiniteWalker) appendIfNonFinite(path string, value float64) {
	if path == "" {
		return
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		w.found = append(w.found, nonFinitePath{path: path, value: value})
	}
}

func joinAttributePath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
