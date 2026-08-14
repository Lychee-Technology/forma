package schemavalidate

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
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
// up on one rather than following it (see nonFinitePaths). There the library's
// text begins "json: unsupported value: encountered a cycle" and goes on to
// name the type it was found via; publishing it is the honest answer, because
// no single attribute is at fault.
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

// maxNonFiniteWalkDepth bounds the recursion below. Nothing upstream enforces a
// shallower limit: encoding/json's decoder refuses nesting past 10000, ten times
// this cap, so a decoded document can sit well beyond it.
//
// The cap therefore fires on two kinds of payload — a cyclic one, which has no
// bottom to reach, and any acyclic payload nested deeper than 1000. Both are
// treated identically: the walk abandons and the caller publishes the library's
// text. That is a safe degradation rather than a wrong answer, and both cases
// are pinned.
//
// What limits the cost of that degradation is the payload's provenance, not its
// depth: JSON has no syntax for a non-finite, so an HTTP request can never
// produce the refusal this walk exists to explain. Only a Go embedder can, which
// makes a message degraded by this cap an embedder-only outcome.
const maxNonFiniteWalkDepth = 1000

// nonFinitePaths walks a document and returns every non-finite float in it,
// sorted by path. It exists for one error path — json.Marshal has just refused
// the payload — and is never on a successful write.
//
// The walk is depth-capped, and exceeding the cap abandons it entirely: nil
// comes back, and the caller publishes the library's text. That is what makes
// a cyclic payload safe here — json.Marshal detects a cycle and refuses, so
// this walk runs precisely when a cycle is possible, and following one has no
// natural end, so the cap is the end. A merely deep acyclic payload hits the
// same cap and gets the same treatment (see maxNonFiniteWalkDepth).
//
// Abandoning rather than truncating is deliberate: a partial walk cannot tell
// "no non-finite in this payload" from "stopped looking", and only the first
// answer may be published. A payload with a shallow non-finite and a too-deep
// branch is what distinguishes them, and is pinned.
//
// Only the shapes a payload is made of are walked: map[string]any, []any,
// float64, float32, *float64, *float32 and json.Number. Every one of them can
// be what Marshal refused — a pointer at a non-finite gives "unsupported
// value: NaN", and a json.Number spelled "NaN"/"Inf"/"Infinity" gives "invalid
// number literal". A nil pointer is skipped: it marshals as null, so it is
// never the cause.
//
// The json.Number case counts a literal only when it parses cleanly to a
// non-finite. That gate excludes exactly the two kinds that must not be named:
// "1e400", which ParseFloat reports out of range but which is valid JSON
// grammar and marshals fine (so it never causes a refusal at all), and garbage
// such as "abc", which does cause one but is already named by the library's
// own "invalid number literal" text.
//
// Everything else yields nothing and falls back to that library text. That
// includes shapes which do refuse and which an embedder could plausibly hand
// in — a []float64 or map[string]float64 holding a non-finite refuses exactly
// like []any would, but is not walked, because type-switching every concrete
// numeric container is a losing game against a caller who can name any type.
// Structs, custom Marshalers and channels land here too. A non-finite at the
// document root is likewise left to the library: there is no attribute to name.
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
	case *float64:
		if v != nil {
			w.appendIfNonFinite(path, *v)
		}
	case *float32:
		if v != nil {
			w.appendIfNonFinite(path, float64(*v))
		}
	case json.Number:
		// A non-nil err covers both literals that must not be named: 1e400,
		// which is out of range but valid JSON that marshals fine, and garbage
		// such as "abc", which the library's own text already names.
		if parsed, err := strconv.ParseFloat(string(v), 64); err == nil {
			w.appendIfNonFinite(path, parsed)
		}
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
