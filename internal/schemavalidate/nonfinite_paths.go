package schemavalidate

import (
	"encoding/json"
	"errors"
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

// invalidNumberLiteral is one json.Number found in a payload whose text
// json.Marshal refuses as JSON number grammar, with the dotted path it sits at.
type invalidNumberLiteral struct {
	path    string
	literal string
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
// rest, matching the precedent in transform's missing-required error. When
// both finding kinds are present the non-finite message wins: either is
// truthful (each would refuse on its own), and a fixed priority keeps the
// published message deterministic.
//
// The invalid-literal message is owned rather than forwarded (#453):
// forwarding stdlib text made the published 4xx body a function of the build
// toolchain — Go 1.27 rewords it — which is a silent API-contract change
// triggered by nothing but a go.mod bump. The raw library error rides along
// as operator detail, so the log keeps it while the body never sees it.
//
// If the walk comes back empty, the refusal is explained by its kind instead
// (see unexplainedRefusalError): the library's text is published only when it
// is prose about a value, and otherwise an owned message names the Go type
// while the library's text stays operator-only.
func marshalRefusalError(doc any, marshalErr error) error {
	nonFinite, invalidLiterals := marshalRefusalPaths(doc)
	if len(nonFinite) > 0 {
		return forma.InvalidInputf(
			"payload cannot be encoded as JSON: attribute %q holds non-finite number %v; a finite value is required%s",
			nonFinite[0].path, nonFinite[0].value, moreSuffix(len(nonFinite)))
	}
	if len(invalidLiterals) > 0 {
		return forma.WithOperatorDetail(forma.InvalidInputf(
			"payload cannot be encoded as JSON: attribute %q holds invalid number literal %q; a valid JSON number is required%s",
			invalidLiterals[0].path, invalidLiterals[0].literal, moreSuffix(len(invalidLiterals))),
			marshalErr)
	}
	return unexplainedRefusalError(marshalErr)
}

// unexplainedRefusalError classifies a refusal the walk could not locate, by
// the concrete error encoding/json returned (#402). Every branch is HTTP
// unreachable — a decoded body holds only maps, slices, strings, bools,
// nils and grammar-valid json.Numbers — so this is embedder-facing prose, but
// it still crosses the public transport and is held to the same width rule.
//
// A *json.MarshalerError carries an embedder type's own MarshalJSON error:
// text this repo neither wrote nor reviewed, which must not be forwarded
// into a body. A *json.UnsupportedTypeError is library-authored, but its
// wording is a function of the toolchain (#453). Both publish an owned
// message naming the Go type — the only fact the caller can act on — and
// keep the library's text as operator detail.
//
// A *json.UnsupportedValueError is the one kind whose text publishes: a
// non-finite in a shape the walk does not cover ([]float64, a struct field),
// or a cycle, which reaches here because the walk is depth-capped and gives
// up on one rather than following it (see marshalRefusalPaths). Its text is
// prose about the value — "encountered a cycle via ..." names the path the
// cycle was found through — and no single attribute is at fault, so the
// library's text is the only truthful description and is published unchanged.
//
// Anything else closes by default: an owned message that names nothing, with
// the library's text as operator detail. The default toolchain produces no
// fourth kind today, but the GOEXPERIMENT=jsonv2 shim's transformMarshalError
// already falls through to a *json.SyntaxError (a malformed json.RawMessage),
// and a kind this build has not reviewed must not widen the body just by
// existing (#453).
func unexplainedRefusalError(marshalErr error) error {
	var marshalerErr *json.MarshalerError
	if errors.As(marshalErr, &marshalerErr) {
		return forma.WithOperatorDetail(forma.InvalidInputf(
			"payload cannot be encoded as JSON: the MarshalJSON method of Go type %s refused the value",
			marshalerErr.Type), marshalErr)
	}
	var typeErr *json.UnsupportedTypeError
	if errors.As(marshalErr, &typeErr) {
		return forma.WithOperatorDetail(forma.InvalidInputf(
			"payload cannot be encoded as JSON: Go type %s is not encodable", typeErr.Type), marshalErr)
	}
	var valueErr *json.UnsupportedValueError
	if errors.As(marshalErr, &valueErr) {
		return forma.InvalidInputf("payload cannot be encoded as JSON: %v", marshalErr)
	}
	return forma.WithOperatorDetail(forma.InvalidInputf(
		"payload cannot be encoded as JSON; no offending attribute could be identified"), marshalErr)
}

// moreSuffix renders the " (and N more)" tail shared by both owned messages:
// only the alphabetically first offender is named, the rest are counted.
func moreSuffix(n int) string {
	if n > 1 {
		return fmt.Sprintf(" (and %d more)", n-1)
	}
	return ""
}

// maxMarshalRefusalWalkDepth bounds the recursion below. Nothing upstream enforces a
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
// depth: JSON has no syntax for a non-finite, and a decoded json.Number is
// grammar-valid by construction, so an HTTP request can never produce either
// refusal this walk exists to explain. Only a Go embedder can, which makes a
// message degraded by this cap an embedder-only outcome.
const maxMarshalRefusalWalkDepth = 1000

// marshalRefusalPaths walks a document and returns every finding the walk can
// name — non-finite floats and invalid json.Number literals (#453) — each
// sorted by path. It exists for one error path — json.Marshal has just refused
// the payload — and is never on a successful write.
//
// The walk is depth-capped, and exceeding the cap abandons it entirely: nil
// comes back for both kinds, and the caller classifies the refusal by its
// kind instead (unexplainedRefusalError; a cycle publishes the library's
// text). That
// is what makes a cyclic payload safe here — json.Marshal detects a cycle and
// refuses, so this walk runs precisely when a cycle is possible, and following
// one has no natural end, so the cap is the end. A merely deep acyclic payload
// hits the same cap and gets the same treatment (see maxMarshalRefusalWalkDepth).
//
// Abandoning rather than truncating is deliberate: a partial walk cannot tell
// "nothing wrong in this payload" from "stopped looking", and only the first
// answer may be published. A payload with a shallow finding and a too-deep
// branch is what distinguishes them, and is pinned.
//
// Only the shapes a payload is made of are walked: map[string]any, []any,
// float64, float32, *float64, *float32 and json.Number. Every one of them can
// be what Marshal refused — a pointer at a non-finite gives "unsupported
// value: NaN", and a json.Number refuses either as a non-finite spelling
// ("NaN"/"Inf"/"Infinity") or as garbage that is not JSON number grammar; the
// two gates on that case are documented at the case itself.
//
// Everything else yields nothing and falls back to unexplainedRefusalError. That
// includes shapes which do refuse and which an embedder could plausibly hand
// in — a []float64 or map[string]float64 holding a non-finite refuses exactly
// like []any would, but is not walked, because type-switching every concrete
// numeric container is a losing game against a caller who can name any type.
// Structs, custom Marshalers and channels land here too. A fault at the
// document root is likewise left to the library: there is no attribute to name.
func marshalRefusalPaths(doc any) ([]nonFinitePath, []invalidNumberLiteral) {
	var walker marshalRefusalWalker
	walker.walk("", doc, 0)
	if walker.aborted {
		return nil, nil
	}
	slices.SortFunc(walker.nonFinite, func(a, b nonFinitePath) int {
		return strings.Compare(a.path, b.path)
	})
	slices.SortFunc(walker.invalid, func(a, b invalidNumberLiteral) int {
		return strings.Compare(a.path, b.path)
	})
	return walker.nonFinite, walker.invalid
}

// marshalRefusalWalker carries the walk's accumulated findings — one slice per
// finding kind — and its abort flag. Once aborted, every pending frame returns
// without doing further work.
type marshalRefusalWalker struct {
	nonFinite []nonFinitePath
	invalid   []invalidNumberLiteral
	aborted   bool
}

func (w *marshalRefusalWalker) walk(path string, node any, depth int) {
	if w.aborted {
		return
	}
	if depth > maxMarshalRefusalWalkDepth {
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
		// Two gates (#453). A literal that parses cleanly to a non-finite
		// ("NaN", "Infinity") is a non-finite finding. Anything else is probed
		// against json.Marshal itself: the literal is invalid exactly when the
		// library refuses it, which keeps this classification immune to grammar
		// drift between toolchains. The probe is also what excludes 1e400 —
		// ParseFloat reports it out of range, but it is valid JSON grammar and
		// marshals fine, so it never causes a refusal and must not be named.
		if parsed, err := strconv.ParseFloat(string(v), 64); err == nil &&
			(math.IsNaN(parsed) || math.IsInf(parsed, 0)) {
			w.appendIfNonFinite(path, parsed)
			return
		}
		if _, err := json.Marshal(v); err != nil {
			w.appendInvalidLiteral(path, string(v))
		}
	}
}

func (w *marshalRefusalWalker) appendIfNonFinite(path string, value float64) {
	if path == "" {
		return
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		w.nonFinite = append(w.nonFinite, nonFinitePath{path: path, value: value})
	}
}

// appendInvalidLiteral mirrors appendIfNonFinite's root rule: a fault at the
// document root has no attribute to name and falls back to
// unexplainedRefusalError.
func (w *marshalRefusalWalker) appendInvalidLiteral(path, literal string) {
	if path == "" {
		return
	}
	w.invalid = append(w.invalid, invalidNumberLiteral{path: path, literal: literal})
}

func joinAttributePath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
