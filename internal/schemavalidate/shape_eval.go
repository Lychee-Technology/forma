package schemavalidate

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// ShapeSchema is one schema's resolved container skeleton (shape.go). Like
// the Validator that holds it, it is read-only and safe for concurrent use.
//
// It answers one question, about one value at a time: may a single value sit
// at a given attribute inside given arrays? The write stores a single value
// as one record, with one array index per array it sits in, so this is the
// question a stored record raises, and the skeleton answers it for every
// construct the schema can use to declare those arrays.
type ShapeSchema struct {
	resolved *jsonschema.Resolved
}

// Shape answers the container skeleton of schemaID's schema, nil when the
// validator is unconfigured, does not know the schema, or the schema's
// skeleton did not resolve.
func (v *Validator) Shape(schemaID int16) *ShapeSchema {
	if v == nil {
		return nil
	}
	return v.shapes[schemaID]
}

// shapeOptions resolves skeletons: opts is resolveOptions with every
// document the loader reads replaced by its skeleton, so a cross-file $ref
// lands in the skeleton of the file it names, and load reads a document as
// it is, for the mapping of a reference into it (shape_refs.go).
type shapeOptions struct {
	opts *jsonschema.ResolveOptions
	load jsonschema.Loader
}

func newShapeOptions(dir string) (shapeOptions, error) {
	opts, err := resolveOptions(dir)
	if err != nil {
		return shapeOptions{}, err
	}
	load := opts.Loader
	opts.Loader = func(u *url.URL) (*jsonschema.Schema, error) {
		s, err := load(u)
		if err != nil {
			return nil, err
		}
		return newShaper(s, u.String(), load).document(s), nil
	}
	return shapeOptions{opts: opts, load: load}, nil
}

// resolveShape resolves the skeleton of one schema document, parsed afresh
// so the resolve of the schema itself shares nothing with it. A skeleton
// that does not resolve is answered as an error, and the schema keeps none:
// no case is known (shape_refs.go), and a skeleton that left a position
// unconstrained would admit an array at a single value there.
func resolveShape(document string, so shapeOptions) (*ShapeSchema, error) {
	var s jsonschema.Schema
	if err := json.Unmarshal([]byte(document), &s); err != nil {
		return nil, fmt.Errorf("failed to parse the schema for its container skeleton: %w", err)
	}
	resolved, err := newShaper(&s, so.opts.BaseURI, so.load).document(&s).Resolve(so.opts)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve the schema's container skeleton: %w", err)
	}
	return &ShapeSchema{resolved: resolved}, nil
}

// AdmitsArrangement reports whether the skeleton admits a single value at
// the attribute whose dotted name is split into segments, inside arrays[i]
// arrays around the value at segments[i], outermost first. It is judged on
// the document holding that one value and nothing else, every array with one
// element. The skeleton has no required and no item or property counts, so
// what the document lacks cannot refuse it: only the arrays and objects on
// the value's own path can. The error is the library's verdict, which names
// schema locations, for an operator.
func (s *ShapeSchema) AdmitsArrangement(segments []string, arrays []int) error {
	return s.resolved.Validate(arrangementWitness(segments, arrays))
}

// arrangementWitness answers the document holding null at segments, with
// arrays[i] arrays around the value at segments[i].
func arrangementWitness(segments []string, arrays []int) map[string]any {
	var value any
	for i := len(segments) - 1; i >= 0; i-- {
		for range arrays[i] {
			value = []any{value}
		}
		value = map[string]any{segments[i]: value}
	}
	doc, _ := value.(map[string]any)
	return doc
}

// maxIndexArrangements bounds the arrangements AdmitsIndices tries for one
// question.
const maxIndexArrangements = 4096

// AdmitsIndices reports whether the skeleton admits a single value of attr
// inside depth arrays arranged in any way along its path: the arrays a
// stored record of attr at depth indices may have been written through, one
// index per array, each around attr itself or around one of its ancestors.
// decided is false when there are more arrangements than it tries
// (maxIndexArrangements).
//
// The skeleton admits every document the schema does, so false is proof
// that no write the schema admits stores such a record.
func (s *ShapeSchema) AdmitsIndices(attr string, depth int) (admitted, decided bool) {
	segments := strings.Split(attr, ".")
	tried := 0
	admitted = forEachArrangement(len(segments), depth, func(arrays []int) bool {
		tried++
		if tried > maxIndexArrangements {
			return true
		}
		return s.AdmitsArrangement(segments, arrays) == nil
	})
	if tried > maxIndexArrangements {
		return false, false
	}
	return admitted, true
}

// forEachArrangement calls try with every way of placing depth arrays over
// parts positions, until try answers true, and reports whether one did.
func forEachArrangement(parts, depth int, try func(arrays []int) bool) bool {
	arrays := make([]int, parts)
	var place func(at, left int) bool
	place = func(at, left int) bool {
		if at == parts-1 {
			arrays[at] = left
			return try(arrays)
		}
		for n := 0; n <= left; n++ {
			arrays[at] = n
			if place(at+1, left-n) {
				return true
			}
		}
		return false
	}
	return place(0, depth)
}
