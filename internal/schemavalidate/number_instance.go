package schemavalidate

import (
	"encoding/json"
	"fmt"

	"github.com/lychee-technology/forma"
)

// exactNumberInstance rewrites every json.Number in a decoded instance tree
// to int64 (when the literal is integral and in int64 range) or float64
// (otherwise), recursing through objects and arrays.
//
// The rewrite exists because jsonschema-go classifies a raw json.Number as
// type "string" (jsonType switches on reflect.Kind), while int64 and float64
// classify as "integer"/"number"; numeric constraint checks (minimum,
// maximum, multipleOf, enum, const) compare via big.Rat, so an int64 instance
// keeps full exactness above 2^53 where a float64 instance silently rounded
// (#282).
//
// It mutates the tree in place and must only receive Validate's private
// round-tripped copy, never the caller's document (#312 relies on the stored
// doc keeping its original values).
//
// A literal that fits neither int64 nor float64 — 1e400 — is the one way the
// rewrite fails, and it is caller input: httpapi decodes with UseNumber, so
// the literal reaches here with its exact text, and json.Marshal re-emits a
// json.Number verbatim, so the marshal step before this one never refuses it
// (#402). The failure therefore carries forma.ErrInvalidInput and publishes,
// naming the attribute path the same way marshalRefusalPaths does. jsonschema
// itself would accept the value — JSON Schema numbers are unbounded — but
// nothing downstream can hold it: transform rejects it on every numeric and
// bool attribute, and no instance shape jsonschema-go types as "number" can
// carry it exactly, so refusing it as input is the only truthful answer.
func exactNumberInstance(v any) (any, error) {
	return rewriteNumbers("", v)
}

// rewriteNumbers carries the dotted attribute path down the recursion so the
// leaf failure can name where the literal sits. The path spelling — "o.xs[1]"
// — matches marshalRefusalWalker's so the two published messages read alike.
func rewriteNumbers(path string, v any) (any, error) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, err := t.Float64()
		if err != nil {
			return nil, outOfRangeLiteralError(path, t, err)
		}
		return f, nil
	case map[string]any:
		for key, item := range t {
			converted, err := rewriteNumbers(joinAttributePath(path, key), item)
			if err != nil {
				return nil, err
			}
			t[key] = converted
		}
		return t, nil
	case []any:
		for i, item := range t {
			converted, err := rewriteNumbers(fmt.Sprintf("%s[%d]", path, i), item)
			if err != nil {
				return nil, err
			}
			t[i] = converted
		}
		return t, nil
	default:
		return v, nil
	}
}

// outOfRangeLiteralError builds the carrier for a literal outside float64
// range. The message is owned rather than forwarded from strconv (#453): the
// stdlib text is a function of the build toolchain, so publishing it would
// make the 4xx body drift on a go.mod bump. The strconv error rides along as
// operator detail so the log still names the parse that failed.
//
// A literal at the document root has no attribute to name and gets the
// literal alone, mirroring appendInvalidLiteral's root rule.
func outOfRangeLiteralError(path string, literal json.Number, parseErr error) error {
	if path == "" {
		return forma.WithOperatorDetail(forma.InvalidInputf(
			"numeric literal %q is outside the representable range; a number within float64 range is required",
			literal.String()), parseErr)
	}
	return forma.WithOperatorDetail(forma.InvalidInputf(
		"attribute %q holds numeric literal %q outside the representable range; a number within float64 range is required",
		path, literal.String()), parseErr)
}
