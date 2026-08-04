package schemavalidate

import (
	"encoding/json"
	"fmt"
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
func exactNumberInstance(v any) (any, error) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		if f, err := t.Float64(); err == nil {
			return f, nil
		}
		return nil, fmt.Errorf("numeric literal %q fits neither int64 nor float64", t.String())
	case map[string]any:
		for key, item := range t {
			converted, err := exactNumberInstance(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric value at property %q: %w", key, err)
			}
			t[key] = converted
		}
		return t, nil
	case []any:
		for i, item := range t {
			converted, err := exactNumberInstance(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric value at index %d: %w", i, err)
			}
			t[i] = converted
		}
		return t, nil
	default:
		return v, nil
	}
}
