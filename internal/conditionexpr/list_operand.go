package conditionexpr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/lychee-technology/forma"
)

// MaxListOperandElements bounds the JSON array an in / not_in leaf takes
// (#646). The list binds as one array parameter, so the bound protects the
// bind and the plan from caller-sized input, not the SQL text.
const MaxListOperandElements = 1000

// ParseListOperand decodes the operand of an in / not_in leaf: a JSON array
// of 1 to MaxListOperandElements strings, numbers or booleans. It returns
// each element's literal spelling, which the normalizer then coerces with
// the rules equals applies to the attribute's value type; a number keeps its
// exact JSON text, so a bigint past 2^53 is not rounded on the way.
//
// The array is decoded as a token stream that stops at the first element
// past the limit, so an oversized list is refused without decoding the rest.
//
// JSON text is UTF-8 (RFC 8259), and encoding/json would replace each
// invalid byte inside a string with U+FFFD rather than fail. The element
// would then match a stored U+FFFD the caller never sent, so invalid UTF-8
// is refused before decoding.
func ParseListOperand(attr, op, raw string) ([]string, error) {
	if !utf8.ValidString(raw) {
		return nil, listShapeError(attr, op, "the value is not valid UTF-8")
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return nil, listShapeError(attr, op, "the value is not a JSON array")
	}
	var literals []string
	for dec.More() {
		if len(literals) == MaxListOperandElements {
			return nil, listShapeError(attr, op, fmt.Sprintf("the array has more than %d values", MaxListOperandElements))
		}
		var element any
		if err := dec.Decode(&element); err != nil {
			return nil, listShapeError(attr, op, "the value is not a well-formed JSON array")
		}
		literal, err := listElementLiteral(attr, op, len(literals), element)
		if err != nil {
			return nil, err
		}
		literals = append(literals, literal)
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim(']') {
		return nil, listShapeError(attr, op, "the value is not a well-formed JSON array")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, listShapeError(attr, op, "the value has data after the JSON array")
	}
	if len(literals) == 0 {
		return nil, listShapeError(attr, op, "the array is empty")
	}
	return literals, nil
}

// listElementLiteral renders one decoded element as the literal an equals
// leaf would carry: a string as itself, a number as its JSON text, a bool as
// true/false.
func listElementLiteral(attr, op string, index int, element any) (string, error) {
	switch v := element.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case nil:
		return "", forma.InvalidInputf(
			"element at index %d of the '%s' list for attribute '%s' is null; the list takes 1 to %d non-null values",
			index, op, attr, MaxListOperandElements)
	default:
		return "", forma.InvalidInputf(
			"element at index %d of the '%s' list for attribute '%s' is a JSON %s; elements must be strings, numbers or booleans",
			index, op, attr, jsonKind(v))
	}
}

func jsonKind(v any) string {
	if _, ok := v.([]any); ok {
		return "array"
	}
	return "object"
}

func listShapeError(attr, op, problem string) error {
	return forma.InvalidInputf(
		"operator '%s' on attribute '%s' takes a JSON array of 1 to %d values; %s",
		op, attr, MaxListOperandElements, problem)
}
