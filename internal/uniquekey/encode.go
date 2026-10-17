// Package uniquekey holds the backend-neutral parts of schema-declared
// unique keys (#651): the canonical key-value encoding, the key tables' DDL
// and the attribute-list hash a built key is recorded under.
package uniquekey

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// MaxInlineValueBytes is the longest key value stored as itself. A longer
// encoding is stored as DigestPrefix followed by the hex SHA-256 of the
// encoding, so the key table's primary key stays bounded whatever text a key
// holds.
const MaxInlineValueBytes = 512

// DigestPrefix starts a digested key value. An inline value always starts
// with a decimal length, so the two forms never collide.
const DigestPrefix = "sha256:"

// Component is one attribute's value in a key tuple, with the attribute's
// value type.
type Component struct {
	Type  forma.ValueType
	Value any
}

// Encode answers the canonical key value of a tuple. Each component is its
// type's canonical text, prefixed by that text's byte length and a colon, so
// ("ab", "c") and ("a", "bc") encode differently. The canonical texts are:
// integers in decimal, numerics in Go's shortest float64 form with -0 as 0,
// dates and datetimes as unix milliseconds, UUIDs in lowercase canonical
// form, booleans as t or f, and text as its exact bytes. A nil component is
// an error: a tuple with an absent attribute is not subject to the key, and
// the caller decides that before encoding.
func Encode(components []Component) (string, error) {
	var b strings.Builder
	for i, c := range components {
		text, err := canonical(c.Type, c.Value)
		if err != nil {
			return "", fmt.Errorf("component %d (%s): %w", i, c.Type, err)
		}
		b.WriteString(strconv.Itoa(len(text)))
		b.WriteByte(':')
		b.WriteString(text)
	}
	encoded := b.String()
	if len(encoded) <= MaxInlineValueBytes {
		return encoded, nil
	}
	sum := sha256.Sum256([]byte(encoded))
	return DigestPrefix + hex.EncodeToString(sum[:]), nil
}

// canonical answers one value's canonical text. It accepts the Go values a
// stored record decodes to, and the JSON spellings a caller might send, so
// equal typed values encode identically however they arrived.
func canonical(vt forma.ValueType, v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("value is absent")
	}
	switch vt {
	case forma.ValueTypeText:
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("text value is %T, not a string", v)
		}
		return s, nil
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		n, err := integerOf(v)
		if err != nil {
			return "", fmt.Errorf("%s value: %w", vt, err)
		}
		return strconv.FormatInt(n, 10), nil
	case forma.ValueTypeNumeric:
		return numericText(v)
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		t, err := instantOf(v)
		if err != nil {
			return "", fmt.Errorf("%s value: %w", vt, err)
		}
		return strconv.FormatInt(t.UnixMilli(), 10), nil
	case forma.ValueTypeUUID:
		return uuidText(v)
	case forma.ValueTypeBool:
		return boolText(v)
	default:
		return "", fmt.Errorf("value type %s cannot be part of a unique key", vt)
	}
}

func integerOf(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case float64:
		if n != math.Trunc(n) || n < math.MinInt64 || n >= math.MaxInt64 {
			return 0, fmt.Errorf("numeric value %v is not an integer", n)
		}
		return int64(n), nil
	case json.Number:
		return strconv.ParseInt(n.String(), 10, 64)
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("text %q is not an integer", n)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("integer value is %T", v)
	}
}

func numericText(v any) (string, error) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case string:
		parsed, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return "", fmt.Errorf("text %q is not a number", n)
		}
		f = parsed
	case json.Number:
		parsed, err := n.Float64()
		if err != nil {
			return "", fmt.Errorf("number %q: %w", n, err)
		}
		f = parsed
	default:
		i, err := integerOf(v)
		if err != nil {
			return "", fmt.Errorf("numeric value is %T", v)
		}
		f = float64(i)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("numeric value %v is not finite", f)
	}
	if f == 0 {
		f = 0 // -0 and 0 are one value
	}
	return strconv.FormatFloat(f, 'g', -1, 64), nil
}

func instantOf(v any) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.DateOnly} {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("text %q is not an RFC 3339 date-time or a date", t)
	default:
		return time.Time{}, fmt.Errorf("date value is %T", v)
	}
}

func uuidText(v any) (string, error) {
	switch u := v.(type) {
	case uuid.UUID:
		return u.String(), nil
	case string:
		parsed, err := uuid.Parse(u)
		if err != nil {
			return "", fmt.Errorf("text %q is not a UUID", u)
		}
		return parsed.String(), nil
	default:
		return "", fmt.Errorf("uuid value is %T", v)
	}
}

func boolText(v any) (string, error) {
	b, ok := v.(bool)
	if !ok {
		return "", fmt.Errorf("bool value is %T", v)
	}
	if b {
		return "t", nil
	}
	return "f", nil
}
