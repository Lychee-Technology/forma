package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestTryParseNumber(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect any
	}{
		{name: "int64", input: "42", expect: int64(42)},
		{name: "negative int64", input: "-7", expect: int64(-7)},
		{name: "float64", input: "3.14", expect: float64(3.14)},
		{name: "scientific float64", input: "1e3", expect: float64(1000)},
		{name: "non-numeric", input: "abc", expect: "abc"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := tryParseNumber(tt.input)
			switch exp := tt.expect.(type) {
			case int64:
				val, ok := got.(int64)
				assert.True(t, ok, "expected int64")
				assert.Equal(t, exp, val)
			case float64:
				val, ok := got.(float64)
				assert.True(t, ok, "expected float64")
				assert.InDelta(t, exp, val, 1e-9)
			case string:
				val, ok := got.(string)
				assert.True(t, ok, "expected string")
				assert.Equal(t, exp, val)
			default:
				t.Fatalf("unsupported expected type %T", exp)
			}
		})
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: ""},
		{name: "trim quotes and spaces", input: `  "a" . "b" .. "c"  `, expected: pgx.Identifier{"a", "b", "c"}.Sanitize()},
		{name: "mixed quoted and plain", input: `foo."Bar baz"`, expected: pgx.Identifier{"foo", "Bar baz"}.Sanitize()},
		{name: "all empty parts fallback", input: "...", expected: pgx.Identifier{"..."}.Sanitize()},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, sanitizeIdentifier(tt.input))
		})
	}
}

func TestToUUID(t *testing.T) {
	u := uuid.New()
	uPtr := uuid.MustParse(u.String())
	validStr := u.String()
	validStrPtr := &validStr
	raw16 := u[:]
	strBytes := []byte(validStr)
	invalidStr := "not-a-uuid"

	tests := []struct {
		name   string
		input  any
		expect uuid.UUID
		ok     bool
	}{
		{name: "uuid value", input: u, expect: u, ok: true},
		{name: "uuid pointer", input: &uPtr, expect: uPtr, ok: true},
		{name: "string valid", input: validStr, expect: u, ok: true},
		{name: "string pointer valid", input: validStrPtr, expect: u, ok: true},
		{name: "string invalid", input: invalidStr, expect: uuid.Nil, ok: false},
		{name: "string pointer nil", input: (*string)(nil), expect: uuid.Nil, ok: false},
		{name: "bytes raw16", input: raw16, expect: u, ok: true},
		{name: "bytes string form", input: strBytes, expect: u, ok: true},
		{name: "bytes invalid", input: []byte("bad-bytes"), expect: uuid.Nil, ok: false},
		{name: "unsupported type", input: 123, expect: uuid.Nil, ok: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toUUID(tt.input)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expect, got)
		})
	}
}
