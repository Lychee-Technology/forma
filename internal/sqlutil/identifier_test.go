package sqlutil

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

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
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, SanitizeIdentifier(tt.input))
		})
	}
}
