package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeLiteral(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: ""},
		{name: "no quotes unchanged", input: "host=h port=5432", expected: "host=h port=5432"},
		{name: "single quote doubled", input: "O'Brien", expected: "O''Brien"},
		{name: "every quote doubled", input: "a'b'c", expected: "a''b''c"},
		{name: "adjacent quotes each doubled", input: "x''y", expected: "x''''y"},
		{name: "quote-only string", input: "'", expected: "''"},
		// Backslash is an ordinary character in a plain '…' literal (PG
		// standard_conforming_strings and DuckDB agree): it must pass through
		// untouched while the quote beside it is still doubled.
		{name: "backslash passes through", input: `a\'b`, expected: `a\''b`},
		// The real payload shape: a pgdsn-quoted DSN embedded in
		// postgres_scan('…') / ATTACH '…' (#301, #290).
		{
			name:     "quoted DSN",
			input:    `host='h' password='p''w' dbname='d'`,
			expected: `host=''h'' password=''p''''w'' dbname=''d''`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, EscapeLiteral(tt.input))
		})
	}
}
