package sqlutil

import (
	"strings"
	"testing"
	"unicode/utf8"

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

func TestSuffixedIdentifier(t *testing.T) {
	tests := []struct{ table, want string }{
		{table: "entity_main", want: "entity_main_x"},
		{table: "tenant.entity_main", want: "tenant.entity_main_x"},
		{table: `"tenant"."Entity Main"`, want: `"tenant"."Entity Main_x"`},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, SuffixedIdentifier(tt.table, "_x"), tt.table)
	}
}

// A derived name past PostgreSQL's 63-byte limit would be truncated by the
// server: from a 63-byte table name on, to the table's own name. Every
// derived name fits, keeps its suffix and is not its table, and a name that
// already fits keeps its plain form.
func TestSuffixedIdentifierFitsAnIdentifier(t *testing.T) {
	names := []string{strings.Repeat("é", 30)} // 60 bytes: a cut must not split a character
	for n := 40; n <= 80; n++ {
		names = append(names, strings.Repeat("m", n))
	}
	for _, table := range names {
		for _, suffix := range []string{"_x", "_row_guard", "_unique_key_state"} {
			name := SuffixedIdentifier(table, suffix)
			assert.LessOrEqual(t, len(name), MaxIdentifierLen, name)
			assert.True(t, utf8.ValidString(name), name)
			assert.True(t, strings.HasSuffix(name, suffix), name)
			assert.NotEqual(t, table[:min(len(table), MaxIdentifierLen)], name, "%d-byte table derives its own name", len(table))
			if len(table)+len(suffix) <= MaxIdentifierLen {
				assert.Equal(t, table+suffix, name)
			}
		}
	}
}

// Every spelling of one table derives the same last part, so init-db naming
// entity_main bare and the factory qualifying it name one derived table.
func TestSuffixedIdentifierIgnoresQualifierAndQuotes(t *testing.T) {
	long := strings.Repeat("m", 70)
	assert.Equal(t, "public."+SuffixedIdentifier(long, "_x"), SuffixedIdentifier("public."+long, "_x"))
	assert.Equal(t, `"app"."`+SuffixedIdentifier(long, "_x")+`"`, SuffixedIdentifier(`"app"."`+long+`"`, "_x"))
	assert.Equal(t, SuffixedIdentifier(long, "_x"), SuffixedName(`"app"."`+long+`"`, "_x"))
	assert.NotEqual(t, SuffixedIdentifier(long+"a", "_x"), SuffixedIdentifier(long+"b", "_x"))
}
