package sqlgen

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A text operand takes one rule on every route of the federated clauses
// (#674 review): PostgreSQL text holds neither a NUL character nor invalid
// UTF-8, so a scalar, LIKE or list operand carrying one is refused before
// any statement instead of failing it (22021). username is bound to
// text_01, tag is stored in eav_data.

func TestUnrepresentableTextOperandIsRefusedOnEveryRoute(t *testing.T) {
	const (
		nul     = "text cannot contain a NUL character (U+0000)"
		badUTF8 = "text must be valid UTF-8"
	)
	cases := []struct{ value, want string }{
		{"equals:a\x00b", "invalid text value for '%s': " + nul},
		{"equals:\x00", "invalid text value for '%s': " + nul},
		{"starts_with:a\x00", "invalid text value for '%s': " + nul},
		{"contains:\x00", "invalid text value for '%s': " + nul},
		{"equals:a\xffb", "invalid text value for '%s': " + badUTF8},
		{"contains:\xc3", "invalid text value for '%s': " + badUTF8},
		{`in:["a\u0000b"]`, "element at index 0 of the 'in' list for text attribute '%s': " + nul},
		{`not_in:["ok","\u0000"]`, "element at index 1 of the 'not_in' list for text attribute '%s': " + nul},
	}
	for _, attr := range []string{"username", "tag"} {
		for _, tc := range cases {
			cond := charKv(attr, tc.value)
			want := fmt.Sprintf(tc.want, attr)
			t.Run(attr+"/"+tc.value, func(t *testing.T) {
				paramIndex := 1
				_, err := ToDualClauses(cond, "eav_table", 7, characterizationCache(), &paramIndex)
				requirePublished(t, err, want)
				_, _, err = BuildDuckClause(cond, characterizationCache())
				requirePublished(t, err, want)
			})
		}
	}
}

// Multi-byte text and a literal U+FFFD are valid UTF-8 and bind unchanged.
func TestRepresentableTextOperandBindsUnchanged(t *testing.T) {
	const operand = "café � 日本"
	paramIndex := 1
	got, err := ToDualClauses(charKv("username", "equals:"+operand), "eav_table", 7, characterizationCache(), &paramIndex)
	require.NoError(t, err)
	require.Equal(t, []any{operand}, got.PgMainArgs)
	require.Equal(t, []any{operand}, got.DuckArgs)

	paramIndex = 1
	got, err = ToDualClauses(charKv("tag", `in:["`+operand+`"]`), "eav_table", 7, characterizationCache(), &paramIndex)
	require.NoError(t, err)
	require.Equal(t, []any{int16(4), []string{operand}}, got.PgArgs)
}
