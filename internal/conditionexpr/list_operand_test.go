package conditionexpr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func jsonList(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("%q", fmt.Sprintf("v%d", i))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func TestParseListOperandAcceptsBoundsAndElementKinds(t *testing.T) {
	got, err := ParseListOperand("status", OpIn, `["pending"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"pending"}, got)

	got, err = ParseListOperand("status", OpIn, jsonList(MaxListOperandElements))
	require.NoError(t, err)
	require.Len(t, got, MaxListOperandElements)

	// Numbers keep their exact JSON text (2^53+1 would round as a float64);
	// bools render as the literal an equals leaf would carry; duplicates stay.
	got, err = ParseListOperand("n", OpNotIn, ` [ 9007199254740993, 1.50, -2e3, true, false, "x", "x" ] `)
	require.NoError(t, err)
	require.Equal(t, []string{"9007199254740993", "1.50", "-2e3", "true", "false", "x", "x"}, got)
}

func TestParseListOperandRefusesShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"empty array", `[]`, "operator 'in' on attribute 'status' takes a JSON array of 1 to 1000 values; the array is empty"},
		{"over the limit", jsonList(MaxListOperandElements + 1), "takes a JSON array of 1 to 1000 values; the array has more than 1000 values"},
		{"scalar string", `pending`, "takes a JSON array of 1 to 1000 values; the value is not a JSON array"},
		{"json object", `{"a":1}`, "the value is not a JSON array"},
		{"unterminated", `["a",`, "the value is not a well-formed JSON array"},
		{"trailing data", `["a"] ["b"]`, "the value has data after the JSON array"},
		{"null element", `["a",null]`, "element at index 1 of the 'in' list for attribute 'status' is null; the list takes 1 to 1000 non-null values"},
		{"object element", `[{"a":1}]`, "element at index 0 of the 'in' list for attribute 'status' is a JSON object"},
		{"array element", `["a",["b"]]`, "element at index 1 of the 'in' list for attribute 'status' is a JSON array"},
		// encoding/json would decode this element as "a�b" (#674 review).
		{"invalid utf-8", "[\"a\xffb\"]", "takes a JSON array of 1 to 1000 values; the value is not valid UTF-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseListOperand("status", OpIn, tc.raw)
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "the refusal must carry a published message")
			require.Contains(t, msg, tc.want)
		})
	}
}

func TestToSQLOperatorAcceptsSetAndNullOperators(t *testing.T) {
	cases := map[string]string{
		"in":          SQLOpAny,
		"not_in":      SQLOpAll,
		"is_null":     SQLOpIsNull,
		"is_not_null": SQLOpIsNotNull,
		"IN":          SQLOpAny,
	}
	for op, want := range cases {
		got, err := ToSQLOperator(op, `["a"]`)
		require.NoError(t, err, op)
		require.Equal(t, want, got.SQLOperator, op)
		require.Equal(t, `["a"]`, got.Value, op)
	}
}
