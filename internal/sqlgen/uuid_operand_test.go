package sqlgen

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// A uuid operand takes one rule on every route of the federated clauses
// (#674 review): the entity_main pushdown, the eav_data EXISTS and the DuckDB
// clause. owner is bound to uuid_01, ref is stored in eav_data.

func TestUUIDOperandBindsTheStoredSpellingOnEveryRoute(t *testing.T) {
	const stored = "6f1c3c3e-1111-4111-8111-111111111111"
	cases := []struct {
		attr       string
		pgMainArgs []any
		pgArgs     []any
	}{
		{"owner", []any{stored}, []any{int16(14), stored}},
		{"ref", nil, []any{int16(11), stored}},
	}
	for _, tc := range cases {
		t.Run(tc.attr, func(t *testing.T) {
			paramIndex := 1
			got, err := ToDualClauses(charKv(tc.attr, "equals:6F1C3C3E-1111-4111-8111-111111111111"), "eav_table", 7, characterizationCache(), &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.pgMainArgs, got.PgMainArgs)
			require.Equal(t, tc.pgArgs, got.PgArgs)
			require.Equal(t, []any{stored}, got.DuckArgs)
		})
	}
}

func TestMalformedUUIDOperandIsRefusedOnEveryRoute(t *testing.T) {
	for _, attr := range []string{"owner", "ref"} {
		for _, operand := range []string{"not-a-uuid", "6f1c"} {
			cond := charKv(attr, "equals:"+operand)
			want := fmt.Sprintf("invalid uuid value for '%s': %q is not a UUID", attr, operand)
			t.Run(attr+"/"+operand, func(t *testing.T) {
				paramIndex := 1
				_, err := ToDualClauses(cond, "eav_table", 7, characterizationCache(), &paramIndex)
				requirePublished(t, err, want)
				_, _, err = BuildDuckClause(cond, characterizationCache())
				requirePublished(t, err, want)
			})
		}
	}
}

// LIKE on a uuid column was pushed down as `m.uuid_01 LIKE ?`, which neither
// engine can evaluate; the whitelist now refuses it there as on eav_data.
func TestUUIDLikeIsRefusedOnTheMainPushdown(t *testing.T) {
	paramIndex := 1
	_, err := ToDualClauses(charKv("owner", "starts_with:6f"), "eav_table", 7, characterizationCache(), &paramIndex)
	require.ErrorContains(t, err, "pg main generation: ")
	requirePublished(t, err, "operator 'starts_with' only supported for text attributes, not 'uuid' (attribute 'owner')")
}

func requirePublished(t *testing.T, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	require.Equal(t, want, message)
}
