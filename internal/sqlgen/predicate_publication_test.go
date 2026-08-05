package sqlgen

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The predicate leaves publish exactly their own message (#313): the
// generator's "pg main generation"/"pg sql generation" phase wraps stay plain
// fmt.Errorf, so the internal phase name never reaches a response body.
func TestPredicateErrorsPublishLeafMessageOnly(t *testing.T) {
	cases := []struct {
		name string
		cond forma.Condition
		want string
	}{
		{"unsupported operator", charKv("username", "similar_to:alice"),
			"unsupported operator: similar_to"},
		{"unknown attribute", charKv("ghost", "equals:1"),
			"attribute not found in cache: ghost"},
		{"invalid numeric value", charKv("age", "gt:notanumber"),
			"invalid numeric value for 'age': notanumber"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 1
			_, err := ToDualClauses(tc.cond, "eav_table", 22, characterizationCache(), &paramIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput)

			var pub forma.PublicError
			require.True(t, errors.As(err, &pub), "predicate error publishes no client message: %v", err)
			require.Equal(t, tc.want, pub.PublicMessage())
			require.NotContains(t, pub.PublicMessage(), "generation")
		})
	}
}
