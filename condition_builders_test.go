package forma

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConditionBuildersRenderExactLeaves(t *testing.T) {
	id := uuid.MustParse("6f1c3c3e-1111-4111-8111-111111111111")
	cases := []struct {
		name string
		got  *KvCondition
		want KvCondition
	}{
		{"in strings", In("status", "pending", "failed"), KvCondition{Attr: "status", Value: `in:["pending","failed"]`}},
		{"in mixed scalars", In("n", 1, 2.5, true, id), KvCondition{Attr: "n", Value: `in:[1,2.5,true,"6f1c3c3e-1111-4111-8111-111111111111"]`}},
		{"in with no values", In("status"), KvCondition{Attr: "status", Value: `in:[]`}},
		{"not_in", NotIn("status", "published", "dead_letter"), KvCondition{Attr: "status", Value: `not_in:["published","dead_letter"]`}},
		{"is_null", IsNull("locked_until"), KvCondition{Attr: "locked_until", Value: "is_null:true"}},
		{"is_not_null", IsNotNull("locked_until"), KvCondition{Attr: "locked_until", Value: "is_not_null:true"}},
		{"now bare", Now("locked_until", FilterLessEq, 0), KvCondition{Attr: "locked_until", Value: "lte:$now"}},
		{"now minus", Now("seen", FilterGreaterThan, -30*time.Second), KvCondition{Attr: "seen", Value: "gt:$now-30s"}},
		{"now plus", Now("due", FilterLessThan, 5*time.Minute), KvCondition{Attr: "due", Value: "lt:$now+5m0s"}},
		{"now millis", Now("due", FilterEquals, 1500*time.Millisecond), KvCondition{Attr: "due", Value: "equals:$now+1.5s"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, *tc.got)
		})
	}
}

// A value encoding/json refuses must not panic the caller: the leaf carries
// a non-array operand, which the server refuses as invalid input.
func TestInWithUnencodableValueYieldsNonArrayOperand(t *testing.T) {
	got := In("score", math.NaN())
	require.Equal(t, "score", got.Attr)
	require.Regexp(t, `^in:"unencodable list operand: json: unsupported value: NaN"$`, got.Value)
}
