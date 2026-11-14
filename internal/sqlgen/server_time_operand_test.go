package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

const nowMs = "(floor(extract(epoch from now()) * 1000)::bigint + $2::bigint)"

func TestServerTimeOperand_RenderComparison(t *testing.T) {
	image := func(ms string) string {
		return `to_char((to_timestamp(floor(` + ms + ` / 1000.0)) + ((` + ms + ` % 1000 + 1000) % 1000) * interval '1 millisecond') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`
	}
	day := "(floor(" + nowMs + " / 86400000.0) * 86400000)::bigint"
	cases := []struct {
		name    string
		operand ServerTimeOperand
		sqlOp   string
		want    string
	}{
		{"datetime millis", ServerTimeOperand{}, "<=", "m.c <= " + nowMs},
		{"date truncates to the UTC day", ServerTimeOperand{Date: true}, "=", "m.c = " + day},
		// #842: the image holds the millisecond, so every operator compares
		// the column's text against the image of the same instant.
		{"rfc3339 lte", ServerTimeOperand{RFC3339: true}, "<=", "m.c <= " + image(nowMs)},
		{"rfc3339 gt", ServerTimeOperand{RFC3339: true}, ">", "m.c > " + image(nowMs)},
		{"rfc3339 lt", ServerTimeOperand{RFC3339: true}, "<", "m.c < " + image(nowMs)},
		{"rfc3339 gte", ServerTimeOperand{RFC3339: true}, ">=", "m.c >= " + image(nowMs)},
		{"rfc3339 equals", ServerTimeOperand{RFC3339: true}, "=", "m.c = " + image(nowMs)},
		{"rfc3339 not_equals", ServerTimeOperand{RFC3339: true}, "!=", "m.c != " + image(nowMs)},
		{"rfc3339 date uses the midnight image", ServerTimeOperand{RFC3339: true, Date: true}, "<",
			"m.c < " + image(day)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.operand.RenderComparison("m.c", tc.sqlOp, "$2"))
		})
	}
}

// The OLTP route renders $now through the EAV form; the offset is the one
// value bind, so the clause text is the same for every offset.
func TestToSQLClauses_ServerTimeOnEAV(t *testing.T) {
	cache := extendedCache()
	gen := NewSQLGenerator()
	render := func(value string) (string, []any) {
		idx := 0
		clause, args, err := gen.ToSQLClauses(charKv("seen", value), "eav_table", 7, cache, &idx)
		require.NoError(t, err)
		require.Equal(t, 2, idx)
		return clause, args
	}

	clause, args := render("lte:$now-30s")
	require.Equal(t, "EXISTS ("+charEavHead("$1")+" AND x.value_numeric <= "+nowMs+")", clause)
	require.Equal(t, []any{int16(10), int64(-30000)}, args)

	other, otherArgs := render("lte:$now+5m")
	require.Equal(t, clause, other, "the offset must not reach the clause text")
	require.Equal(t, []any{int16(10), int64(300000)}, otherArgs)

	idx := 0
	dateClause, dateArgs, err := gen.ToSQLClauses(charKv("due", "gte:$now"), "eav_table", 7, cache, &idx)
	require.NoError(t, err)
	require.Equal(t, "EXISTS ("+charEavHead("$1")+" AND x.value_numeric >= (floor("+nowMs+" / 86400000.0) * 86400000)::bigint)", dateClause)
	require.Equal(t, []any{int16(21), int64(0)}, dateArgs)
}

func TestToSQLClauses_ServerTimeRefusals(t *testing.T) {
	cache := extendedCache()
	cases := []struct {
		name, attr, value, wantErr string
	}{
		{"starts_with", "seen", "starts_with:$now",
			"operator 'starts_with' does not accept the $now operand on datetime attribute 'seen' (expected equals, not_equals, gt, gte, lt or lte)"},
		{"malformed offset", "seen", "gt:$now-1x", `invalid $now operand "$now-1x" for datetime attribute 'seen'`},
		{"sub-millisecond offset", "due", "lt:$now+10us", "the offset must be a whole number of milliseconds"},
		{"bigint keeps the literal", "total", "lte:$now", "invalid numeric value for 'total': $now"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := 0
			_, _, err := NewSQLGenerator().ToSQLClauses(charKv(tc.attr, tc.value), "eav_table", 7, cache, &idx)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok)
			require.Contains(t, msg, tc.wantErr)
		})
	}
}

func TestFederatedServerTimeError(t *testing.T) {
	cache := extendedCache()
	refused := []forma.Condition{
		charKv("seen", "lte:$now"),
		charKv("born", "equals:$now+24h"),
		charAnd(charKv("tag", "equals:x"), charOr(charKv("username", "is_null:true"), charKv("joined", "gt:$now-1h"))),
		// A malformed offset is refused for the route first: either refusal
		// names the attribute.
		charKv("seen", "gt:$now-1x"),
	}
	for _, cond := range refused {
		err := FederatedServerTimeError(cond, cache)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		msg, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok)
		require.Contains(t, msg, "is not supported on a federated query")
	}

	allowed := []forma.Condition{
		nil,
		charKv("username", "equals:$now"),          // text: a literal
		charKv("total", "lte:$now"),                // bigint: a literal
		charKv("ghost", "lte:$now"),                // unknown attribute: no value type
		charKv("seen", "in:[1]"),                   // set operand, not $now
		charKv("seen", "lte:2024-01-02T00:00:00Z"), // absolute timestamp
		charKv("seen", "unknown_op:$now"),          // left to the operator check
	}
	for _, cond := range allowed {
		require.NoError(t, FederatedServerTimeError(cond, cache), "%v", cond)
	}

	err := FederatedServerTimeError(charKv("joined", "lt:$now"), cache)
	msg, _ := forma.ResolvePublicMessage(err)
	require.Contains(t, msg, "the $now operand on datetime attribute 'joined'")
}
