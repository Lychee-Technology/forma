package sqlgen

import (
	"fmt"
	"strconv"
	"testing"
	"time"

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

// A $now leaf that reaches the DuckDB payload unresolved is an internal
// error, not caller input: a federated Query pins every $now leaf before
// routing (#715), so only a direct engine caller that skipped
// ResolveServerTime lands here. A malformed $now leaf keeps its own refusal.
func TestToDualClauses_UnresolvedServerTime(t *testing.T) {
	cache := extendedCache()
	for _, cond := range []forma.Condition{charKv("seen", "lte:$now-30s"), charKv("born", "equals:$now")} {
		idx := 0
		_, err := ToDualClauses(cond, "eav_table", 7, cache, &idx)
		require.Error(t, err)
		require.NotErrorIs(t, err, forma.ErrInvalidInput)
		require.Contains(t, err.Error(), "reached a duckdb pass unresolved")
	}

	idx := 0
	_, err := ToDualClauses(charKv("seen", "gt:$now-1x"), "eav_table", 7, cache, &idx)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, _ := forma.ResolvePublicMessage(err)
	require.Contains(t, msg, `invalid $now operand "$now-1x" for datetime attribute 'seen'`)
}

// resolvedNowMs is a millisecond instant off any whole second, on
// 2025-11-17 UTC.
const resolvedNowMs = int64(1_763_351_234_567)

// TestResolveServerTime pins the rewrite #715 runs before routing a federated
// query: each well-formed $now leaf on a date or datetime attribute becomes
// the epoch-millisecond literal of the shifted instant, under the same
// operator; a date attribute takes the start of the instant's UTC day, the
// truncation RenderComparison applies. Every other leaf, a malformed $now
// leaf included, is kept as it is, and the input tree is not modified.
func TestResolveServerTime(t *testing.T) {
	cache := extendedCache()
	ms := func(v int64) string { return strconv.FormatInt(v, 10) }
	day := func(v int64) int64 { return v - v%86_400_000 }
	cases := []struct {
		name string
		in   forma.Condition
		want forma.Condition
	}{
		{"datetime", charKv("seen", "lte:$now"), charKv("seen", "lte:"+ms(resolvedNowMs))},
		{"datetime offset", charKv("seen", "gt:$now-30s"), charKv("seen", "gt:"+ms(resolvedNowMs-30_000))},
		{"bare $now is equals", charKv("seen", "$now+1ms"), charKv("seen", "equals:"+ms(resolvedNowMs+1))},
		{"date truncates", charKv("due", "gte:$now+24h"), charKv("due", "gte:"+ms(day(resolvedNowMs+86_400_000)))},
		{"main-bound date", charKv("born", "not_equals:$now"), charKv("born", "not_equals:"+ms(day(resolvedNowMs)))},
		{"rfc3339-bound datetime", charKv("joined", "lt:$now-1500ms"), charKv("joined", "lt:"+ms(resolvedNowMs-1500))},
		{"nested", charAnd(charKv("tag", "equals:x"), charOr(charKv("username", "is_null:true"), charKv("joined", "gte:$now"))),
			charAnd(charKv("tag", "equals:x"), charOr(charKv("username", "is_null:true"), charKv("joined", "gte:"+ms(resolvedNowMs))))},
		{"text keeps the literal", charKv("username", "equals:$now"), charKv("username", "equals:$now")},
		{"bigint keeps the literal", charKv("total", "lte:$now"), charKv("total", "lte:$now")},
		{"unknown attribute", charKv("ghost", "lte:$now"), charKv("ghost", "lte:$now")},
		{"malformed offset", charKv("seen", "gt:$now-1x"), charKv("seen", "gt:$now-1x")},
		{"operator refusing $now", charKv("seen", "starts_with:$now"), charKv("seen", "starts_with:$now")},
		{"absolute timestamp", charKv("seen", "lte:2024-01-02T00:00:00Z"), charKv("seen", "lte:2024-01-02T00:00:00Z")},
		{"nil", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := fmt.Sprint(tc.in)
			require.Equal(t, tc.want, ResolveServerTime(tc.in, cache, resolvedNowMs))
			require.Equal(t, before, fmt.Sprint(tc.in), "the input must not be modified")
		})
	}

	require.True(t, HasServerTime(charAnd(charKv("tag", "equals:x"), charKv("seen", "lte:$now")), cache))
	require.False(t, HasServerTime(charKv("seen", "gt:$now-1x"), cache), "a malformed leaf needs no clock")
	require.False(t, HasServerTime(charKv("username", "equals:$now"), cache))
	require.False(t, HasServerTime(nil, cache))
}

// TestResolveServerTime_DayBeforeTheEpoch: the UTC day of an instant before
// 1970 floors like PostgreSQL's floor(ms / 86400000.0), not toward zero.
func TestResolveServerTime_DayBeforeTheEpoch(t *testing.T) {
	got := ResolveServerTime(charKv("due", "lte:$now"), extendedCache(), -1)
	require.Equal(t, charKv("due", "lte:-86400000"), got)
}

// TestResolveServerTime_BothLegsBindTheInstant is the dialect half of the
// parity #715 needs: after the rewrite the PostgreSQL and the DuckDB
// clauses of every scalar operator bind the same instant, the one $now
// names, on an EAV datetime and date, a unix_ms-bound date and an
// rfc3339-bound datetime, with no now() left in either. The rfc3339 column
// compares the instant's canonical image on PostgreSQL (#842).
func TestResolveServerTime_BothLegsBindTheInstant(t *testing.T) {
	cache := extendedCache()
	shifted := resolvedNowMs - 90_000
	midnight := shifted - shifted%86_400_000
	image := time.UnixMilli(shifted).UTC().Format("2006-01-02T15:04:05.000Z")
	attrs := []struct {
		attr    string
		instant int64
		pgArg   any
	}{
		{"seen", shifted, shifted},
		{"due", midnight, midnight},
		{"born", midnight, midnight},
		{"joined", shifted, image},
	}
	for _, a := range attrs {
		for _, op := range []string{"equals", "not_equals", "gt", "gte", "lt", "lte"} {
			t.Run(a.attr+"/"+op, func(t *testing.T) {
				resolved := ResolveServerTime(charKv(a.attr, op+":$now-90s"), cache, resolvedNowMs)
				literal := charKv(a.attr, op+":"+strconv.FormatInt(a.instant, 10))
				require.Equal(t, literal, resolved)

				idx := 0
				dual, err := ToDualClauses(resolved, "eav_table", 7, cache, &idx)
				require.NoError(t, err)
				require.NotContains(t, dual.PgClause, "now()")
				require.Contains(t, dual.PgArgs, a.pgArg, "the postgresql leg binds the instant")
				require.Contains(t, dual.DuckArgs, a.instant, "the duckdb leg binds the instant")
			})
		}
	}
}
