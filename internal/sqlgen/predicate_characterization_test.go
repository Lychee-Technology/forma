package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// characterizationCache covers every storage/encoding combination the three
// emitters treat differently: bound/unbound text, integer, numeric, bool
// (int/text encodings), date (unix-ms/ISO8601 encodings), datetime, uuid.
func characterizationCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"username": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"score": {AttributeID: 3, ValueType: forma.ValueTypeNumeric},
		"tag":   {AttributeID: 4, ValueType: forma.ValueTypeText},
		"active": {AttributeID: 5, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bool_01"), Encoding: forma.MainColumnEncodingBoolInt}},
		"verified": {AttributeID: 6, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingBoolText}},
		"flag": {AttributeID: 7, ValueType: forma.ValueTypeBool},
		"born": {AttributeID: 8, ValueType: forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bigint_01"), Encoding: forma.MainColumnEncodingUnixMs}},
		"joined": {AttributeID: 9, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_03"), Encoding: forma.MainColumnEncodingISO8601}},
		"seen": {AttributeID: 10, ValueType: forma.ValueTypeDateTime},
		"ref":  {AttributeID: 11, ValueType: forma.ValueTypeUUID},
		"amount": {AttributeID: 12, ValueType: forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bigint_02")}},
		"total": {AttributeID: 13, ValueType: forma.ValueTypeBigInt},
	}
}

func charKv(attr, value string) forma.Condition { return &forma.KvCondition{Attr: attr, Value: value} }

func charAnd(cs ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: cs}
}

func charOr(cs ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: cs}
}

const charEXISTS = "EXISTS (SELECT 1 FROM eav_table x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = "

func charEavClause(attrPh, valueColumn, sqlOp, valuePh string) string {
	return charEXISTS + attrPh + " AND x." + valueColumn + " " + sqlOp + " " + valuePh + ")"
}

// charCase is one row of the characterization matrix: a condition and the exact
// clause bytes, argument values (with Go types), and paramIndex advancement the
// three emitters must reproduce.
type charCase struct {
	name string
	cond forma.Condition
	want DualClauses
	span int // expected paramIndex advancement
}

// buildCharTextCases covers the text storage class: bound/unbound equals, the bare
// value default, and the LIKE wildcard rewrites (starts_with / contains).
func buildCharTextCases() []charCase {
	return []charCase{
		{
			name: "text equals bound",
			cond: charKv("username", "equals:Alice"),
			want: DualClauses{
				PgMainClause: "m.text_01 = ?", PgMainArgs: []any{"Alice"},
				PgClause: charEavClause("$2", "value_text", "=", "$3"), PgArgs: []any{int16(1), "Alice"},
				DuckClause: "username = ?", DuckArgs: []any{"Alice"},
			},
			span: 3,
		},
		{
			name: "text bare value defaults to equals",
			cond: charKv("username", "Alice"),
			want: DualClauses{
				PgMainClause: "m.text_01 = ?", PgMainArgs: []any{"Alice"},
				PgClause: charEavClause("$2", "value_text", "=", "$3"), PgArgs: []any{int16(1), "Alice"},
				DuckClause: "username = ?", DuckArgs: []any{"Alice"},
			},
			span: 3,
		},
		{
			name: "text equals unbound goes EAV only",
			cond: charKv("tag", "equals:x"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_text", "=", "$2"), PgArgs: []any{int16(4), "x"},
				DuckClause: "tag = ?", DuckArgs: []any{"x"},
			},
			span: 2,
		},
		{
			name: "starts_with wildcards suffix",
			cond: charKv("username", "starts_with:Al"),
			want: DualClauses{
				PgMainClause: "m.text_01 LIKE ?", PgMainArgs: []any{"Al%"},
				PgClause: charEavClause("$2", "value_text", "LIKE", "$3"), PgArgs: []any{int16(1), "Al%"},
				DuckClause: "username LIKE ?", DuckArgs: []any{"Al%"},
			},
			span: 3,
		},
		{
			name: "contains wildcards both sides",
			cond: charKv("tag", "contains:mid"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_text", "LIKE", "$2"), PgArgs: []any{int16(4), "%mid%"},
				DuckClause: "tag LIKE ?", DuckArgs: []any{"%mid%"},
			},
			span: 2,
		},
	}
}

// buildCharNumericBoolCases covers the numeric and boolean storage classes, including
// the >2^31 range limit test where all three emitters now bind integer/smallint as int64
// (#355), and the bool-int / bool-text / unbound-bool encodings. The bigint storage class
// lives in predicate_characterization_bigint_test.go.
func buildCharNumericBoolCases() []charCase {
	return []charCase{
		{
			name: "integer gt: main/eav/duck all int64",
			cond: charKv("age", "gt:30"),
			want: DualClauses{
				PgMainClause: "m.integer_01 > ?", PgMainArgs: []any{int64(30)},
				PgClause: charEavClause("$2", "value_numeric", ">", "$3"), PgArgs: []any{int16(2), int64(30)},
				DuckClause: "age > CAST(? AS BIGINT)", DuckArgs: []any{int64(30)},
			},
			span: 3,
		},
		{
			name: "numeric unbound: duck binds decimal string",
			cond: charKv("score", "lt:3.5"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_numeric", "<", "$2"), PgArgs: []any{int16(3), 3.5},
				DuckClause: "score < CAST(? AS DOUBLE)", DuckArgs: []any{"3.5"},
			},
			span: 2,
		},
		{
			name: "integer above 2^31: main/eav/duck all int64, duck compares at BIGINT (#384)",
			cond: charKv("age", "equals:9007199254740993"),
			want: DualClauses{
				PgMainClause: "m.integer_01 = ?", PgMainArgs: []any{int64(9007199254740993)},
				PgClause: charEavClause("$2", "value_numeric", "=", "$3"), PgArgs: []any{int16(2), int64(9007199254740993)},
				DuckClause: "age = CAST(? AS BIGINT)", DuckArgs: []any{int64(9007199254740993)},
			},
			span: 3,
		},
		{
			name: "bool bool-int encoding: main int64(1), eav truthy bool, duck true",
			cond: charKv("active", "equals:1"),
			want: DualClauses{
				PgMainClause: "m.bool_01 = ?", PgMainArgs: []any{int64(1)},
				PgClause: charEXISTS+"$2 AND (x.value_numeric <> 0) = $3)", PgArgs: []any{int16(5), true},
				DuckClause: "active = CAST(? AS BOOLEAN)", DuckArgs: []any{true},
			},
			span: 3,
		},
		{
			name: "bool bool-text encoding zero: main \"0\", eav truthy bool, duck false",
			cond: charKv("verified", "equals:0"),
			want: DualClauses{
				PgMainClause: "m.text_02 = ?", PgMainArgs: []any{"0"},
				PgClause: charEXISTS+"$2 AND (x.value_numeric <> 0) = $3)", PgArgs: []any{int16(6), false},
				DuckClause: "verified = CAST(? AS BOOLEAN)", DuckArgs: []any{false},
			},
			span: 3,
		},
		{
			name: "bool unbound: eav truthy bool, duck true",
			cond: charKv("flag", "equals:1"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEXISTS+"$1 AND (x.value_numeric <> 0) = $2)", PgArgs: []any{int16(7), true},
				DuckClause: "flag = CAST(? AS BOOLEAN)", DuckArgs: []any{true},
			},
			span: 2,
		},
	}
}

// buildCharTemporalUuidCases covers the date/datetime encodings (unix-ms, ISO8601,
// unbound) and the uuid storage class.
func buildCharTemporalUuidCases() []charCase {
	iso := "2024-01-02T03:04:05Z"
	return []charCase{
		{
			name: "date unix-ms encoding: main/eav int64 ms, duck epoch-ms int64",
			cond: charKv("born", "gte:1700000000000"),
			want: DualClauses{
				PgMainClause: "m.bigint_01 >= ?", PgMainArgs: []any{int64(1700000000000)},
				PgClause: charEavClause("$2", "value_numeric", ">=", "$3"), PgArgs: []any{int16(8), int64(1700000000000)},
				DuckClause: "born >= CAST(? AS BIGINT)", DuckArgs: []any{int64(1700000000000)},
			},
			span: 3,
		},
		{
			name: "datetime ISO8601 encoding: main/eav bind ISO string, duck epoch-ms int64",
			cond: charKv("joined", "gte:"+iso),
			want: DualClauses{
				PgMainClause: "m.text_03 >= ?", PgMainArgs: []any{iso},
				PgClause: charEavClause("$2", "value_numeric", ">=", "$3"), PgArgs: []any{int16(9), iso},
				DuckClause: "joined >= CAST(? AS BIGINT)", DuckArgs: []any{int64(1704164645000)},
			},
			span: 3,
		},
		{
			name: "datetime unbound: eav unix-ms int64, duck epoch-ms int64",
			cond: charKv("seen", "gte:"+iso),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_numeric", ">=", "$2"), PgArgs: []any{int16(10), int64(1704164645000)},
				DuckClause: "seen >= CAST(? AS BIGINT)", DuckArgs: []any{int64(1704164645000)},
			},
			span: 2,
		},
		{
			name: "uuid equals unbound: eav value_text, duck VARCHAR cast",
			cond: charKv("ref", "equals:0b210f52-1f4d-4f47-9799-1e2f2c0efc07"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_text", "=", "$2"), PgArgs: []any{int16(11), "0b210f52-1f4d-4f47-9799-1e2f2c0efc07"},
				DuckClause: "ref = CAST(? AS VARCHAR)", DuckArgs: []any{"0b210f52-1f4d-4f47-9799-1e2f2c0efc07"},
			},
			span: 2,
		},
	}
}

// buildCharCompositeCases covers nested AND/OR pushdown: pg-main keeping only the
// pushable branch, an all-pushable OR prefilter, and an AND of two main
// predicates on the same column.
func buildCharCompositeCases() []charCase {
	return []charCase{
		{
			name: "AND(main leaf, vetoed OR): pg-main keeps only pushable branch",
			cond: charAnd(charKv("username", "equals:Alice"), charOr(charKv("age", "gt:18"), charKv("tag", "equals:x"))),
			want: DualClauses{
				PgMainClause: "(m.text_01 = ?)", PgMainArgs: []any{"Alice"},
				PgClause: "((" + charEavClause("$2", "value_text", "=", "$3") + ") AND (((" +
					charEavClause("$4", "value_numeric", ">", "$5") + ") OR (" +
					charEavClause("$6", "value_text", "=", "$7") + "))))",
				PgArgs:     []any{int16(1), "Alice", int16(2), int64(18), int16(4), "x"},
				DuckClause: "(username = ?) AND ((age > CAST(? AS BIGINT)) OR (tag = ?))",
				DuckArgs:   []any{"Alice", int64(18), "x"},
			},
			span: 7,
		},
		{
			name: "OR all pushable emits pg-main prefilter",
			cond: charOr(charKv("username", "equals:A"), charKv("age", "gt:5")),
			want: DualClauses{
				PgMainClause: "((m.text_01 = ?) OR (m.integer_01 > ?))", PgMainArgs: []any{"A", int64(5)},
				PgClause: "((" + charEavClause("$3", "value_text", "=", "$4") + ") OR (" +
					charEavClause("$5", "value_numeric", ">", "$6") + "))",
				PgArgs:     []any{int16(1), "A", int16(2), int64(5)},
				DuckClause: "(username = ?) OR (age > CAST(? AS BIGINT))",
				DuckArgs:   []any{"A", int64(5)},
			},
			span: 6,
		},
		{
			name: "AND of two main predicates on same column",
			cond: charAnd(charKv("age", "gt:10"), charKv("age", "lt:90")),
			want: DualClauses{
				PgMainClause: "((m.integer_01 > ?) AND (m.integer_01 < ?))", PgMainArgs: []any{int64(10), int64(90)},
				PgClause: "((" + charEavClause("$3", "value_numeric", ">", "$4") + ") AND (" +
					charEavClause("$5", "value_numeric", "<", "$6") + "))",
				PgArgs:     []any{int16(2), int64(10), int16(2), int64(90)},
				DuckClause: "(age > CAST(? AS BIGINT)) AND (age < CAST(? AS BIGINT))",
				DuckArgs:   []any{int64(10), int64(90)},
			},
			span: 6,
		},
	}
}

// TestToDualClauses_Characterization locks the exact clause bytes, argument
// values (including Go types), and paramIndex advancement of the three
// emitters for the full storage/encoding matrix. Any parse-once refactor
// must keep every case byte- and type-identical. The matrix is assembled from
// per-storage-class case builders to keep each function within size limits.
func TestToDualClauses_Characterization(t *testing.T) {
	cache := characterizationCache()

	var cases []charCase
	cases = append(cases, buildCharTextCases()...)
	cases = append(cases, buildCharNumericBoolCases()...)
	cases = append(cases, buildCharBigIntCases()...)
	cases = append(cases, buildCharTemporalUuidCases()...)
	cases = append(cases, buildCharCompositeCases()...)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 0
			dc, err := ToDualClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.want, dc)
			require.Equal(t, tc.span, paramIndex, "paramIndex advancement")

			// Plan+Bind must reproduce the direct path exactly for every case.
			plan, err := PlanDualClauses(tc.cond, "eav_table", 7, cache, 0)
			require.NoError(t, err)
			bound := 0
			got, err := plan.Bind(tc.cond, cache, &bound)
			require.NoError(t, err)
			require.Equal(t, dc, got)
			require.Equal(t, paramIndex, bound)
		})
	}
}

// TestToDualClauses_Characterization_Errors locks the error surface: which
// generator reports first (pg-main runs before EAV), the exact message, and
// the strict-vs-lenient parse split between entrypoints.
func TestToDualClauses_Characterization_Errors(t *testing.T) {
	cache := characterizationCache()

	cases := []struct {
		name    string
		cond    forma.Condition
		wantErr string
		// clientError marks the cases driven purely by the caller's condition
		// payload. Since #301 the HTTP boundary classifies on sentinel evidence
		// alone, so these must wrap forma.ErrInvalidInput or an advanced_query with a
		// bad filter would answer 500 with a redacted body instead of a 400.
		// The unmarked cases are schema/config or internal invariants.
		clientError bool
	}{
		{
			// classifyPredicate splits the raw value at the first colon, so a
			// bare ISO timestamp reads as an unknown operator and vetoes the
			// bound date column with an error (not a silent skip).
			name:        "bare ISO on bound date errors in pg-main",
			cond:        charKv("born", "2024-01-02T03:04:05Z"),
			wantErr:     "pg main generation: unsupported operator: equals",
			clientError: true,
		},
		{
			// neq is canonicalized by ToSQLOperator but NOT by
			// classifyPredicate, so it is unpushable on a bound column.
			name:        "neq alias on bound column errors in pg-main",
			cond:        charKv("age", "neq:7"),
			wantErr:     "pg main generation: unsupported operator: neq",
			clientError: true,
		},
		{
			// Strict parse splits at the colon: op "2024-01-02T03" passes the
			// non-empty check, then the value "04:05Z" fails date parsing.
			name:        "bare ISO on unbound datetime errors in EAV date parse",
			cond:        charKv("seen", "2024-01-02T03:04:05Z"),
			wantErr:     "pg sql generation: invalid date value for 'seen': invalid date value: expected ISO 8601 format or unix milliseconds, got '04:05Z'",
			clientError: true,
		},
		{
			name:        "empty value after operator fails strict EAV parse",
			cond:        charKv("tag", "equals:"),
			wantErr:     "pg sql generation: invalid KvCondition value format: equals:",
			clientError: true,
		},
		{
			name:        "unknown attribute errors in EAV",
			cond:        charKv("ghost", "equals:1"),
			wantErr:     "pg sql generation: attribute not found in cache: ghost",
			clientError: true,
		},
		{
			// The operator is caller-supplied, so this is a fixable 400. The
			// earlier #301 sweep missed normalizePgEavPayload's whitelist and
			// left it an opaque 500 (#307 round-4 Finding 4).
			name:        "LIKE on uuid rejected by EAV whitelist",
			cond:        charKv("ref", "starts_with:0b"),
			wantErr:     "pg sql generation: operator 'starts_with' only supported for text attributes, not 'uuid'",
			clientError: true,
		},
		{
			name:        "gt on unbound bool rejected by EAV whitelist",
			cond:        charKv("flag", "gt:1"),
			wantErr:     "pg sql generation: operator 'gt' not supported for boolean attributes",
			clientError: true,
		},
		{
			name:        "gt on bound bool rejected by pg-main",
			cond:        charKv("active", "gt:1"),
			wantErr:     "pg main generation: unsupported operator: gt",
			clientError: true,
		},
		{
			name:    "nil child in composite errors in EAV",
			cond:    charAnd(charKv("username", "equals:Alice"), nil),
			wantErr: "pg sql generation: nil condition not allowed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 0
			_, err := ToDualClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.Error(t, err)
			// Contains, not Equal: a sentinel wrap appends ": invalid input", and the
			// prose above is the part that is actually the contract.
			require.Contains(t, err.Error(), tc.wantErr)
			if tc.clientError {
				require.ErrorIs(t, err, forma.ErrInvalidInput)
			}
		})
	}
}

// TestStandaloneBuilders_Characterization locks the lenient semantics of the
// standalone BuildDuckClause / BuildPgMainClause entrypoints, which must NOT
// inherit the EAV path's strict parse or its error surface.
func TestStandaloneBuilders_Characterization(t *testing.T) {
	cache := characterizationCache()

	t.Run("duck lenient keeps malformed pair as equals(raw)", func(t *testing.T) {
		clause, args, err := BuildDuckClause(charKv("tag", "equals:"), cache)
		require.NoError(t, err)
		require.Equal(t, "tag = ?", clause)
		require.Equal(t, []any{"equals:"}, args)
	})

	t.Run("duck nil child renders 1=1", func(t *testing.T) {
		clause, args, err := BuildDuckClause(charAnd(charKv("username", "equals:Alice"), nil), cache)
		require.NoError(t, err)
		require.Equal(t, "(username = ?) AND (1=1)", clause)
		require.Equal(t, []any{"Alice"}, args)
	})

	t.Run("duck no-metadata inference matrix", func(t *testing.T) {
		cases := []struct {
			name       string
			value      string
			wantClause string
			wantArgs   []any
		}{
			{"numeric literal", "equals:42", "ghost = CAST(? AS DOUBLE)", []any{"42"}},
			{"bool literal", "equals:true", "ghost = CAST(? AS BOOLEAN)", []any{true}},
			{"uuid literal", "equals:0b210f52-1f4d-4f47-9799-1e2f2c0efc07", "ghost = CAST(? AS VARCHAR)", []any{"0b210f52-1f4d-4f47-9799-1e2f2c0efc07"}},
			{"iso literal", "gte:2024-01-02T03:04:05Z", "ghost >= CAST(? AS BIGINT)", []any{int64(1704164645000)}},
			{"bare iso literal", "2024-01-02T03:04:05Z", "ghost = CAST(? AS BIGINT)", []any{int64(1704164645000)}},
			{"text literal", "equals:hello", "ghost = ?", []any{"hello"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				clause, args, err := BuildDuckClause(charKv("ghost", tc.value), cache)
				require.NoError(t, err)
				require.Equal(t, tc.wantClause, clause)
				require.Equal(t, tc.wantArgs, args)
			})
		}
	})

	t.Run("pg-main nil child skipped silently", func(t *testing.T) {
		idx := 0
		clause, args, err := BuildPgMainClause(charAnd(charKv("username", "equals:Alice"), nil), cache, &idx)
		require.NoError(t, err)
		require.Equal(t, "(m.text_01 = ?)", clause)
		require.Equal(t, []any{"Alice"}, args)
		require.Equal(t, 1, idx)
	})

	t.Run("pg-main unbound attr with strict-invalid value skipped without error", func(t *testing.T) {
		idx := 0
		clause, args, err := BuildPgMainClause(charKv("tag", "equals:"), cache, &idx)
		require.NoError(t, err)
		require.Equal(t, "", clause)
		require.Nil(t, args)
		require.Equal(t, 0, idx)
	})

	t.Run("pg-main nested OR veto cascades from inner AND", func(t *testing.T) {
		idx := 0
		cond := charOr(charKv("age", "gt:18"), charAnd(charKv("username", "equals:A"), charKv("tag", "equals:x")))
		clause, args, err := BuildPgMainClause(cond, cache, &idx)
		require.NoError(t, err)
		require.Equal(t, "", clause)
		require.Nil(t, args)
		require.Equal(t, 0, idx)
	})

	t.Run("pg-main bound bool gt surfaces error", func(t *testing.T) {
		idx := 0
		_, _, err := BuildPgMainClause(charKv("active", "gt:1"), cache, &idx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported operator: gt")
		require.ErrorIs(t, err, forma.ErrInvalidInput)
	})
}
