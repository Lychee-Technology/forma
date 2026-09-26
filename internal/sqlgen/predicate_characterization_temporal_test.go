package sqlgen

// The temporal rows of the characterization matrix (TestToDualClauses_Characterization),
// split out of predicate_characterization_test.go when #588 added the
// iso8601 literal shapes and that file reached the 500-line limit.

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
			// #588: the literal's offset is folded into the UTC image the
			// column stores; before, PG bound "2024-01-02T05:04:05+02:00"
			// and compared it lexically against "…T03:04:05Z".
			name: "datetime ISO8601 encoding: offset literal binds the UTC image on PG, epoch-ms on duck",
			cond: charKv("joined", "gte:2024-01-02T05:04:05+02:00"),
			want: DualClauses{
				PgMainClause: "m.text_03 >= ?", PgMainArgs: []any{iso},
				PgClause: charEavClause("$2", "value_numeric", ">=", "$3"), PgArgs: []any{int16(9), iso},
				DuckClause: "joined >= CAST(? AS BIGINT)", DuckArgs: []any{int64(1704164645000)},
			},
			span: 3,
		},
		{
			// #588: a unix-ms literal renders in UTC, not the process zone
			// (TestMain pins Europe/Berlin; before, this bound "…T04:04:05+01:00").
			name: "datetime ISO8601 encoding: unix-ms literal binds the UTC image on PG, epoch-ms on duck",
			cond: charKv("joined", "equals:1704164645000"),
			want: DualClauses{
				PgMainClause: "m.text_03 = ?", PgMainArgs: []any{iso},
				PgClause: charEavClause("$2", "value_numeric", "=", "$3"), PgArgs: []any{int16(9), iso},
				DuckClause: "joined = CAST(? AS BIGINT)", DuckArgs: []any{int64(1704164645000)},
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
