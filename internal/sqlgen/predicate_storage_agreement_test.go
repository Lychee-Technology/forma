package sqlgen

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// dslScalarOps is every scalar comparison operator of the condition DSL.
var dslScalarOps = []string{"equals", "not_equals", "gt", "gte", "lt", "lte", "starts_with", "contains"}

// TestToDualClauses_ScalarOperatorsAgreeAcrossStorage pins #676: on the
// federated route each scalar operator gets the same answer, accepted or
// refused, on a column-bound attribute and on an eav_data attribute of the
// same value type. Whether a filter works must not depend on a storage choice
// the caller cannot see. Before #676 the entity_main pushdown refused
// not_equals and the range operators on bound text and uuid columns, which
// the eav_data form and the DuckDB clause both take.
func TestToDualClauses_ScalarOperatorsAgreeAcrossStorage(t *testing.T) {
	cache := characterizationCache()
	cache["level"] = forma.AttributeMetadata{AttributeID: 20, ValueType: forma.ValueTypeInteger}
	cache["price"] = forma.AttributeMetadata{AttributeID: 21, ValueType: forma.ValueTypeNumeric,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("double_01")}}
	cache["day"] = forma.AttributeMetadata{AttributeID: 22, ValueType: forma.ValueTypeDate}

	pairs := []struct {
		vt           string
		bound, eav   string
		operand      string
		wantAccepted []string // the operators both forms take; the rest both refuse
	}{
		{"text", "username", "tag", "m", dslScalarOps},
		{"uuid", "owner", "ref", "6F1C3C3E-1111-4111-8111-111111111111", dslScalarOps[:6]},
		{"integer", "age", "level", "7", dslScalarOps[:6]},
		{"numeric", "price", "score", "7.5", dslScalarOps[:6]},
		{"bigint", "amount", "total", "7", dslScalarOps[:6]},
		{"bool int", "active", "flag", "true", dslScalarOps[:2]},
		{"bool text", "verified", "flag", "true", dslScalarOps[:2]},
		{"unix-ms date", "born", "day", "1704164645000", dslScalarOps[:6]},
		{"iso8601 datetime", "joined", "seen", "2024-01-02T03:04:05Z", dslScalarOps[:6]},
	}
	for _, p := range pairs {
		for _, op := range dslScalarOps {
			t.Run(p.vt+"/"+op, func(t *testing.T) {
				wantAccepted := false
				for _, accepted := range p.wantAccepted {
					wantAccepted = wantAccepted || accepted == op
				}
				for _, attr := range []string{p.bound, p.eav} {
					paramIndex := 0
					_, err := ToDualClauses(charKv(attr, op+":"+p.operand), "eav_table", 7, cache, &paramIndex)
					if wantAccepted {
						require.NoError(t, err, "%s %s:%s", attr, op, p.operand)
						continue
					}
					require.Error(t, err, "%s %s:%s", attr, op, p.operand)
					require.ErrorIs(t, err, forma.ErrInvalidInput)
				}
			})
		}
	}
}

// buildCharTextUuidOrderCases pins the renders #676 admits to the entity_main
// pushdown: not_equals and the range operators on a bound text or uuid
// column, which PG_WHERE_CLAUSE filters before the logical clause does.
func buildCharTextUuidOrderCases() []charCase {
	const stored = "6f1c3c3e-1111-4111-8111-111111111111"
	return []charCase{
		{
			name: "text not_equals bound",
			cond: charKv("username", "not_equals:Alice"),
			want: DualClauses{
				PgMainClause: "m.text_01 != ?", PgMainArgs: []any{"Alice"},
				PgClause: charEavClause("$2", "value_text", "!=", "$3"), PgArgs: []any{int16(1), "Alice"},
				DuckClause: "username != ?", DuckArgs: []any{"Alice"},
			},
			span: 3,
		},
		{
			name: "text gte bound",
			cond: charKv("username", "gte:M"),
			want: DualClauses{
				PgMainClause: "m.text_01 >= ?", PgMainArgs: []any{"M"},
				PgClause: charEavClause("$2", "value_text", ">=", "$3"), PgArgs: []any{int16(1), "M"},
				DuckClause: "username >= ?", DuckArgs: []any{"M"},
			},
			span: 3,
		},
		{
			name: "uuid lt bound binds the canonical spelling",
			cond: charKv("owner", "lt:6F1C3C3E-1111-4111-8111-111111111111"),
			want: DualClauses{
				PgMainClause: "m.uuid_01 < ?", PgMainArgs: []any{stored},
				PgClause: charEavClause("$2", "value_text", "<", "$3"), PgArgs: []any{int16(14), stored},
				DuckClause: "CAST(owner AS VARCHAR) < CAST(? AS VARCHAR)", DuckArgs: []any{stored},
			},
			span: 3,
		},
	}
}

// TestPgMainTextUUIDOrderMatchesTheLogicalClause executes the #676 renders in
// DuckDB, which evaluates PG_WHERE_CLAUSE over the postgres_scan rows of
// entity_main (m.text_01 VARCHAR, m.uuid_01 UUID), and the DuckClause over
// the columns the federated CTEs project for the same attributes: VARCHAR
// from pg_source and the EAV pivot, and UUID from a bound column's parquet.
// pg_source drops a hot row the pushdown rejects, and the dirty set already
// discards its parquet versions, so a pushdown narrower than the logical
// clause would lose the row. Both must select exactly the rows Go's byte
// order selects against the operand's bound spelling (lowercase for a uuid),
// including uuids whose top bit is set and text whose case or encoding orders
// differently under a linguistic collation.
func TestPgMainTextUUIDOrderMatchesTheLogicalClause(t *testing.T) {
	db := openDuckDB(t)
	texts := []string{"B", "a", "abc", "ab", "é", "z", "Z"}
	uuids := []string{
		"00000000-0000-4000-8000-000000000000",
		"7fffffff-ffff-4fff-bfff-ffffffffffff",
		"80000000-0000-4000-8000-000000000000",
		"6f1c3c3e-1111-4111-8111-111111111111",
		"ffffffff-ffff-4fff-bfff-ffffffffffff",
	}
	_, err := db.Exec("CREATE TABLE ent (text_01 VARCHAR, uuid_01 UUID)")
	require.NoError(t, err)
	for i := range max(len(texts), len(uuids)) {
		_, err := db.Exec("INSERT INTO ent VALUES (?, CAST(? AS UUID))", texts[i%len(texts)], uuids[i%len(uuids)])
		require.NoError(t, err)
	}
	_, err = db.Exec("CREATE TABLE unified AS SELECT text_01 AS username, CAST(uuid_01 AS VARCHAR) AS owner FROM ent")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE parquet_tier AS SELECT text_01 AS username, uuid_01 AS owner FROM ent")
	require.NoError(t, err)

	cases := []struct {
		attr, column string
		values       []string
		operands     []string
		canonical    func(string) string // the spelling the operand binds as
	}{
		{"username", "text_01", texts, []string{"a", "ab", "B", "é", "Z"}, func(s string) string { return s }},
		{"owner", "uuid_01", uuids, []string{"7FFFFFFF-FFFF-4FFF-BFFF-FFFFFFFFFFFF", "80000000-0000-4000-8000-000000000000"}, strings.ToLower},
	}
	for _, tc := range cases {
		for _, operand := range tc.operands {
			for _, op := range dslScalarOps[:6] {
				t.Run(tc.attr+"/"+op+":"+operand, func(t *testing.T) {
					paramIndex := 0
					dc, err := ToDualClauses(charKv(tc.attr, op+":"+operand), "eav_table", 7, characterizationCache(), &paramIndex)
					require.NoError(t, err)
					var want []string
					for _, v := range tc.values {
						if opHolds(dslNumericOps[op], strings.Compare(v, tc.canonical(operand))) && !slices.Contains(want, v) {
							want = append(want, v)
						}
					}
					column := "CAST(m." + tc.column + " AS VARCHAR)"
					require.ElementsMatch(t, want, queryDuckStrings(t, db, column, "ent m", dc.PgMainClause, dc.PgMainArgs), "pushdown %s", dc.PgMainClause)
					for _, table := range []string{"unified", "parquet_tier"} {
						got := queryDuckStrings(t, db, "CAST("+tc.attr+" AS VARCHAR)", table, dc.DuckClause, dc.DuckArgs)
						require.ElementsMatch(t, want, got, "logical %s over %s", dc.DuckClause, table)
					}
				})
			}
		}
	}
}

// queryDuckStrings returns the distinct column values over the rows of table
// that clause selects.
func queryDuckStrings(t *testing.T, db *sql.DB, column, table, clause string, args []any) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s", column, table, clause), args...)
	require.NoError(t, err, clause)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		out = append(out, v)
	}
	require.NoError(t, rows.Err())
	return out
}
