package sqlgen

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/stretchr/testify/require"
)

// dslNumericOps maps each scalar DSL operator to the comparison it denotes;
// in:[x] and not_in:[x] denote = and != over present values.
var dslNumericOps = map[string]string{
	"equals": "=", "not_equals": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=",
	"in": "=", "not_in": "!=",
}

func dslLeafValue(op, literal string) string {
	if op == "in" || op == "not_in" {
		return op + ":[" + literal + "]"
	}
	return op + ":" + literal
}

func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// queryDuckColumn returns the sorted int64 images of column over the rows of
// table that clause selects.
func queryDuckColumn(t *testing.T, db *sql.DB, column, table, clause string, args []any) []int64 {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("SELECT DISTINCT CAST(%s AS BIGINT) FROM %s WHERE %s", column, table, clause), args...)
	require.NoError(t, err, clause)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []int64
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		out = append(out, v)
	}
	require.NoError(t, rows.Err())
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func seedDuckColumn(t *testing.T, db *sql.DB, table, column, sqlType string, values []int64) {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (%s %s)", table, column, sqlType))
	require.NoError(t, err)
	for _, v := range values {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (CAST(? AS %s))", table, sqlType), v)
		require.NoError(t, err)
	}
}

func expectedWhole(values []int64, sqlOp string, operand any) []int64 {
	var out []int64
	for _, v := range values {
		if opHolds(sqlOp, exactCmp(v, operand)) {
			out = append(out, v)
		}
	}
	return out
}

// TestNumericOperand_DuckDBConformance executes the DuckDB renders of every
// numeric operator against DuckDB itself and checks the selected rows with
// exact arithmetic: the DuckClause over the attribute-named columns the
// federated CTEs project (a bigint attribute as BIGINT, an integer one as
// INTEGER or DOUBLE), and the pg-main pushdown clause over m.<column>.
// Before the rewrite, CAST(? AS BIGINT) rounded a fractional operand, so
// equals:7.5 matched 8 and lte:7.5 matched 8 on a bigint attribute.
//
// The pushdown clause runs here against native DuckDB tables; in production
// DuckDB evaluates it over postgres_scan, which this test does not reach.
func TestNumericOperand_DuckDBConformance(t *testing.T) {
	db := openDuckDB(t)
	cache := characterizationCache()
	int32Samples := []int64{math.MinInt32, -8, -7, -2, -1, 0, 1, 2, 7, 8, math.MaxInt32}
	seedDuckColumn(t, db, "f_big", "amount", "BIGINT", wholeNumberSamples)
	seedDuckColumn(t, db, "f_int", "age", "INTEGER", int32Samples)
	seedDuckColumn(t, db, "f_dbl", "age", "DOUBLE", int32Samples)
	_, err := db.Exec("CREATE TABLE m (integer_01 INTEGER, bigint_02 BIGINT)")
	require.NoError(t, err)
	for i, v := range wholeNumberSamples {
		_, err := db.Exec("INSERT INTO m VALUES (?, ?)", int32Samples[i%len(int32Samples)], v)
		require.NoError(t, err)
	}

	targets := []struct {
		attr, table, column string
		values              []int64
		pgMain              bool
	}{
		{"amount", "f_big", "amount", wholeNumberSamples, false},
		{"age", "f_int", "age", int32Samples, false},
		{"age", "f_dbl", "age", int32Samples, false},
		{"amount", "m", "bigint_02", wholeNumberSamples, true},
		{"age", "m", "integer_01", int32Samples, true},
	}
	for _, target := range targets {
		vt := cache[target.attr].ValueType
		for _, literal := range numericOperandSamples {
			operand, _, rangeErr := numericOperand(target.attr, literal, vt)
			for op, sqlOp := range dslNumericOps {
				cond := charKv(target.attr, dslLeafValue(op, literal))
				clause, args, err := duckOrPgMainClause(cond, cache, target.pgMain)
				if rangeErr != nil {
					require.ErrorIs(t, err, forma.ErrInvalidInput, "%v refused before SQL", cond)
					continue
				}
				require.NoError(t, err, "%v", cond)
				if clause == "" {
					continue // not pushable to entity_main: DuckDB evaluates it on the merged result
				}
				got := queryDuckColumn(t, db, target.column, target.table, clause, args)
				want := expectedWhole(distinctSorted(target.values), sqlOp, operand)
				require.Equal(t, want, got, "%s.%s %s %s: %s %v", target.table, target.column, op, literal, clause, args)
			}
		}
	}
}

func duckOrPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, pgMain bool) (string, []any, error) {
	if !pgMain {
		return BuildDuckClause(cond, cache)
	}
	idx := 0
	return BuildPgMainClause(cond, cache, &idx)
}

func distinctSorted(values []int64) []int64 {
	seen := map[int64]bool{}
	var out []int64
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestFederatedShapeHashCoversClauseText guards the federated plan cache:
// two conditions with one shape hash reuse one compiled plan and differ only
// in their binds, so they must render identical clause text on all three
// targets. The numeric rewrite makes the operator depend on the operand's
// class; the shape hash must key on that class.
func TestFederatedShapeHashCoversClauseText(t *testing.T) {
	cache := characterizationCache()
	attrs := []string{"age", "amount", "score", "total", "username", "tag", "born", "active", "flag"}
	operands := append(append([]string{}, numericOperandSamples...), "abc", "true", "1704164645000", "NaN")
	type rendered struct{ cond, pg, pgMain, duck string }
	byShape := map[string]rendered{}
	checked := 0
	check := func(label string, cond forma.Condition) {
		hash, err := queryplan.HashFederatedQueryShape(&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: cond}})
		require.NoError(t, err)
		idx := 0
		dc, err := ToDualClauses(cond, "eav_table", 7, cache, &idx)
		if err != nil {
			return
		}
		got := rendered{label, dc.PgClause, dc.PgMainClause, dc.DuckClause}
		checked++
		prev, seen := byShape[hash]
		if !seen {
			byShape[hash] = got
			return
		}
		require.Equal(t, [3]string{prev.pg, prev.pgMain, prev.duck}, [3]string{got.pg, got.pgMain, got.duck},
			"%s and %s share a shape hash", prev.cond, got.cond)
	}
	for _, attr := range attrs {
		for op := range dslNumericOps {
			for _, operand := range operands {
				leaf := attr + " " + dslLeafValue(op, operand)
				check(leaf, charKv(attr, dslLeafValue(op, operand)))
				check(leaf+" AND amount lt:"+operand, charAnd(charKv(attr, dslLeafValue(op, operand)), charKv("amount", "lt:"+operand)))
			}
		}
	}
	require.Greater(t, checked, 1000)
}
