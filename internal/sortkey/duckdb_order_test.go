package sortkey

import (
	"database/sql"
	"math"
	"sort"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// TestDuckDBDoubleOrder pins the DOUBLE order the comparator assumes against
// the engine, before anything relies on it (design.md §6.1): NaN equals
// NaN and sorts above +Infinity in both directions, -0 equals 0 (so the
// next key decides between them), and NULL sorts last either way.
func TestDuckDBDoubleOrder(t *testing.T) {
	db, err := sql.Open("duckdb", "?lambda_syntax=DISABLE_SINGLE_ARROW")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	negZero := math.Copysign(0, -1)
	values := []any{0.0, math.NaN(), negZero, math.Inf(1), nil, -1.5, math.Inf(-1), negZero, math.NaN(), 0.0, 1.5, nil}
	_, err = db.Exec("CREATE TABLE d (id BIGINT, x DOUBLE)")
	require.NoError(t, err)
	for i, v := range values {
		_, err = db.Exec("INSERT INTO d VALUES (?, ?)", int64(i), v)
		require.NoError(t, err)
	}

	for _, desc := range []bool{false, true} {
		dir := "ASC"
		if desc {
			dir = "DESC"
		}
		rows, err := db.Query("SELECT id FROM d ORDER BY x " + dir + ", id ASC")
		require.NoError(t, err)
		var got []int64
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			got = append(got, id)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		order := Order{Columns: []Column{{Name: "x", Type: Double, Desc: desc}, {Name: "id", Type: BigInt}}}
		keys := make([][]any, len(values))
		for i, v := range values {
			keys[i] = []any{v, int64(i)}
		}
		sort.SliceStable(keys, func(a, b int) bool { return order.Compare(keys[a], keys[b]) < 0 })
		want := make([]int64, len(keys))
		for i, k := range keys {
			want[i] = k[1].(int64)
		}
		require.Equal(t, want, got, "DOUBLE %s: the comparator's order is DuckDB's", dir)
	}

	pairs := [][2]float64{
		{math.NaN(), math.NaN()}, {math.NaN(), math.Inf(1)}, {math.Inf(1), math.NaN()},
		{negZero, 0}, {0, negZero}, {math.NaN(), 0.5}, {-1.5, 1.5},
	}
	for _, p := range pairs {
		var lt, eq, gt bool
		require.NoError(t, db.QueryRow("SELECT CAST(? AS DOUBLE) < CAST(? AS DOUBLE), CAST(? AS DOUBLE) = CAST(? AS DOUBLE), CAST(? AS DOUBLE) > CAST(? AS DOUBLE)",
			p[0], p[1], p[0], p[1], p[0], p[1]).Scan(&lt, &eq, &gt))
		c := CompareDouble(p[0], p[1])
		require.Equal(t, []bool{c < 0, c == 0, c > 0}, []bool{lt, eq, gt}, "comparison of %v and %v", p[0], p[1])
	}
}
