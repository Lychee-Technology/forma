package federated

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// strictLambdaDSN opens an in-memory DuckDB that refuses the deprecated
// single-arrow lambda (`x -> ...`), as DuckDB 2.0 does by default (#751).
// The tests here that execute generated SQL open it, so a reintroduced
// arrow lambda fails on the pinned 1.5.x runtime, before the driver bump.
const strictLambdaDSN = "?lambda_syntax=DISABLE_SINGLE_ARROW"

// TestStrictLambdaDSNRefusesSingleArrow proves the DSN option takes: were it
// ignored, every test opening strictLambdaDSN would accept an arrow lambda
// and stop guarding the 2.0 syntax.
func TestStrictLambdaDSNRefusesSingleArrow(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var n int
	require.Error(t, db.QueryRow(`SELECT len(list_filter([1, 2], x -> x > 1))`).Scan(&n))
	require.NoError(t, db.QueryRow(`SELECT len(list_filter([1, 2], lambda x: x > 1))`).Scan(&n))
	require.Equal(t, 1, n)
}
