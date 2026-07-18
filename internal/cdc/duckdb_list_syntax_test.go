package cdc

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDuckDBListSyntaxSupport pins the DuckDB constructs the list export/read
// path depends on (#204): aggregate ORDER BY + FILTER, two-arg lambda with
// 1-based index in list_transform, and flatten over mixed typed/empty
// sublists. If a DuckDB upgrade drops any of these, this test fails before
// the export SQL does. The ::VARCHAR casts mirror the production SQL's ::TEXT
// (the driver does not scan the raw JSON logical type into a string).
func TestDuckDBListSyntaxSupport(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var s string

	// list() aggregate with ORDER BY inside the aggregate plus FILTER — the
	// export pivot shape for list attributes.
	err = db.QueryRow(`SELECT to_json(list(v ORDER BY TRY_CAST(i AS BIGINT)) FILTER (WHERE k = 1))::VARCHAR
		FROM (VALUES ('b','1',1),('a','0',1),('x','0',2)) t(v,i,k)`).Scan(&s)
	require.NoError(t, err, "list(x ORDER BY y) FILTER (WHERE ...)")
	require.Equal(t, `["a","b"]`, s)

	// Two-arg lambda (element, 1-based index) — the attributes_json
	// reconstruction shape for positional array_indices.
	err = db.QueryRow(`SELECT to_json(list_transform(['a','b'], (x, i) -> CAST(i - 1 AS VARCHAR) || ':' || x))::VARCHAR`).Scan(&s)
	require.NoError(t, err, "list_transform two-arg lambda")
	require.Equal(t, `["0:a","1:b"]`, s)

	// flatten of mixed typed/empty sublists then NULL filtering — how scalar
	// single-object parts combine with per-element list parts.
	err = db.QueryRow(`SELECT to_json(list_filter(flatten([['a', NULL], [], ['b']]), x -> x IS NOT NULL))::VARCHAR`).Scan(&s)
	require.NoError(t, err, "flatten mixed sublists")
	require.Equal(t, `["a","b"]`, s)

	// Struct-typed parts unify across scalar and per-element forms: a struct
	// literal list concatenated with a list_transform product.
	err = db.QueryRow(`SELECT to_json(list_filter(flatten([
		[CASE WHEN 1=0 THEN {'attr_id': 1, 'array_indices': '', 'value_text': 'x'} END],
		list_transform(['a','b'], (x, i) -> {'attr_id': 18, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': x})
	]), x -> x IS NOT NULL))::VARCHAR`).Scan(&s)
	require.NoError(t, err, "struct unification across scalar/list parts")
	require.Equal(t, `[{"attr_id":18,"array_indices":"0","value_text":"a"},{"attr_id":18,"array_indices":"1","value_text":"b"}]`, s)

	// Empty-list representation (#204): the presence-count CASE distinguishes
	// an explicit [] (marker row only) from NULL (no rows at all), and the
	// untyped [] literal unifies with the typed LIST through coalesce.
	err = db.QueryRow(`SELECT to_json(CASE WHEN count(*) FILTER (WHERE k = 1) > 0
			THEN coalesce(list(v ORDER BY TRY_CAST(i AS BIGINT)) FILTER (WHERE k = 1 AND i <> ''), [])
		END)::VARCHAR
		FROM (VALUES ('x', '', 1)) t(v, i, k)`).Scan(&s)
	require.NoError(t, err, "empty-list presence CASE")
	require.Equal(t, `[]`, s)

	var typ string
	err = db.QueryRow(`SELECT typeof(coalesce(CAST(NULL AS VARCHAR[]), []))`).Scan(&typ)
	require.NoError(t, err, "untyped [] coalesce unification")
	require.Equal(t, `VARCHAR[]`, typ)

	// A struct literal whose fields are all NULL is itself non-NULL, so the
	// empty-list marker object survives list_filter(x -> x IS NOT NULL).
	err = db.QueryRow(`SELECT to_json(list_filter([{'a': CAST(NULL AS VARCHAR)}], x -> x IS NOT NULL))::VARCHAR`).Scan(&s)
	require.NoError(t, err, "all-NULL-field struct retention")
	require.Equal(t, `[{"a":null}]`, s)
}
