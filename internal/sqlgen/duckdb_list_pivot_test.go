package sqlgen

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestDuckDBAttrCastListPassesThrough: CAST(list AS VARCHAR) would stringify
// the column; the list branch must return the expression unchanged.
func TestDuckDBAttrCastListPassesThrough(t *testing.T) {
	require.Equal(t, "tags", duckDBAttrCast("tags", forma.ValueTypeList))
	// Scalars keep their casts.
	require.Equal(t, "CAST(note AS VARCHAR)", duckDBAttrCast("note", forma.ValueTypeText))
}

// TestDuckDBListLegUnification pins the engine fact the list read path rests
// on: UNION ALL type-unifies VARCHAR[] with a NULL-typed leg (an absent
// column under union_by_name).
func TestDuckDBListLegUnification(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var typ string
	require.NoError(t, db.QueryRow(
		`SELECT typeof(x) FROM (SELECT ['a'] AS x UNION ALL SELECT NULL) LIMIT 1`).Scan(&typ))
	require.Equal(t, "VARCHAR[]", typ)
}
