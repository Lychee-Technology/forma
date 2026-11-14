package federated

import (
	"database/sql"
	"sort"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/stretchr/testify/require"
)

// A column-bound uuid attribute's parquet column is a DuckDB UUID (the
// export's TRY_CAST(col AS UUID)), while its sort key's export type is
// VARCHAR, the canonical text the hot leg derives. The parquet leg must read
// the scanned UUID as that text, and DuckDB's UUID order must be the text's
// byte order, or the merge places hot rows among parquet rows by the UUID's
// raw bytes (#857, found by TestDualRunSortKeyEdges).
func TestParquetUUIDSortKey(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	texts := []string{
		"ffffffff-0000-4000-8000-000000000000", "7fffffff-0000-4000-8000-000000000000",
		"80000000-0000-4000-8000-000000000000", "00000000-0000-4000-8000-000000000001",
		"3b31567d-e046-43b0-93ae-5aaba596c42b", "dca3e78a-8c41-4f84-9361-dc61b7591fa8",
	}
	values := make([]string, len(texts))
	for i, s := range texts {
		values[i] = "('" + s + "'::UUID)"
	}
	rows, err := db.Query("SELECT u FROM (VALUES " + strings.Join(values, ", ") + ") AS t(u) ORDER BY u")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var got []string
	for rows.Next() {
		var raw any
		require.NoError(t, rows.Scan(&raw))
		key, err := parquetKeyValue(sortkey.Varchar, raw)
		require.NoError(t, err)
		got = append(got, key.(string))
	}
	require.NoError(t, rows.Err())

	want := append([]string{}, texts...)
	sort.Strings(want)
	require.Equal(t, want, got, "DuckDB's UUID order, read as text, is the text's byte order")
}

// Bytes that are not a UUID's 16 are refused, not read as text.
func TestParquetVarcharKeyRefusesOtherBytes(t *testing.T) {
	_, err := parquetKeyValue(sortkey.Varchar, []byte("not a uuid"))
	require.Error(t, err)
	key, err := parquetKeyValue(sortkey.Varchar, "plain text")
	require.NoError(t, err)
	require.Equal(t, "plain text", key)
}
