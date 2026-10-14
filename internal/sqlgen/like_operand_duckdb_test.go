package sqlgen

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLikeOperand_DuckDBConformance: a starts_with / contains operand is
// literal text on the DuckDB renders, metacharacters included.
func TestLikeOperand_DuckDBConformance(t *testing.T) {
	db := openDuckDB(t)
	cache := characterizationCache()
	values := []string{"abc", "a_c", "a%c", `a\c`, "xa_cx", "A_C", "a__c"}
	_, err := db.Exec("CREATE TABLE f (username VARCHAR, text_01 VARCHAR)")
	require.NoError(t, err)
	for _, v := range values {
		_, err := db.Exec("INSERT INTO f VALUES (?, ?)", v, v)
		require.NoError(t, err)
	}
	operands := []string{"a_c", "_", "%", `a\`, `\c`, "a", "a__"}
	for _, operand := range operands {
		for _, op := range []string{"starts_with", "contains"} {
			var want []string
			for _, v := range values {
				if (op == "starts_with" && strings.HasPrefix(v, operand)) || (op == "contains" && strings.Contains(v, operand)) {
					want = append(want, v)
				}
			}
			sort.Strings(want)
			cond := charKv("username", op+":"+operand)
			for _, pgMain := range []bool{false, true} {
				clause, args, err := duckOrPgMainClause(cond, cache, pgMain)
				require.NoError(t, err)
				clause = strings.ReplaceAll(clause, "m.text_01", "text_01")
				rows, err := db.Query("SELECT username FROM f WHERE "+clause+" ORDER BY username", args...)
				require.NoError(t, err, clause)
				var got []string
				for rows.Next() {
					var v string
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				require.NoError(t, rows.Close())
				sort.Strings(got)
				require.Equal(t, want, got, "%s:%s via %s %v", op, operand, clause, args)
			}
		}
	}
}
