package federated

// The federated engine's DuckDB client reads only the parquet tiers: the hot
// tier is read over pgx (design.md §6.1), so no connection loads the
// postgres_scanner extension or attaches PostgreSQL (#858). Before #858 the
// scanner was a required step of every connection of the federated client
// (#752).

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
)

// stepRequirements maps each step's first statement label to whether the
// step is required.
func stepRequirements(t *testing.T, steps []duckdbinit.Step) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	for _, st := range steps {
		label := st.Stmts[0].Label
		_, dup := required[label]
		require.False(t, dup, "step %q appears twice", label)
		required[label] = st.Required
		if st.Required && strings.HasPrefix(label, "install ") {
			require.NotNil(t, st.Verify, "%s: a required extension is verified loaded", label)
		}
	}
	return required
}

// The client the factory builds (S3 and parquet on) requires no
// postgres_scanner step and attaches nothing. An operator listing the
// extension, under either name, gets an optional step and no more: the
// listing loads it, it does not make the engine need it.
func TestBuildInitSteps_FederatedClientNeitherLoadsNorAttachesPostgres(t *testing.T) {
	for _, listed := range [][]string{nil, {"postgres"}, {"Postgres_Scanner"}} {
		cfg := newS3ConfiguredDuckDBConfig()
		cfg.Extensions = listed
		steps, err := buildInitSteps(cfg)
		require.NoError(t, err)

		required := stepRequirements(t, steps)
		require.True(t, required["install httpfs"], "listed %q", listed)
		require.True(t, required["install parquet"], "listed %q", listed)
		for _, st := range steps {
			stmts := append([]duckdbinit.Stmt(nil), st.Stmts...)
			if st.Verify != nil {
				stmts = append(stmts, *st.Verify)
			}
			for _, stmt := range stmts {
				sql := strings.ToLower(stmt.SQL)
				require.NotContains(t, sql, "attach", "listed %q: %s", listed, stmt.Label)
				if strings.Contains(sql, "postgres") {
					require.False(t, st.Required, "listed %q: %s must stay optional", listed, stmt.Label)
					require.NotEmpty(t, listed, "%s: nothing listed the extension, so nothing may load it", stmt.Label)
				}
			}
		}
	}
}
