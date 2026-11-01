package duckdbinit

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// pinnedDuckDBVersion is the engine the go.mod duckdb-go pin (v2.20000.0)
// bundles. The extension cache keys in CI, scripts/duckdb_layer.sh's default,
// the statically linked extension set RequiredExtensionStep relies on, and
// the engine facts design.md records were all checked against it; a driver
// bump that changes the engine fails here so they are rechecked with it
// (#752).
const pinnedDuckDBVersion = "v2.0.0"

// The version comes from the engine the test binary actually loaded, not
// from go.mod: duckdb-go fetches the native library as a separate module.
func TestRunningDuckDBEngineIsThePinnedVersion(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var version string
	require.NoError(t, db.QueryRow("SELECT version()").Scan(&version))
	require.Equal(t, pinnedDuckDBVersion, version)
}
