package federated

// The federated engine's DuckDB client requires the postgres_scanner
// extension on every physical connection, since every hot-leg query attaches
// PostgreSQL; a client opened for S3/parquet reads only does not (#752).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
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

func TestBuildInitSteps_PostgresScannerRequiredOnlyForTheFederatedClient(t *testing.T) {
	cfg := newS3ConfiguredDuckDBConfig()

	steps, err := buildInitSteps(cfg, true)
	require.NoError(t, err)
	require.Equal(t, true, stepRequirements(t, steps)["install postgres_scanner"])

	steps, err = buildInitSteps(cfg, false)
	require.NoError(t, err)
	require.NotContains(t, stepRequirements(t, steps), "install postgres_scanner")
}

// Listing a required extension in DuckDBConfig.Extensions, under any
// spelling DuckDB resolves to it, leaves only its required step.
func TestBuildInitSteps_ListedRequiredExtensionStaysRequired(t *testing.T) {
	for _, listed := range []string{"postgres", "Postgres_Scanner", " postgres_scanner ", "httpfs", "PARQUET"} {
		cfg := newS3ConfiguredDuckDBConfig()
		cfg.Extensions = []string{listed, "json"}
		steps, err := buildInitSteps(cfg, true)
		require.NoError(t, err)
		required := stepRequirements(t, steps)
		for label, req := range required {
			if strings.HasPrefix(label, "install ") && label != "install json" {
				require.True(t, req, "listed %q: %s must not be optional", listed, label)
			}
		}
		require.True(t, required["install postgres_scanner"], "listed %q", listed)
		require.Contains(t, required, "install json")
		require.False(t, required["install json"])
	}

	// A client without the scanner requirement keeps a listed postgres
	// optional, as before.
	cfg := newS3ConfiguredDuckDBConfig()
	cfg.Extensions = []string{"postgres"}
	steps, err := buildInitSteps(cfg, false)
	require.NoError(t, err)
	required := stepRequirements(t, steps)
	require.Contains(t, required, "install postgres")
	require.False(t, required["install postgres"])
}

// unavailableExtensionsDSN opens an in-memory database whose extension
// directory is empty and whose extension repository refuses connections, so
// no extension that is not linked into the engine can be installed or
// loaded: the state of a host whose extension repository is unreachable on
// a cold extension cache.
func unavailableExtensionsDSN(t *testing.T) string {
	return "?" + url.Values{
		"extension_directory":          {t.TempDir()},
		"custom_extension_repository":  {"http://127.0.0.1:1"},
		"autoinstall_known_extensions": {"false"},
		"autoload_known_extensions":    {"false"},
	}.Encode()
}

func requireScannerStepError(t *testing.T, err error) {
	t.Helper()
	var stepErr *duckdbinit.RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "verify postgres_scanner loaded", stepErr.Label)
}

// Without the scanner, the federated client fails to open instead of
// passing its constructor's ping and failing every query; an S3-only client
// of the same database still opens.
func TestNewFederatedDuckDBClient_MissingPostgresScannerFailsTheConstructor(t *testing.T) {
	useTestGlobalLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := forma.DuckDBConfig{Enabled: true, DBPath: unavailableExtensionsDSN(t), MaxConnections: 1, QueryTimeout: 5 * time.Second}

	_, err := NewFederatedDuckDBClientContext(ctx, cfg)
	requireScannerStepError(t, err)

	duck, err := NewDuckDBClientContext(ctx, cfg)
	require.NoError(t, err, "a client that reads S3/parquet only does not need the scanner")
	require.NoError(t, duck.Close())
}

func requireScannerLoaded(t *testing.T, db *sql.DB, what string) {
	t.Helper()
	var loaded bool
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'postgres_scanner'").Scan(&loaded), what)
	require.True(t, loaded, what)
}

// Every fresh physical connection of the federated client has the scanner
// loaded, so does a connection of a database the recovering connector
// reopened, and a reopened database that cannot load it fails the query that
// needed the connection (#702, #752).
func TestNewFederatedDuckDBClient_ScannerOnFreshAndReopenedConnections(t *testing.T) {
	useTestGlobalLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	missing := unavailableExtensionsDSN(t)
	var opens atomic.Int32
	swapOpenDuckDBConnector(t, func(dsn string, init func(driver.ExecerContext) error) (driver.Connector, error) {
		if opens.Add(1) >= 3 {
			dsn = missing
		}
		return openFatalInjecting(dsn, init)
	})

	duck, err := NewFederatedDuckDBClientContext(ctx, forma.DuckDBConfig{Enabled: true, MaxConnections: 2, QueryTimeout: 30 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })
	duck.DB.SetMaxIdleConns(0) // every query opens a new physical connection

	for i := 0; i < 3; i++ {
		requireScannerLoaded(t, duck.DB, "fresh connection")
	}

	_, err = duck.DB.ExecContext(ctx, injectFatalSQL)
	require.Error(t, err)
	requireScannerLoaded(t, duck.DB, "connection of the reopened database")
	require.Equal(t, int32(2), opens.Load(), "the query reached a reopened database")

	_, err = duck.DB.ExecContext(ctx, injectFatalSQL)
	require.Error(t, err)
	_, err = duck.DB.ExecContext(ctx, "SELECT 1")
	requireScannerStepError(t, err)
	require.Equal(t, int32(3), opens.Load())
}
