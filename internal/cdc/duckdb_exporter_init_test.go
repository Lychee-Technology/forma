package cdc

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newExporterInitTestConfig() CDCConfig {
	return CDCConfig{
		DuckDBPath:   "", // :memory:
		S3Region:     "us-test-1",
		S3Endpoint:   "http://127.0.0.1:9000",
		S3UseSSL:     false,
		S3UsePath:    true,
		QueryTimeout: 5 * time.Second,
	}
}

// Session-scoped S3 settings must be present on every physical connection,
// not only the one the constructor happened to initialize (#285, same class
// as #245). SetMaxIdleConns(0) discards released connections so each
// subsequent query runs on a brand-new physical connection.
func TestNewDuckExporter_FreshConnectionsConfigured(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	ctx := context.Background()
	// zaptest, not zap.NewNop(): optional connection init steps are fail-open, so a degraded
	// init (e.g. an INSTALL timing out) surfaces only as a logged warning —
	// with a nop logger the failure output shows just the downstream NULL
	// scan with zero trace of the real cause (#487).
	exp, err := NewDuckExporter(ctx, newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	defer exp.DB.Close()

	requireS3Region := func() {
		var region string
		require.NoError(t, exp.DB.QueryRowContext(ctx, "SELECT current_setting('s3_region')").Scan(&region))
		require.Equal(t, "us-test-1", region)
	}
	requireS3Region() // the connection the constructor initialized

	exp.DB.SetMaxIdleConns(0)
	for i := 0; i < 3; i++ {
		requireS3Region() // red pre-fix: fresh connections carry no session SETs
	}
}

// The exporter pool must be bounded (#285: sql.Open default is unlimited).
func TestNewDuckExporter_PoolBoundedToSingleConnection(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	exp, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	defer exp.DB.Close()
	require.Equal(t, 1, exp.DB.Stats().MaxOpenConnections)
}

// Credential validation must fail construction (regression: this already
// holds pre-fix; post-fix it fails before any connection is opened).
func TestNewDuckExporter_InvalidCredentialFailsFast(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	_, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "bad'key", "testsecretvalue", "", zap.NewNop())
	require.Error(t, err)
}

func TestBuildExporterInitSteps_FullConfigStatementSet(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	cfg := CDCConfig{
		DuckMemLimit: "1GB", DuckThreads: 2,
		S3Region: "us-test-1", S3Endpoint: "https://s3.example.com",
		S3UseSSL: true, S3UsePath: true,
	}
	steps, err := buildExporterInitSteps(cfg, "AKID", "secretvalue", "tok123", true)
	require.NoError(t, err)
	require.Equal(t, []string{
		"PRAGMA memory_limit='1GB';",
		"PRAGMA threads=2;",
		"INSTALL postgres_scanner;", "LOAD postgres_scanner;",
		"INSTALL httpfs;", "LOAD httpfs;",
		"INSTALL parquet;", "LOAD parquet;",
		"SET s3_access_key_id='AKID';",
		"SET s3_secret_access_key='secretvalue';",
		"SET s3_session_token='tok123';",
		"SET s3_region='us-test-1';",
		"SET s3_endpoint='s3.example.com';",
		"SET s3_use_ssl=true;",
		"SET s3_url_style='path';",
	}, flattenStepSQL(steps))
}

// The exporter must never reach for an ambient AWS_SESSION_TOKEN of its own:
// the token is resolved once by the caller alongside the key/secret pair, so a
// caller that supplies no token means no token applies (#329).
func TestBuildExporterInitSteps_IgnoresAmbientEnvSessionToken(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue", "", true)
	require.NoError(t, err)
	for _, sql := range flattenStepSQL(steps) {
		require.NotContains(t, sql, "s3_session_token")
	}
}

// The caller-resolved token is what reaches DuckDB, even when the environment
// offers a competing one (#329).
func TestBuildExporterInitSteps_EmitsCallerResolvedSessionToken(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue", "callertok", true)
	require.NoError(t, err)
	require.Contains(t, flattenStepSQL(steps), "SET s3_session_token='callertok';")
}

func TestBuildExporterInitSteps_MinimalConfigOmitsOptionalStatements(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	steps, err := buildExporterInitSteps(CDCConfig{}, "", "", "", true)
	require.NoError(t, err)
	// no pragmas, no SETs except the unconditional s3_use_ssl
	require.Equal(t, []string{
		"INSTALL postgres_scanner;", "LOAD postgres_scanner;",
		"INSTALL httpfs;", "LOAD httpfs;",
		"INSTALL parquet;", "LOAD parquet;",
		"SET s3_use_ssl=false;",
	}, flattenStepSQL(steps))
}

func flattenStepSQL(steps []duckdbinit.Step) []string {
	var sqls []string
	for _, st := range steps {
		for _, s := range st.Stmts {
			sqls = append(sqls, s.SQL)
		}
	}
	return sqls
}

// httpfs, parquet and the S3 settings are required for every exporter
// engine; postgres_scanner only for the CDC exports that ATTACH PostgreSQL,
// so the S3-only tools never fail on a scanner they do not use (#752).
func TestBuildExporterInitSteps_RequiredCapabilities(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	cfg := CDCConfig{DuckMemLimit: "1GB", S3Region: "us-test-1", S3Endpoint: "http://127.0.0.1:9000", S3UsePath: true}
	for _, requirePostgres := range []bool{true, false} {
		steps, err := buildExporterInitSteps(cfg, "AKID", "secretvalue", "", requirePostgres)
		require.NoError(t, err)
		required := map[string]bool{}
		for _, st := range steps {
			label := st.Stmts[0].Label
			required[label] = st.Required
			if strings.HasPrefix(label, "install ") {
				require.Equal(t, st.Required, st.Verify != nil,
					"%s: a required extension, and only one, is verified loaded", label)
			}
		}
		require.Equal(t, map[string]bool{
			"set memory_limit":         false,
			"install postgres_scanner": requirePostgres,
			"install httpfs":           true,
			"install parquet":          true,
			"set s3_access_key_id":     true,
			"set s3_secret_access_key": true,
			"set s3_region":            true,
			"set s3_endpoint":          true,
			"set s3_use_ssl":           true,
			"set s3_url_style":         true,
		}, required, "requirePostgres=%v", requirePostgres)
	}
}

// verifyingExecer stands in for a DuckDB connection whose INSTALL and LOAD
// statements succeed unless listed in failOn, and whose duckdb_extensions()
// reports loaded exactly the extensions listed in loaded: a
// RequiredExtensionStep's verify statement passes only for those.
type verifyingExecer struct {
	failOn map[string]bool
	loaded map[string]bool
}

func (v *verifyingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(query, "SELECT CASE WHEN EXISTS") {
		for ext := range v.loaded {
			if strings.Contains(query, "extension_name = '"+ext+"'") {
				return driver.RowsAffected(0), nil
			}
		}
		return nil, errors.New("Invalid Input Error: extension is not loaded")
	}
	if v.failOn[query] {
		return nil, errors.New("injected init failure")
	}
	return driver.RowsAffected(0), nil
}

func runExporterInit(t *testing.T, requirePostgres bool, execer driver.ExecerContext) error {
	t.Helper()
	t.Setenv("AWS_SESSION_TOKEN", "")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue", "", requirePostgres)
	require.NoError(t, err)
	return duckdbinit.MakeConnInit(steps, zaptest.NewLogger(t).Sugar(), duckdbinit.DefaultInitTimeout)(execer)
}

// A CDC exporter connection whose INSTALL and LOAD postgres_scanner both
// "succeed" while DuckDB does not report the scanner loaded fails to open:
// the statements succeeding is not the capability (#752).
func TestExporterInit_ScannerNotLoadedDespiteSuccessfulStatementsFails(t *testing.T) {
	execer := &verifyingExecer{loaded: map[string]bool{"httpfs": true, "parquet": true}}
	err := runExporterInit(t, true, execer)
	var stepErr *duckdbinit.RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "verify postgres_scanner loaded", stepErr.Label)

	// The S3-only engines keep the scanner optional and never verify it.
	require.NoError(t, runExporterInit(t, false, execer))
}

// A failed INSTALL postgres_scanner (an unreachable extension repository on
// a host whose scanner is already loaded) does not fail a CDC exporter
// connection the scanner works on (#752).
func TestExporterInit_FailedInstallOfLoadedScannerPasses(t *testing.T) {
	execer := &verifyingExecer{
		failOn: map[string]bool{"INSTALL postgres_scanner;": true},
		loaded: map[string]bool{"postgres_scanner": true, "httpfs": true, "parquet": true},
	}
	require.NoError(t, runExporterInit(t, true, execer))
	require.NoError(t, runExporterInit(t, false, execer))
}

// A failed INSTALL of a scanner that is not loaded either fails the CDC
// exporter's connection and names the INSTALL failure, while an S3-only
// engine still opens.
func TestExporterInit_FailedInstallOfMissingScanner(t *testing.T) {
	execer := &verifyingExecer{
		failOn: map[string]bool{"INSTALL postgres_scanner;": true},
		loaded: map[string]bool{"httpfs": true, "parquet": true},
	}
	err := runExporterInit(t, true, execer)
	var stepErr *duckdbinit.RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "verify postgres_scanner loaded", stepErr.Label)
	require.ErrorContains(t, err, "install postgres_scanner failed: injected init failure")

	require.NoError(t, runExporterInit(t, false, execer))
}
