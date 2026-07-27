package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	exp, err := NewDuckExporter(ctx, newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", zap.NewNop())
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
	exp, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", zap.NewNop())
	require.NoError(t, err)
	defer exp.DB.Close()
	require.Equal(t, 1, exp.DB.Stats().MaxOpenConnections)
}

// Credential validation must fail construction (regression: this already
// holds pre-fix; post-fix it fails before any connection is opened).
func TestNewDuckExporter_InvalidCredentialFailsFast(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	_, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "bad'key", "testsecretvalue", zap.NewNop())
	require.Error(t, err)
}

func TestBuildExporterInitSteps_FullConfigStatementSet(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	cfg := CDCConfig{
		DuckMemLimit: "1GB", DuckThreads: 2,
		S3Region: "us-test-1", S3Endpoint: "https://s3.example.com",
		S3UseSSL: true, S3UsePath: true, S3SessionToken: "tok123",
	}
	steps, err := buildExporterInitSteps(cfg, "AKID", "secretvalue")
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

func TestBuildExporterInitSteps_SessionTokenEnvFallback(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue")
	require.NoError(t, err)
	require.Contains(t, flattenStepSQL(steps), "SET s3_session_token='envtok';")
}

func TestBuildExporterInitSteps_MinimalConfigOmitsOptionalStatements(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	steps, err := buildExporterInitSteps(CDCConfig{}, "", "")
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
