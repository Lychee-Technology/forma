package cdc

import (
	"context"
	"testing"
	"time"

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
