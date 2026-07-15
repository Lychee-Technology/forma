package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// TestDuckDBConfigFromEnv_DisabledByDefault pins that the federated DuckDB
// engine stays off unless DUCKDB_ENABLED is set — the production default.
func TestDuckDBConfigFromEnv_DisabledByDefault(t *testing.T) {
	if got := duckDBConfigFromEnv(forma.DuckDBConfig{Enabled: false}); got.Enabled {
		t.Fatalf("expected DuckDB disabled by default, got enabled")
	}
}

// TestDuckDBConfigFromEnv_EnablesWithS3 pins that DUCKDB_ENABLED turns on the
// engine and wires the S3/httpfs settings from the environment.
func TestDuckDBConfigFromEnv_EnablesWithS3(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "true")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("S3_ACCESS_KEY", "minio")
	t.Setenv("S3_SECRET_KEY", "minio_password")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if !got.Enabled || !got.EnableS3 || !got.EnableParquet {
		t.Fatalf("expected DuckDB+S3+parquet enabled, got %+v", got)
	}
	if got.S3Endpoint != "http://localhost:9000" || got.S3AccessKey != "minio" || got.S3SecretKey != "minio_password" {
		t.Fatalf("S3 settings not wired from env: %+v", got)
	}
	if got.S3Region != "us-east-1" {
		t.Fatalf("expected default region us-east-1, got %q", got.S3Region)
	}
}

// TestDuckDBConfigFromEnv_DuckDBOverridesWin pins that DUCKDB_S3_* overrides
// take precedence over the shared S3_* vars.
func TestDuckDBConfigFromEnv_DuckDBOverridesWin(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("S3_ENDPOINT", "http://shared:9000")
	t.Setenv("DUCKDB_S3_ENDPOINT", "http://duck:9000")

	if got := duckDBConfigFromEnv(forma.DuckDBConfig{}); got.S3Endpoint != "http://duck:9000" {
		t.Fatalf("expected DUCKDB_S3_ENDPOINT override, got %q", got.S3Endpoint)
	}
}

func TestBootstrapServer_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := bootstrapServer(ctx, zap.NewNop().Sugar())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestRunServer_GracefulShutdown(t *testing.T) {
	srv := &http.Server{
		Addr:    ":0",
		Handler: http.NewServeMux(),
	}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- runServer(ctx, srv) }()

	// Cancel after 20 ms — enough for the server goroutine to call ListenAndServe.
	time.AfterFunc(20*time.Millisecond, cancel)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil after graceful shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runServer did not return after context cancellation")
	}
}
