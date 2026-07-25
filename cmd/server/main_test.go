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
	t.Setenv("S3_BUCKET", "forma-cdc")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if !got.Enabled || !got.EnableS3 || !got.EnableParquet {
		t.Fatalf("expected DuckDB+S3+parquet enabled, got %+v", got)
	}
	if got.S3Endpoint != "http://localhost:9000" || got.S3AccessKey != "minio" || got.S3SecretKey != "minio_password" {
		t.Fatalf("S3 settings not wired from env: %+v", got)
	}
	// The bucket falls back to the shared name unconditionally: a bucket alone
	// never makes the config inert, so it cannot break an upgrade.
	if got.S3Bucket != "forma-cdc" {
		t.Fatalf("expected S3Bucket from S3_BUCKET, got %q", got.S3Bucket)
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

// TestDuckDBConfigFromEnv_ManifestFieldsFromSharedVars pins that manifest config
// fields are wired from shared MANIFEST_* vars when DUCKDB_MANIFEST_* are absent.
func TestDuckDBConfigFromEnv_ManifestFieldsFromSharedVars(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "true")
	t.Setenv("MANIFEST_PREFIX", "shared-prefix")
	t.Setenv("MANIFEST_TEMPLATE", "shared-template")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.ManifestPrefix != "shared-prefix" {
		t.Fatalf("expected ManifestPrefix from MANIFEST_PREFIX, got %q", got.ManifestPrefix)
	}
	if got.ManifestTemplate != "shared-template" {
		t.Fatalf("expected ManifestTemplate from MANIFEST_TEMPLATE, got %q", got.ManifestTemplate)
	}
}

// TestDuckDBConfigFromEnv_DuckDBManifestOverridesWin pins that DUCKDB_MANIFEST_*
// overrides take precedence over the shared MANIFEST_* vars.
func TestDuckDBConfigFromEnv_DuckDBManifestOverridesWin(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("MANIFEST_PREFIX", "shared-prefix")
	t.Setenv("MANIFEST_TEMPLATE", "shared-template")
	t.Setenv("DUCKDB_MANIFEST_PREFIX", "duck-prefix")
	t.Setenv("DUCKDB_MANIFEST_TEMPLATE", "duck-template")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.ManifestPrefix != "duck-prefix" {
		t.Fatalf("expected DUCKDB_MANIFEST_PREFIX override, got %q", got.ManifestPrefix)
	}
	if got.ManifestTemplate != "duck-template" {
		t.Fatalf("expected DUCKDB_MANIFEST_TEMPLATE override, got %q", got.ManifestTemplate)
	}
}

// TestDuckDBConfigFromEnv_ManifestOffByDefault pins that setting only
// DUCKDB_ENABLED does not enable manifest reads (ManifestReadEnabled() == false).
func TestDuckDBConfigFromEnv_ManifestOffByDefault(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.ManifestReadEnabled() {
		t.Fatalf("expected manifest reads disabled by default, got enabled")
	}
}

// TestDuckDBConfigFromEnv_SharedPrefixesIgnoredWithoutTemplate is the upgrade
// safety pin: an existing single-stack deployment exports S3_PREFIX (and maybe
// MANIFEST_PREFIX) for the CDC tooling and turns DUCKDB_ENABLED on. Adopting
// those shared names without a manifest template would produce an inert config
// that ValidateManifestRead rejects, so the server would refuse to boot after
// upgrading. The shared-name fallbacks are all-or-nothing with the template.
func TestDuckDBConfigFromEnv_SharedPrefixesIgnoredWithoutTemplate(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("S3_PREFIX", "delta")
	t.Setenv("MANIFEST_PREFIX", "x")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.S3DataPrefix != "" {
		t.Fatalf("expected S3DataPrefix ignored without a template, got %q", got.S3DataPrefix)
	}
	if got.ManifestPrefix != "" {
		t.Fatalf("expected ManifestPrefix ignored without a template, got %q", got.ManifestPrefix)
	}
	if got.ManifestReadEnabled() {
		t.Fatalf("expected manifest reads disabled without a template")
	}
	if err := got.ValidateManifestRead(); err != nil {
		t.Fatalf("expected an upgraded single-stack config to still boot, got %v", err)
	}
}

// TestDuckDBConfigFromEnv_SharedPrefixesAdoptedWithTemplate pins the other half
// of the all-or-nothing rule: once a manifest template is in play, the shared
// prefix names are the single-stack configuration point and are adopted.
func TestDuckDBConfigFromEnv_SharedPrefixesAdoptedWithTemplate(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("S3_PREFIX", "delta")
	t.Setenv("MANIFEST_PREFIX", "manifests")
	t.Setenv("MANIFEST_TEMPLATE", "manifest/{{.SchemaID}}.json")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.S3DataPrefix != "delta" {
		t.Fatalf("expected S3DataPrefix from S3_PREFIX, got %q", got.S3DataPrefix)
	}
	if got.ManifestPrefix != "manifests" {
		t.Fatalf("expected ManifestPrefix from MANIFEST_PREFIX, got %q", got.ManifestPrefix)
	}
	if !got.ManifestReadEnabled() {
		t.Fatalf("expected manifest reads enabled with a template")
	}
}

// TestDuckDBConfigFromEnv_ExplicitPrefixKeptWithoutTemplate pins that the
// DUCKDB_-prefixed names are always adopted, template or not. The resulting
// config is inert and the factory rejects it at startup — that is the strict
// rule working as intended: an operator who typed DUCKDB_S3_PREFIX by hand
// gets told the template is missing rather than having the value dropped.
func TestDuckDBConfigFromEnv_ExplicitPrefixKeptWithoutTemplate(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("DUCKDB_S3_PREFIX", "delta")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.S3DataPrefix != "delta" {
		t.Fatalf("expected explicit DUCKDB_S3_PREFIX kept, got %q", got.S3DataPrefix)
	}
	if err := got.ValidateManifestRead(); err == nil {
		t.Fatalf("expected an explicitly-set inert config to be rejected")
	}
}

// TestDuckDBConfigFromEnv_ExplicitManifestPrefixKeptWithoutTemplate mirrors the
// previous pin for DUCKDB_MANIFEST_PREFIX.
func TestDuckDBConfigFromEnv_ExplicitManifestPrefixKeptWithoutTemplate(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "1")
	t.Setenv("DUCKDB_MANIFEST_PREFIX", "manifests")

	got := duckDBConfigFromEnv(forma.DuckDBConfig{})
	if got.ManifestPrefix != "manifests" {
		t.Fatalf("expected explicit DUCKDB_MANIFEST_PREFIX kept, got %q", got.ManifestPrefix)
	}
	if err := got.ValidateManifestRead(); err == nil {
		t.Fatalf("expected an explicitly-set inert config to be rejected")
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
