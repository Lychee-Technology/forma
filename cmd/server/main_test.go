package main

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"go.uber.org/zap"
)

// readDuckDBConfig is duckDBConfigFromEnv for a test whose environment is
// valid: an error fails the test.
func readDuckDBConfig(t *testing.T, base forma.DuckDBConfig) forma.DuckDBConfig {
	t.Helper()
	cfg, err := duckDBConfigFromEnv(base)
	if err != nil {
		t.Fatalf("duckDBConfigFromEnv: %v", err)
	}
	return cfg
}

// TestDuckDBConfigFromEnv_DisabledByDefault pins that the federated DuckDB
// engine stays off unless DUCKDB_ENABLED is set — the production default.
func TestDuckDBConfigFromEnv_DisabledByDefault(t *testing.T) {
	if got := readDuckDBConfig(t, forma.DuckDBConfig{Enabled: false}); got.Enabled {
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

	got := readDuckDBConfig(t, forma.DuckDBConfig{})
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

	if got := readDuckDBConfig(t, forma.DuckDBConfig{}); got.S3Endpoint != "http://duck:9000" {
		t.Fatalf("expected DUCKDB_S3_ENDPOINT override, got %q", got.S3Endpoint)
	}
}

// TestDuckDBConfigFromEnv_AllowPartialDegradedMode pins #715: the degraded
// fallback is deployment configuration, off unless
// DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE is set, and turning it on flips the flag.
func TestDuckDBConfigFromEnv_AllowPartialDegradedMode(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "true")
	if got := readDuckDBConfig(t, forma.DuckDBConfig{}); got.AllowPartialDegradedMode {
		t.Fatalf("expected AllowPartialDegradedMode off by default, got true")
	}

	t.Setenv("DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE", "true")
	if got := readDuckDBConfig(t, forma.DuckDBConfig{}); !got.AllowPartialDegradedMode {
		t.Fatalf("expected AllowPartialDegradedMode on when DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE=true")
	}
}

// TestDuckDBConfigFromEnv_RoutingStrategy pins #715's deployment-level
// route: DUCKDB_ROUTING_STRATEGY sets the engine's strategy, unset keeps the
// base value, and an unknown name is refused rather than left to the
// engine's send-everything-to-DuckDB default.
func TestDuckDBConfigFromEnv_RoutingStrategy(t *testing.T) {
	base := forma.DefaultConfig(nil).DuckDB
	t.Setenv("DUCKDB_ENABLED", "true")
	if got := readDuckDBConfig(t, base).Routing.Strategy; got != forma.RoutingStrategyHybrid {
		t.Fatalf("expected the base hybrid strategy kept, got %q", got)
	}
	t.Setenv("DUCKDB_ROUTING_STRATEGY", "freshness-first")
	if got := readDuckDBConfig(t, base).Routing.Strategy; got != forma.RoutingStrategyFreshnessFirst {
		t.Fatalf("expected freshness-first, got %q", got)
	}
	t.Setenv("DUCKDB_ROUTING_STRATEGY", "duckdb-always")
	got, err := duckDBConfigFromEnv(base)
	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) || envErr.Key != "DUCKDB_ROUTING_STRATEGY" {
		t.Fatalf("expected an EnvError naming DUCKDB_ROUTING_STRATEGY, got %v", err)
	}
	if !reflect.DeepEqual(got, base) {
		t.Fatalf("expected the base config back untouched, got %+v", got)
	}
}

// TestDuckDBConfigFromEnv_RejectsUnrecognisedBoolean pins #642 on both DuckDB
// switches. DUCKDB_ENABLED=yes used to leave the engine off; it and
// DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE=on are each an *EnvError, and the base
// config comes back untouched. The opt-in is judged
// alongside a refused DUCKDB_ENABLED, so one boot names both, but stays unread
// while DuckDB is off.
func TestDuckDBConfigFromEnv_RejectsUnrecognisedBoolean(t *testing.T) {
	base := forma.DuckDBConfig{S3Bucket: "base-bucket"}
	cases := []struct {
		name    string
		enabled string
		allow   string
		want    []string
	}{
		{name: "enabled refused", enabled: "yes", want: []string{"DUCKDB_ENABLED"}},
		{name: "opt-in refused", enabled: "true", allow: "on", want: []string{"DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE"}},
		{name: "both refused", enabled: "on", allow: "yes", want: []string{"DUCKDB_ENABLED", "DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE"}},
		{name: "opt-in unread while off", enabled: "false", allow: "on"},
		{name: "opt-in unread while unset", allow: "on"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DUCKDB_ENABLED", tc.enabled)
			t.Setenv("DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE", tc.allow)
			got, err := duckDBConfigFromEnv(base)
			if !reflect.DeepEqual(got, base) {
				t.Fatalf("expected the base config back untouched, got %+v", got)
			}
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("DuckDB is off, so the opt-in must not be judged: %v", err)
				}
				return
			}
			for _, key := range tc.want {
				if !strings.Contains(err.Error(), key+"=") {
					t.Fatalf("expected the error to name %s, got %v", key, err)
				}
			}
			var envErr *bootstrap.EnvError
			if !errors.As(err, &envErr) {
				t.Fatalf("expected an *bootstrap.EnvError, got %v", err)
			}
		})
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

// TestDuckDBConfigFromEnv_HotLegTransport: the server keeps the default's
// pgx transport unless DUCKDB_HOT_LEG_TRANSPORT names another, and reads the
// variable only when DuckDB is on.
func TestDuckDBConfigFromEnv_HotLegTransport(t *testing.T) {
	base := forma.DefaultConfig(nil).DuckDB
	t.Setenv("DUCKDB_ENABLED", "true")
	if got := readDuckDBConfig(t, base).HotLegTransport; got != forma.HotLegTransportPgx {
		t.Fatalf("unset DUCKDB_HOT_LEG_TRANSPORT: transport %q, want %q", got, forma.HotLegTransportPgx)
	}
	t.Setenv("DUCKDB_HOT_LEG_TRANSPORT", "attach")
	if got := readDuckDBConfig(t, base).HotLegTransport; got != "attach" {
		t.Fatalf("DUCKDB_HOT_LEG_TRANSPORT=attach: transport %q, want it read so validation refuses it", got)
	}
	t.Setenv("DUCKDB_ENABLED", "false")
	if got := readDuckDBConfig(t, base).HotLegTransport; got != forma.HotLegTransportPgx {
		t.Fatalf("DuckDB off: transport %q, want the base's %q", got, forma.HotLegTransportPgx)
	}
}

// TestBootstrapServer_RejectsAHotLegTransportOtherThanPgx: the retired
// attach rollback (#858) and any unknown value fail startup with a
// ConfigError naming the field, so a deployment left on attach does not move
// to pgx silently.
func TestBootstrapServer_RejectsAHotLegTransportOtherThanPgx(t *testing.T) {
	for _, value := range []string{"attach", "duckdb"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("DUCKDB_ENABLED", "true")
			t.Setenv("DUCKDB_HOT_LEG_TRANSPORT", value)
			_, err := bootstrapServer(context.Background(), zap.NewNop().Sugar())
			var configErr *forma.ConfigError
			if !errors.As(err, &configErr) || configErr.Field != "duckdb.hotLegTransport" {
				t.Fatalf("expected a ConfigError on duckdb.hotLegTransport, got %v", err)
			}
		})
	}
}

// TestBootstrapServer_RejectsDuckDBWithoutABucket: with DuckDB on, the
// federated reader scans the published generations in the bucket, so a
// server without one fails startup with a ConfigError naming it, before any
// I/O.
func TestBootstrapServer_RejectsDuckDBWithoutABucket(t *testing.T) {
	t.Setenv("DUCKDB_ENABLED", "true")
	_, err := bootstrapServer(context.Background(), zap.NewNop().Sugar())
	var configErr *forma.ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "duckdb.s3Bucket" {
		t.Fatalf("expected a ConfigError on duckdb.s3Bucket, got %v", err)
	}
}

// TestBootstrapServer_RejectsUnparsableIntegerEnv pins #600: a set but
// unparsable integer variable fails startup with an *EnvError naming it,
// where it used to keep the default silently. One variable per overlay the
// server applies (pool, limits, http.Server), so all three are wired, and
// all three are named at once. The context is already canceled, so the env
// check is shown to run before any I/O: reaching the pool would report
// context.Canceled instead.
func TestBootstrapServer_RejectsUnparsableIntegerEnv(t *testing.T) {
	t.Setenv("DB_MAX_CONNECTIONS", "twenty")
	t.Setenv("QUERY_TIMEOUT_SECONDS", "30s")
	t.Setenv("HTTP_MAX_HEADER_BYTES", "1_000")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bootstrapServer(ctx, zap.NewNop().Sugar())

	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) {
		t.Fatalf("expected an *bootstrap.EnvError, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("the environment must be refused before the pool is opened, got %v", err)
	}
	for _, want := range []string{`DB_MAX_CONNECTIONS="twenty"`, `QUERY_TIMEOUT_SECONDS="30s"`, `HTTP_MAX_HEADER_BYTES="1_000"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("startup error must name %s, got %v", want, err)
		}
	}
}

// TestBootstrapServer_RejectsUnrecognisedBooleanEnv pins #642 on the server:
// each of its three boolean overlays (entity, DuckDB, metrics) fails startup
// with an *EnvError naming the variable, where the value used to read as
// false. They join the integer overlays' failures, so one boot names a bad
// boolean and a bad integer together. The context is already canceled, so
// the check is shown to run before any I/O.
func TestBootstrapServer_RejectsUnrecognisedBooleanEnv(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "yes")
	t.Setenv("DUCKDB_ENABLED", "on")
	t.Setenv("METRICS_STDOUT", "TRUE ")
	t.Setenv("QUERY_TIMEOUT_SECONDS", "30s")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bootstrapServer(ctx, zap.NewNop().Sugar())

	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) {
		t.Fatalf("expected an *bootstrap.EnvError, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("the environment must be refused before the pool is opened, got %v", err)
	}
	for _, want := range []string{`VALIDATE_UPDATES_STRICT="yes"`, `DUCKDB_ENABLED="on"`, `METRICS_STDOUT="TRUE "`, `QUERY_TIMEOUT_SECONDS="30s"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("startup error must name %s, got %v", want, err)
		}
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
