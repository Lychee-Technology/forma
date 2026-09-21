package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/httpapi"
	"go.uber.org/zap"
)

type serverRuntime struct {
	pool    *pgxpool.Pool
	manager forma.EntityManager
	server  *httpapi.Server
	// httpCfg holds the validated http.Server bounds main listens with.
	httpCfg bootstrap.HTTPServerConfig
}

func main() {
	logger, err := factory.NewProductionLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	sugar := logger.Sugar()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtime, err := bootstrapServer(rootCtx, sugar)
	if err != nil {
		sugar.Fatalf("failed to bootstrap server: %v", err)
	}
	defer runtime.pool.Close()
	// Defers run LIFO, so the manager — whose DuckDB holds a Postgres
	// attachment DSN — is released before the pool it points at (#302).
	defer func() {
		if err := runtime.manager.Close(); err != nil {
			sugar.Warnw("failed to close entity manager", "err", err)
		}
	}()

	port := bootstrap.Env("PORT", "8080")
	zap.S().Infow("starting server", "port", port)
	srv := bootstrap.NewHTTPServer(":"+port, runtime.server.Handler(), runtime.httpCfg)
	if err := runServer(rootCtx, srv); err != nil {
		sugar.Fatalf("server error: %v", err)
	}
}

// duckDBConfigFromEnv turns on the federated DuckDB engine when DUCKDB_ENABLED
// is set, wiring its S3/httpfs and manifest settings from the environment. When
// disabled it returns base unchanged (DuckDB off — the production default).
//
// Each field reads its DUCKDB_-prefixed name first, then a shared name reserved
// as the single-stack configuration point (future CDC runners may read the same
// names), then the base value. The two prefix fields — S3DataPrefix and
// ManifestPrefix — deviate: a prefix set without a manifest template is inert
// and ValidateManifestRead rejects it, so adopting a shared prefix on its own
// would stop an existing deployment from booting after an upgrade. Therefore:
//
//   - DUCKDB_S3_PREFIX / DUCKDB_MANIFEST_PREFIX are always adopted. An
//     explicitly-named inert value is a misconfiguration the operator should
//     hear about, so the factory's startup rejection is the intended outcome.
//   - S3_PREFIX / MANIFEST_PREFIX are adopted only when the effective manifest
//     template (resolved first, from DUCKDB_MANIFEST_TEMPLATE, then
//     MANIFEST_TEMPLATE, then base) is non-empty — all-or-nothing with the
//     template that gives them meaning.
//
// S3_BUCKET has no such condition: a bucket alone never makes the config inert.
// DUCKDB_MANIFEST_TEMPLATE is the switch for manifest-driven reads; if set
// without a bucket, the server fails at startup (factory fail-fast).
func duckDBConfigFromEnv(base forma.DuckDBConfig) forma.DuckDBConfig {
	if !bootstrap.EnvBool("DUCKDB_ENABLED", false) {
		return base
	}
	base.Enabled = true
	base.EnableS3 = true
	base.EnableParquet = true
	base.S3Endpoint = bootstrap.Env("DUCKDB_S3_ENDPOINT", bootstrap.Env("S3_ENDPOINT", base.S3Endpoint))
	base.S3AccessKey = bootstrap.Env("DUCKDB_S3_ACCESS_KEY", bootstrap.Env("S3_ACCESS_KEY", base.S3AccessKey))
	base.S3SecretKey = bootstrap.Env("DUCKDB_S3_SECRET_KEY", bootstrap.Env("S3_SECRET_KEY", base.S3SecretKey))
	base.S3Region = bootstrap.Env("DUCKDB_S3_REGION", "us-east-1")
	base.S3Bucket = bootstrap.Env("DUCKDB_S3_BUCKET", bootstrap.Env("S3_BUCKET", base.S3Bucket))

	base.ManifestTemplate = bootstrap.Env("DUCKDB_MANIFEST_TEMPLATE", bootstrap.Env("MANIFEST_TEMPLATE", base.ManifestTemplate))
	manifestOn := strings.TrimSpace(base.ManifestTemplate) != ""
	base.S3DataPrefix = prefixFromEnv("DUCKDB_S3_PREFIX", "S3_PREFIX", base.S3DataPrefix, manifestOn)
	base.ManifestPrefix = prefixFromEnv("DUCKDB_MANIFEST_PREFIX", "MANIFEST_PREFIX", base.ManifestPrefix, manifestOn)
	base.AllowCallerParquetPaths = bootstrap.EnvBool("DUCKDB_ALLOW_CALLER_PARQUET_PATHS", base.AllowCallerParquetPaths)
	return base
}

// prefixFromEnv resolves one of the two inert-when-alone prefix fields. The
// DUCKDB_-prefixed name always wins; the shared name is consulted only when a
// manifest template is in effect. See duckDBConfigFromEnv for why.
func prefixFromEnv(duckDBName, sharedName, baseValue string, manifestOn bool) string {
	if explicit := bootstrap.Env(duckDBName, ""); explicit != "" {
		return explicit
	}
	if manifestOn {
		return bootstrap.Env(sharedName, baseValue)
	}
	return baseValue
}

func bootstrapServer(ctx context.Context, sugar *zap.SugaredLogger) (*serverRuntime, error) {
	// Get configuration from environment variables
	schemaDir := bootstrap.Env("SCHEMA_DIR", "")
	sugar.Infof("schemaDir: %s", schemaDir)

	// Invariant: the whole configuration is resolved and validated before the
	// server opens any connection. The factory validates the DuckDB manifest
	// surface too, but only after it has already queried the database, so
	// doing it here is what makes the documented "invalid configuration fails
	// at startup, before any I/O" contract (docs/federated-query/design.md
	// §4.3.1, README request limits) literally true for the server. Keep this
	// call above NewPostgresPoolFromConfigContext.
	config, httpCfg, err := serverConfigFromEnv(schemaDir)
	if err != nil {
		return nil, err
	}
	dbConfig := config.Database

	startupTimeout := dbConfig.Timeout
	if startupTimeout <= 0 {
		startupTimeout = 30 * time.Second
	}
	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	pool, err := bootstrap.NewPostgresPoolFromConfigContext(startupCtx, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}

	// Create file-based schema registry from database
	registry, err := schemameta.NewFileSchemaRegistryContext(startupCtx, pool, dbConfig.TableNames.SchemaRegistry, schemaDir)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create schema registry: %w", err)
	}
	config.SchemaRegistry = registry

	// Initialize EntityManager with the same pool used by schema registry.
	manager, err := factory.NewEntityManagerWithConfigContext(startupCtx, config, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create entity manager: %w", err)
	}

	return &serverRuntime{
		pool:    pool,
		manager: manager,
		httpCfg: httpCfg,
		server: httpapi.NewServer(manager, httpapi.Options{
			EnableHealth: true,
			MaxBodyBytes: int64(config.Entity.MaxEntitySize),
		}),
	}, nil
}

// serverConfigFromEnv assembles the forma.Config and the http.Server bounds
// this entry point starts with, entirely from the environment and defaults,
// and validates both. It performs no I/O, so bootstrapServer can call it
// before opening the database and an out-of-range value (a negative budget,
// a zero body cap, a write timeout that does not outlast the body read plus
// a budget) fails at boot instead of silently widening a limit (#465). The
// schema registry is the one field it cannot fill; bootstrapServer sets it
// once the pool exists.
func serverConfigFromEnv(schemaDir string) (*forma.Config, bootstrap.HTTPServerConfig, error) {
	config := forma.DefaultConfig(nil)

	// Set entity options from the environment, then the schema directory. The
	// overlay runs first so it cannot clobber the directory passed in.
	config.Entity = bootstrap.EntityConfigFromEnv(config.Entity)
	config.Entity.SchemaDirectory = schemaDir

	// Database configuration
	config.Database = bootstrap.DatabaseConfigFromEnv(bootstrap.DBDefaults{
		Host:                   "localhost",
		Port:                   5432,
		Database:               "forma",
		Username:               "postgres",
		Password:               "",
		SSLMode:                "disable",
		Schema:                 "public",
		MaxConnections:         25,
		MaxIdleConns:           5,
		ConnMaxLifetimeSeconds: 3600,
		ConnMaxIdleTimeSeconds: 300,
		TimeoutSeconds:         30,
	})
	config.Database.TableNames = bootstrap.TableNamesFromEnv(forma.TableNames{
		SchemaRegistry: "schema_registry_dev",
		EAVData:        "eav_data_dev",
		EntityMain:     "entity_main_dev",
		ChangeLog:      "change_log_dev",
	})

	// Enable the federated DuckDB engine when configured (disabled by default).
	// This lets a deployment exercise the real warm/cold S3 read path; the e2e
	// suite turns it on so its federated checks are genuinely federated.
	config.DuckDB = duckDBConfigFromEnv(config.DuckDB)

	// METRICS_STDOUT=true makes every metric this instance emits a JSON line
	// on stdout (#423); unset, Forma's no-op default emits nothing.
	config.Metrics.Emitter = bootstrap.MetricEmitterFromEnv(os.Stdout)

	// Request limits and budgets (#465): body cap, batch cap, query,
	// transaction and DuckDB timeouts. Applied last so it sees the resolved
	// DuckDB config.
	bootstrap.ApplyLimitsFromEnv(config)

	// Validate covers every rule the manager relies on, the DuckDB manifest
	// read surface and the #456 caller-path opt-in included, so a rejection
	// here names the field rather than surfacing on the first request.
	if err := config.Validate(); err != nil {
		return nil, bootstrap.HTTPServerConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}

	// Every connection phase is bounded (#465); the defaults and the HTTP_*
	// overrides are documented in the README. The manager's own per-request
	// budgets run underneath these, since a server timeout never cancels a
	// handler's context, which is why WriteTimeout has to cover them, and
	// the body read that precedes them (HTTPServerConfig.Validate).
	httpCfg := bootstrap.HTTPServerConfigFromEnv(bootstrap.DefaultHTTPServerConfig())
	if err := httpCfg.Validate(config); err != nil {
		return nil, bootstrap.HTTPServerConfig{}, fmt.Errorf("invalid http server configuration: %w", err)
	}
	return config, httpCfg, nil
}

// runServer starts srv in a background goroutine and blocks until either the
// server fails or ctx is cancelled. On cancellation it calls Shutdown with a
// 5-second grace period, allowing in-flight requests to complete.
func runServer(ctx context.Context, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
