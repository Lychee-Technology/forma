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
	pool   *pgxpool.Pool
	server *httpapi.Server
}

func main() {
	logger, err := zap.NewProduction()
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

	port := bootstrap.Env("PORT", "8080")
	zap.S().Infow("starting server", "port", port)
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: runtime.server.Handler(),
	}
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

	// Database configuration
	dbConfig := bootstrap.DatabaseConfigFromEnv(bootstrap.DBDefaults{
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

	// Table names configuration
	tableNames := bootstrap.TableNamesFromEnv(forma.TableNames{
		SchemaRegistry: "schema_registry_dev",
		EAVData:        "eav_data_dev",
		EntityMain:     "entity_main_dev",
		ChangeLog:      "change_log_dev",
	})

	// Invariant: the DuckDB manifest read surface is resolved and validated
	// before the server opens any connection. The factory validates it too, but
	// only after it has already queried the database — so doing it here is what
	// makes the documented "invalid configuration fails at startup, before any
	// I/O" contract (docs/federated-query/design.md §4.3.1) literally true for
	// the server. Keep this block above NewPostgresPoolFromConfigContext.
	duckCfg := duckDBConfigFromEnv(forma.DefaultConfig(nil).DuckDB)
	if err := duckCfg.ValidateManifestRead(); err != nil {
		return nil, fmt.Errorf("invalid duckdb manifest configuration: %w", err)
	}

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
	registry, err := schemameta.NewFileSchemaRegistryContext(startupCtx, pool, tableNames.SchemaRegistry, schemaDir)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create schema registry: %w", err)
	}

	// Load configuration with schema registry
	config := forma.DefaultConfig(registry)

	// Set entity options from the environment, then the schema directory. The
	// overlay runs first so it cannot clobber the directory resolved above.
	config.Entity = bootstrap.EntityConfigFromEnv(config.Entity)
	config.Entity.SchemaDirectory = schemaDir

	// Set database configuration
	config.Database = dbConfig
	config.Database.TableNames = tableNames
	config.SchemaRegistry = registry

	// Enable the federated DuckDB engine when configured (disabled by default).
	// This lets a deployment exercise the real warm/cold S3 read path; the e2e
	// suite turns it on so its federated checks are genuinely federated. The
	// value was resolved and validated above, before any I/O; re-resolving it
	// here would risk the two copies drifting apart.
	config.DuckDB = duckCfg

	// Initialize EntityManager with the same pool used by schema registry.
	manager, err := factory.NewEntityManagerWithConfigContext(startupCtx, config, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create entity manager: %w", err)
	}

	return &serverRuntime{
		pool:   pool,
		server: httpapi.NewServer(manager, httpapi.Options{EnableHealth: true}),
	}, nil
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
