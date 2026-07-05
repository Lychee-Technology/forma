package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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

	// Set schema directory
	config.Entity.SchemaDirectory = schemaDir

	// Set database configuration
	config.Database = dbConfig
	config.Database.TableNames = tableNames
	config.SchemaRegistry = registry

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
