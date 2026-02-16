package main

import (
	"net/http"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/httpapi"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	zap.ReplaceGlobals(logger)
	sugar := logger.Sugar()

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

	// Create database connection pool
	pool, err := bootstrap.NewPostgresPoolFromConfig(dbConfig)
	if err != nil {
		sugar.Fatalf("failed to create database pool: %v", err)
	}
	defer pool.Close()

	// Create file-based schema registry from database
	registry, err := internal.NewFileSchemaRegistry(pool, tableNames.SchemaRegistry, schemaDir)
	if err != nil {
		sugar.Fatalf("failed to create schema registry: %v", err)
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
	manager, err := factory.NewEntityManagerWithConfig(config, pool)
	if err != nil {
		sugar.Fatalf("failed to create entity manager: %v", err)
	}

	server := httpapi.NewServer(manager, httpapi.Options{})

	port := bootstrap.Env("PORT", "8080")
	zap.S().Infow("starting server", "port", port)
	if err := http.ListenAndServe(":"+port, server.Handler()); err != nil {
		sugar.Fatalf("server error: %v", err)
	}
}
