// Package main provides the AWS Lambda entry point for the Forma API server.
// It uses aws-lambda-go-api-proxy to adapt the existing HTTP handlers to Lambda's
// API Gateway v2 (HTTP API) event format.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	dsqlauth "github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/httpapi"
	"go.uber.org/zap"
)

var (
	httpAdapter *httpadapter.HandlerAdapterV2
)

type lambdaRuntime struct {
	adapter *httpadapter.HandlerAdapterV2
}

func bootstrapLambda(ctx context.Context, sugar *zap.SugaredLogger) (*lambdaRuntime, error) {
	// Get configuration from environment variables
	schemaDir := bootstrap.Env("SCHEMA_DIR", "")
	if schemaDir == "" {
		schemaDir = "/var/task/schemas"
	}
	sugar.Infof("schemaDir: %s", schemaDir)

	// The whole configuration, the pool settings included, is resolved and
	// validated before the pool is opened, so an out-of-range limit or an
	// unparsable integer variable fails the cold start here instead of
	// silently widening a protection or keeping a default (#465, #600).
	formaConfig, dbConfig, err := lambdaConfigFromEnv(schemaDir)
	if err != nil {
		return nil, err
	}
	tableNames := formaConfig.Database.TableNames

	startupTimeout := dbConfig.Timeout
	if startupTimeout <= 0 {
		startupTimeout = 30 * time.Second
	}
	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	// Check if we're using Aurora DSQL (indicated by DSQL_ENDPOINT env var)
	var dbPool *pgxpool.Pool
	if dsqlEndpoint := bootstrap.Env("DSQL_ENDPOINT", ""); dsqlEndpoint != "" {
		// Aurora DSQL mode - use IAM authentication
		sugar.Infof("Using Aurora DSQL endpoint: %s", dsqlEndpoint)
		dbPool, err = createDSQLPool(startupCtx, dsqlEndpoint, dbConfig)
	} else {
		// Traditional PostgreSQL mode - use password authentication
		dbPool, err = bootstrap.NewPostgresPoolFromConfigContext(startupCtx, dbConfig)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}

	// Create file-based schema registry from database
	registry, err := schemameta.NewFileSchemaRegistryContext(startupCtx, dbPool, tableNames.SchemaRegistry, schemaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry: %w", err)
	}
	formaConfig.SchemaRegistry = registry

	// Initialize EntityManager using factory. Deliberately never Closed (#302):
	// the manager is process-lifetime — bootstrapLambda runs once per execution
	// environment — and the Go Lambda runtime has no shutdown hook to run
	// cleanup from (SIGTERM reaches the function process only when an external
	// extension is registered; otherwise the frozen sandbox is reclaimed with
	// the process, DuckDB instance included).
	manager, err := factory.NewEntityManagerWithConfigContext(startupCtx, formaConfig, dbPool)
	if err != nil {
		return nil, fmt.Errorf("failed to create entity manager: %w", err)
	}

	// Create server and register routes
	server := httpapi.NewServer(manager, httpapi.Options{
		EnableHealth: true,
		MaxBodyBytes: int64(formaConfig.Entity.MaxEntitySize),
	})

	// Create HTTP adapter for API Gateway v2
	httpAdapter = httpadapter.NewV2(server.Handler())

	sugar.Info("Lambda handler initialized successfully")
	return &lambdaRuntime{
		adapter: httpAdapter,
	}, nil
}

// lambdaDBDefaults are the pool settings an execution environment starts from,
// in both database modes: one environment serves one request at a time, so it
// holds few connections and recycles them quickly. Aurora DSQL reads only the
// pool sizing, lifetimes and timeout; its endpoint and IAM token replace the
// host, port and credentials.
var lambdaDBDefaults = bootstrap.DBDefaults{
	Host:                   "localhost",
	Port:                   5432,
	Database:               "forma",
	Username:               "postgres",
	Password:               "",
	SSLMode:                "require",
	Schema:                 "public",
	MaxConnections:         10,
	MaxIdleConns:           2,
	ConnMaxLifetimeSeconds: 300,
	ConnMaxIdleTimeSeconds: 60,
	TimeoutSeconds:         30,
}

// lambdaConfigFromEnv assembles and validates the forma.Config and the pool
// settings this entry point starts with. It performs no I/O, so bootstrapLambda
// calls it before opening the pool; the schema registry is the one field it
// cannot fill, and bootstrapLambda sets it once the pool exists. Extracted
// from bootstrapLambda to keep that function inside the 100-line cap.
func lambdaConfigFromEnv(schemaDir string) (*forma.Config, forma.DatabaseConfig, error) {
	config := forma.DefaultConfig(nil)

	// Table names configuration
	tableNames := bootstrap.TableNamesFromEnv(forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
		ChangeLog:      "change_log",
	})

	// Set entity options from the environment, then the schema directory. This
	// entry point serves the same write routes as cmd/server, so it must honour
	// VALIDATE_UPDATES_STRICT too or the #314 staged rollout is unavailable on
	// Lambda. The overlay runs first so it cannot clobber the directory below.
	config.Entity = bootstrap.EntityConfigFromEnv(config.Entity)
	config.Entity.SchemaDirectory = schemaDir

	// Database schema used by factory table discovery.
	config.Database.Schema = bootstrap.Env("DB_SCHEMA", config.Database.Schema)
	config.Database.TableNames = tableNames

	// METRICS_STDOUT=true makes every metric this instance emits a JSON line
	// on stdout, which Lambda forwards to the function's log group (#423);
	// unset, Forma's no-op default emits nothing.
	config.Metrics.Emitter = bootstrap.MetricEmitterFromEnv(os.Stdout)

	// Request limits and budgets (#465), the same overlay cmd/server applies;
	// API Gateway bounds the connection itself, so there is no http.Server
	// to configure here.
	limitsErr := bootstrap.ApplyLimitsFromEnv(config)

	// The pool settings, read in both database modes so a DSQL deployment's
	// DB_* sizing is parsed before any I/O too (#600).
	dbConfig, dbErr := bootstrap.DatabaseConfigFromEnv(lambdaDBDefaults)

	// Both overlays are read before either is judged, so one cold start
	// names every unparsable variable.
	if err := errors.Join(dbErr, limitsErr); err != nil {
		return nil, forma.DatabaseConfig{}, fmt.Errorf("invalid environment: %w", err)
	}

	// Validate covers every rule the manager relies on, so a rejection names
	// the field at cold start rather than surfacing on the first request.
	if err := config.Validate(); err != nil {
		return nil, forma.DatabaseConfig{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return config, dbConfig, nil
}

// handler is the Lambda handler function
func handler(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	if httpAdapter == nil {
		return events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       `{"error":"lambda handler not initialized"}`,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}, nil
	}
	return httpAdapter.ProxyWithContext(ctx, req)
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

	sugar.Info("initializing Lambda handler")

	runtime, err := bootstrapLambda(context.Background(), sugar)
	if err != nil {
		sugar.Fatalf("failed to bootstrap lambda runtime: %v", err)
	}
	httpAdapter = runtime.adapter

	lambda.Start(handler)
}

// createDSQLPool creates a connection pool for Aurora DSQL using IAM authentication.
// DSQL requires IAM auth tokens instead of passwords, and always uses:
// - Database: postgres (fixed)
// - User: admin (default admin user)
// - SSL: required
//
// settings supplies the pool sizing, lifetimes and connect timeout, resolved
// from the environment before any I/O; its host, port and credentials are
// not read.
func createDSQLPool(ctx context.Context, endpoint string, settings forma.DatabaseConfig) (*pgxpool.Pool, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("dsql bootstrap context: %w", err)
	}

	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(bootstrap.Env("AWS_REGION", "us-east-2")))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Generate IAM auth token for DSQL
	// The token is valid for 15 minutes to 7 days (default: 15 minutes)
	token, err := dsqlauth.GenerateDBConnectAdminAuthToken(ctx, endpoint, awsCfg.Region, awsCfg.Credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DSQL auth token: %w", err)
	}

	// Build connection string for DSQL
	// DSQL always uses: database=postgres, user=admin, sslmode=require
	connString := fmt.Sprintf(
		"postgres://admin:%s@%s:5432/postgres?sslmode=require",
		url.QueryEscape(token),
		endpoint,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSQL connection string: %w", err)
	}

	// Configure pool for Lambda - use conservative settings. DB_MAX_IDLE_CONNS
	// is the warm floor here.
	poolConfig.MaxConns = int32(settings.MaxConnections)
	poolConfig.MinConns = int32(settings.MaxIdleConns)
	poolConfig.MaxConnLifetime = settings.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = settings.ConnMaxIdleTime
	poolConfig.ConnConfig.ConnectTimeout = settings.Timeout

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create DSQL connection pool: %w", err)
	}

	// Test the connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("failed to ping DSQL database: %w", err)
	}

	return pool, nil
}
