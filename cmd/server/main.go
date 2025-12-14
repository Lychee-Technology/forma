package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"go.uber.org/zap"
)

// Server represents the HTTP server with EntityManager
type Server struct {
	manager forma.EntityManager
	mux     *http.ServeMux
}

// NewServer creates a new Server instance
func NewServer(manager forma.EntityManager) *Server {
	return &Server{
		manager: manager,
		mux:     http.NewServeMux(),
	}
}

// RegisterRoutes registers all API routes
func (s *Server) RegisterRoutes() {
	// API routes - use custom path matching in handlers
	s.mux.HandleFunc("/api/v1/advanced_query", s.handleAdvancedQuery)
	s.mux.HandleFunc("/api/v1/search", s.handleSearch)
	s.mux.HandleFunc("/api/v1/", s.apiHandler)
}

// Start starts the HTTP server on the given port
func (s *Server) Start(port string) error {
	zap.S().Infow("starting server", "port", port)
	return http.ListenAndServe(":"+port, s.mux)
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)
	sugar := logger.Sugar()

	// Get configuration from environment variables
	schemaDir := os.Getenv("SCHEMA_DIR")
	sugar.Infof("schemaDir: %s", schemaDir)

	// Database configuration
	dbConfig := forma.DatabaseConfig{
		Host:            getEnv("DB_HOST", "localhost"),
		Port:            getEnvInt("DB_PORT", 5432),
		Database:        getEnv("DB_NAME", "forma"),
		Username:        getEnv("DB_USER", "postgres"),
		Password:        getEnv("DB_PASSWORD", ""),
		SSLMode:         getEnv("DB_SSL_MODE", "disable"),
		MaxConnections:  getEnvInt("DB_MAX_CONNECTIONS", 25),
		MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime: time.Duration(getEnvInt("DB_CONN_MAX_LIFETIME_SECONDS", 3600)) * time.Second,
		ConnMaxIdleTime: time.Duration(getEnvInt("DB_CONN_MAX_IDLE_TIME_SECONDS", 300)) * time.Second,
		Timeout:         time.Duration(getEnvInt("DB_TIMEOUT_SECONDS", 30)) * time.Second,
	}

	// Table names configuration
	tableNames := forma.TableNames{
		SchemaRegistry: getEnv("SCHEMA_TABLE", "schema_registry_dev"),
		EAVData:        getEnv("EAV_TABLE", "eav_data_dev"),
		EntityMain:     getEnv("ENTITY_MAIN_TABLE", "entity_main_dev"),
	}

	// Create database connection pool
	pool, err := createDatabasePoolFromConfig(dbConfig)
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

	// Initialize EntityManager
	manager := NewEntityManager(config)

	server := NewServer(manager)
	server.RegisterRoutes()

	port := getEnv("PORT", "8080")
	if err := server.Start(port); err != nil {
		sugar.Fatalf("server error: %v", err)
	}
}

// createDatabasePoolFromConfig creates a PostgreSQL connection pool from config
func createDatabasePoolFromConfig(config forma.DatabaseConfig) (*pgxpool.Pool, error) {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		config.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(config.MaxConnections)
	poolConfig.MinConns = int32(config.MaxIdleConns)
	poolConfig.MaxConnLifetime = config.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.ConnMaxIdleTime
	poolConfig.ConnConfig.ConnectTimeout = config.Timeout

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
