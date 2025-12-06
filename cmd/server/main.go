package main

import (
	"net/http"
	"os"
	"strconv"

	"github.com/lychee-technology/forma"
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

	// Set schema directory
	schemaDir := os.Getenv("SCHEMA_DIR")
	sugar.Infof("schemaDir: %s", schemaDir)
	// Create file-based schema registry
	registry, err := NewFileSchemaRegistry(schemaDir)
	if err != nil {
		sugar.Fatalf("failed to create schema registry: %v", err)
	}

	// Load configuration with schema registry
	config := forma.DefaultConfig(registry)

	// Set schema directory
	config.Entity.SchemaDirectory = schemaDir

	// Set database configuration from environment variables
	config.Database.Host = getEnv("DB_HOST", "localhost")
	config.Database.Port = getEnvInt("DB_PORT", 5432)
	config.Database.Database = getEnv("DB_NAME", "forma")
	config.Database.Username = getEnv("DB_USER", "postgres")
	config.Database.Password = getEnv("DB_PASSWORD", "")
	config.Database.SSLMode = getEnv("DB_SSL_MODE", "disable")
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry_dev",
		EAVData:        "eav_data_dev",
		EntityMain:     "entity_main_dev",
	}

	// Initialize EntityManager and get metadata cache
	manager := NewEntityManager(config)

	server := NewServer(manager)
	server.RegisterRoutes()

	port := getEnv("PORT", "8080")
	if err := server.Start(port); err != nil {
		sugar.Fatalf("server error: %v", err)
	}
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
