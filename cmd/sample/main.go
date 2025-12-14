package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

"github.com/jackc/pgx/v5/pgxpool"
"github.com/lychee-technology/forma"
"github.com/lychee-technology/forma/factory"
"github.com/lychee-technology/forma/internal"
"go.uber.org/zap"
)

func main() {
	// Command line flags
	csvFile := flag.String("csv", "", "Path to CSV file to import (required)")
	schemaDir := flag.String("schema-dir", "./schemas", "Directory containing schema files")
	schemaName := flag.String("schema", "watch", "Target schema name")
	batchSize := flag.Int("batch-size", 100, "Batch size for import operations")
	dbURL := flag.String("db", "postgres://postgres:postgres@localhost:5432/ltbase", "PostgreSQL connection URL (or set DATABASE_URL env)")
	dryRun := flag.Bool("dry-run", false, "Parse CSV and validate mappings without writing to database")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	// Setup logging
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	if *verbose {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		cfg.Development = true
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Errorf("failed to build logger: %w", err))
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)
	sugar := logger.Sugar()

	flag.Parse()

	// Validate required flags
	if *csvFile == "" {
		sugar.Error("Error: -csv flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Get database URL from flag or environment
	databaseURL := *dbURL
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}

	if databaseURL == "" && !*dryRun {
		sugar.Error("Error: Database URL is required. Use -db flag or set DATABASE_URL environment variable.")
		os.Exit(1)
	}

	ctx := context.Background()

	// Create mapper based on schema name
	var mapper CSVToSchemaMapper
	switch *schemaName {
	case "watch":
		mapper = NewWatchMapper()
		sugar.Infof("Using Watch mapper with schema: %s", mapper.SchemaName())
	default:
		sugar.Fatalf("Unknown schema: %s. Supported schemas: watch", *schemaName)
	}

	// Dry run mode - just validate the CSV
	if *dryRun {
		sugar.Infof("Dry run mode: validating CSV file %s", *csvFile)
		result, err := dryRunImport(ctx, *csvFile, mapper, sugar)
		if err != nil {
			sugar.Fatalf("Dry run failed: %v", err)
		}
		printResult(result, sugar)
		os.Exit(0)
	}

	// Connect to database
	sugar.Infof("Connecting to database...")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		sugar.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		sugar.Fatalf("Failed to ping database: %v", err)
	}
	sugar.Infof("Database connected successfully")

// Create schema registry
sugar.Infof("Loading schemas from: %s", *schemaDir)
registry, err := internal.NewFileSchemaRegistryFromDirectory(*schemaDir)
if err != nil {
sugar.Fatalf("Failed to create schema registry: %v", err)
}

	// Verify schema exists
	schemaID, _, err := registry.GetSchemaAttributeCacheByName(*schemaName)
	if err != nil {
		sugar.Fatalf("Schema '%s' not found: %v", *schemaName, err)
	}
	sugar.Infof("Schema '%s' found with ID: %d", *schemaName, schemaID)

	// Create configuration
	config := forma.DefaultConfig(registry)
	config.Database.TableNames.EAVData = "eav_data_sample"
	config.Database.TableNames.EntityMain = "entity_main_sample"
	config.Database.TableNames.SchemaRegistry = "schema_registry_sample"

	config.Entity.SchemaDirectory = *schemaDir

	// Clear sample tables before import
	sugar.Infof("Clearing sample tables...")
	_, err = pool.Exec(ctx, "TRUNCATE TABLE eav_data_sample, entity_main_sample CASCADE")
	if err != nil {
		sugar.Fatalf("Failed to clear sample tables: %v", err)
	}
	sugar.Infof("Sample tables cleared successfully")

	// Create entity manager
	sugar.Infof("Initializing entity manager...")
	entityManager, err := factory.NewEntityManagerWithConfig(config, pool)
	if err != nil {
		sugar.Fatalf("Failed to create entity manager: %v", err)
	}

	// Create importer
	importer := NewCSVImporter(entityManager, mapper, *batchSize)
	importer.SetLogger(sugar.Named("Import"))

	// Execute import
	sugar.Infof("Starting import from: %s", *csvFile)
	sugar.Infof("Target schema: %s, Batch size: %d", *schemaName, *batchSize)

	startTime := time.Now()
	result, err := importer.ImportFromFile(ctx, *csvFile)
	if err != nil {
		sugar.Fatalf("Import failed: %v", err)
	}

	// Print results
	sugar.Infof("Import completed in %v", time.Since(startTime))
	printResult(result, sugar)

	// Execute advanced query example
	sugar.Infof("Executing advanced query example...")
	sugar.Infof("Query conditions: yearOfProduction.year >= 2020 AND size.width >= 40")
	queryResult, err := entityManager.Query(ctx, &forma.QueryRequest{
		SchemaName:   *schemaName,
		Page:         1,
		ItemsPerPage: 10,
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "yearOfProduction.year",
					Value: "gte:2020",
				},
				&forma.KvCondition{
					Attr:  "size.width",
					Value: "gte:40",
				},
			},
		},
	})
	if err != nil {
		sugar.Errorf("Advanced query failed: %v", err)
	} else {
		sugar.Infof("Advanced query completed successfully")
		sugar.Infof("  Total records found: %d", queryResult.TotalRecords)
		sugar.Infof("  Records returned: %d", len(queryResult.Data))

		// Output query results as JSON conforming to the schema
		sugar.Info("Query results as JSON (conforming to schema):")
		for i, record := range queryResult.Data {
			jsonBytes, err := json.MarshalIndent(record.Attributes, "", "  ")
			if err != nil {
				sugar.Infof("  [%d] Error marshaling to JSON: %v", i+1, err)
				continue
			}
			sugar.Infof("  Record %d:", i+1)
			sugar.Info(string(jsonBytes))
		}
	}

	// Exit with error code if there were failures
	if result.FailedCount > 0 {
		os.Exit(1)
	}
}

// dryRunImport performs a dry run of the import process without database operations.
func dryRunImport(ctx context.Context, csvFile string, mapper CSVToSchemaMapper, logger *zap.SugaredLogger) (*ImportResult, error) {
	file, err := os.Open(csvFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Use a mock entity manager for dry run
	mockEM := &mockEntityManager{}
	importer := NewCSVImporter(mockEM, mapper, 1000)
	importer.SetLogger(logger)

	return importer.ImportFromReader(ctx, file)
}

// printResult prints the import result summary.
func printResult(result *ImportResult, logger *zap.SugaredLogger) {
	logger.Info("=" + string(make([]byte, 50)) + "=")
	logger.Info("Import Summary")
	logger.Info("=" + string(make([]byte, 50)) + "=")
	logger.Infof("  Total rows:     %d", result.TotalRows)
	logger.Infof("  Successful:     %d", result.SuccessCount)
	logger.Infof("  Failed:         %d", result.FailedCount)
	logger.Infof("  Duration:       %v", result.Duration)

	if result.FailedCount > 0 {
		successRate := float64(result.SuccessCount) / float64(result.TotalRows) * 100
		logger.Infof("  Success rate:   %.2f%%", successRate)
	}

	if len(result.Errors) > 0 {
		logger.Info("")
		logger.Infof("First %d errors:", min(10, len(result.Errors)))
		for i, err := range result.Errors {
			if i >= 10 {
				logger.Infof("  ... and %d more errors", len(result.Errors)-10)
				break
			}
			logger.Infof("  [%d] %s", i+1, err.Error())
		}
	}
}

// mockEntityManager is a mock implementation for dry run mode.
type mockEntityManager struct{}

func (m *mockEntityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		Attributes: req.Data,
	}, nil
}

func (m *mockEntityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	return nil, nil
}

func (m *mockEntityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return nil, nil
}

func (m *mockEntityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	return nil
}

func (m *mockEntityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	return &forma.QueryResult{}, nil
}

func (m *mockEntityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	return &forma.QueryResult{}, nil
}

func (m *mockEntityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	successful := make([]*forma.DataRecord, len(req.Operations))
	for i, op := range req.Operations {
		successful[i] = &forma.DataRecord{
			SchemaName: op.SchemaName,
			Attributes: op.Data,
		}
	}
	return &forma.BatchResult{
		Successful: successful,
		Failed:     []forma.OperationError{},
		TotalCount: len(req.Operations),
	}, nil
}

func (m *mockEntityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return &forma.BatchResult{}, nil
}

func (m *mockEntityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return &forma.BatchResult{}, nil
}
