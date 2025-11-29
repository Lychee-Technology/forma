package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
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

	flag.Parse()

	// Validate required flags
	if *csvFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -csv flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Get database URL from flag or environment
	databaseURL := *dbURL
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}

	if databaseURL == "" && !*dryRun {
		fmt.Fprintln(os.Stderr, "Error: Database URL is required. Use -db flag or set DATABASE_URL environment variable.")
		os.Exit(1)
	}

	// Setup logging
	logger := log.New(os.Stdout, "", log.LstdFlags)
	if *verbose {
		logger.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	ctx := context.Background()

	// Create mapper based on schema name
	var mapper CSVToSchemaMapper
	switch *schemaName {
	case "watch":
		mapper = NewWatchMapper()
		logger.Printf("Using Watch mapper with schema: %s", mapper.SchemaName())
	default:
		logger.Fatalf("Unknown schema: %s. Supported schemas: watch", *schemaName)
	}

	// Dry run mode - just validate the CSV
	if *dryRun {
		logger.Printf("Dry run mode: validating CSV file %s", *csvFile)
		result, err := dryRunImport(ctx, *csvFile, mapper, logger)
		if err != nil {
			logger.Fatalf("Dry run failed: %v", err)
		}
		printResult(result, logger)
		os.Exit(0)
	}

	// Connect to database
	logger.Printf("Connecting to database...")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		logger.Fatalf("Failed to ping database: %v", err)
	}
	logger.Printf("Database connected successfully")

	// Create schema registry
	logger.Printf("Loading schemas from: %s", *schemaDir)
	registry, err := NewFileSchemaRegistry(*schemaDir)
	if err != nil {
		logger.Fatalf("Failed to create schema registry: %v", err)
	}

	// Verify schema exists
	schemaID, _, err := registry.GetSchemaByName(*schemaName)
	if err != nil {
		logger.Fatalf("Schema '%s' not found: %v", *schemaName, err)
	}
	logger.Printf("Schema '%s' found with ID: %d", *schemaName, schemaID)

	// Create configuration
	config := forma.DefaultConfig(registry)
	config.Database.TableNames.EAVData = "eav_data_sample"
	config.Database.TableNames.EntityMain = "entity_main_sample"
	config.Database.TableNames.SchemaRegistry = "schema_registry_sample"

	config.Entity.SchemaDirectory = *schemaDir

	// Create entity manager
	logger.Printf("Initializing entity manager...")
	entityManager, err := factory.NewEntityManagerWithConfig(config, pool)
	if err != nil {
		logger.Fatalf("Failed to create entity manager: %v", err)
	}

	// Create importer
	importer := NewCSVImporter(entityManager, mapper, *batchSize)
	importer.SetLogger(log.New(os.Stderr, "[Import] ", log.LstdFlags))

	// Execute import
	logger.Printf("Starting import from: %s", *csvFile)
	logger.Printf("Target schema: %s, Batch size: %d", *schemaName, *batchSize)

	startTime := time.Now()
	result, err := importer.ImportFromFile(ctx, *csvFile)
	if err != nil {
		logger.Fatalf("Import failed: %v", err)
	}

	// Print results
	logger.Printf("Import completed in %v", time.Since(startTime))
	printResult(result, logger)

	// Execute advanced query example
	logger.Printf("Executing advanced query example...")
	logger.Printf("Query conditions: yearOfProduction.year >= 2020 AND size.width >= 40")
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
		logger.Printf("Advanced query failed: %v", err)
	} else {
		logger.Printf("Advanced query completed successfully")
		logger.Printf("  Total records found: %d", queryResult.TotalRecords)
		logger.Printf("  Records returned: %d", len(queryResult.Data))

		// Output query results as JSON conforming to the schema
		logger.Println("Query results as JSON (conforming to schema):")
		for i, record := range queryResult.Data {
			jsonBytes, err := json.MarshalIndent(record.Attributes, "", "  ")
			if err != nil {
				logger.Printf("  [%d] Error marshaling to JSON: %v", i+1, err)
				continue
			}
			logger.Printf("  Record %d:", i+1)
			fmt.Println(string(jsonBytes))
		}
	}

	// Exit with error code if there were failures
	if result.FailedCount > 0 {
		os.Exit(1)
	}
}

// dryRunImport performs a dry run of the import process without database operations.
func dryRunImport(ctx context.Context, csvFile string, mapper CSVToSchemaMapper, logger *log.Logger) (*ImportResult, error) {
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
func printResult(result *ImportResult, logger *log.Logger) {
	logger.Println("=" + string(make([]byte, 50)) + "=")
	logger.Printf("Import Summary")
	logger.Println("=" + string(make([]byte, 50)) + "=")
	logger.Printf("  Total rows:     %d", result.TotalRows)
	logger.Printf("  Successful:     %d", result.SuccessCount)
	logger.Printf("  Failed:         %d", result.FailedCount)
	logger.Printf("  Duration:       %v", result.Duration)

	if result.FailedCount > 0 {
		successRate := float64(result.SuccessCount) / float64(result.TotalRows) * 100
		logger.Printf("  Success rate:   %.2f%%", successRate)
	}

	if len(result.Errors) > 0 {
		logger.Println("")
		logger.Printf("First %d errors:", min(10, len(result.Errors)))
		for i, err := range result.Errors {
			if i >= 10 {
				logger.Printf("  ... and %d more errors", len(result.Errors)-10)
				break
			}
			logger.Printf("  [%d] %s", i+1, err.Error())
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
