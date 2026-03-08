package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
)

type initDBOptions struct {
	host        string
	port        int
	database    string
	user        string
	password    string
	sslMode     string
	schemaTable string
	eavTable    string
	entityMain  string
	changeLog   string
	schemaDir   string
}

func runInitDB(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("init-db", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools init-db [options]")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	opts := initDBOptions{}
	var pg postgresFlags
	pg.register(flags, postgresFlagOptions{
		hostFlag:        "db-host",
		portFlag:        "db-port",
		userFlag:        "db-user",
		passwordFlag:    "db-password",
		databaseFlag:    "db-name",
		sslModeFlag:     "db-ssl-mode",
		hostDefault:     bootstrap.Env("DB_HOST", "localhost"),
		portDefault:     bootstrap.EnvInt("DB_PORT", 5432),
		userDefault:     bootstrap.Env("DB_USER", "postgres"),
		passwordDefault: bootstrap.Env("DB_PASSWORD", "postgres"),
		databaseDefault: bootstrap.Env("DB_NAME", "forma"),
		sslModeDefault:  bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage:       "database host",
		portUsage:       "database port",
		userUsage:       "database user",
		passwordUsage:   "database password",
		databaseUsage:   "database name",
		sslModeUsage:    "database sslmode",
	})
	flags.StringVar(&opts.schemaTable, "schema-table", bootstrap.Env("SCHEMA_TABLE", "schema_registry"), "schema registry table name")
	flags.StringVar(&opts.eavTable, "eav-table", bootstrap.Env("EAV_TABLE", "eav_dev"), "EAV data table name")
	flags.StringVar(&opts.entityMain, "entity-main-table", bootstrap.Env("ENTITY_MAIN_TABLE", "entity_main_dev"), "Entity main table name")
	flags.StringVar(&opts.changeLog, "change-log-table", bootstrap.Env("CHANGE_LOG_TABLE", "change_log_dev"), "Change log table name")
	flags.StringVar(&opts.schemaDir, "schema-dir", bootstrap.Env("SCHEMA_DIR", ""), "Directory containing JSON schema files to register (optional)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	opts.host = pg.host
	opts.port = pg.port
	opts.database = pg.database
	opts.user = pg.user
	opts.password = pg.resolvedPassword("DB_PASSWORD")
	opts.sslMode = pg.sslMode

	return initDatabase(ctx, opts, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 4,
		timeout:        30 * time.Second,
	}))
}

func initDatabase(ctx context.Context, opts initDBOptions, dbConfig forma.DatabaseConfig) error {
	pool, err := buildToolPostgresPool(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if err := withTx(ctx, conn, func(tx pgx.Tx) error {
		return ensureTables(ctx, tx, opts)
	}); err != nil {
		return err
	}

	fmt.Println("Database initialized successfully.")
	return nil
}

func ensureTables(ctx context.Context, tx pgx.Tx, opts initDBOptions) error {
	schemaTable := quoteIdentifier(opts.schemaTable)
	eavTable := quoteIdentifier(opts.eavTable)
	entityMain := quoteIdentifier(opts.entityMain)
	changeLog := quoteIdentifier(opts.changeLog)

	ddlSchema := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_name TEXT PRIMARY KEY,
		schema_id SMALLINT UNIQUE NOT NULL
	)`, schemaTable)

	if _, err := tx.Exec(ctx, ddlSchema); err != nil {
		return fmt.Errorf("ensure schema registry table: %w", err)
	}
	fmt.Printf("Created schema registry table: %s\n", opts.schemaTable)

	ddlMain := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id   SMALLINT NOT NULL,
		ltbase_row_id      UUID NOT NULL,
		text_01            TEXT,
		text_02            TEXT,
		text_03            TEXT,
		text_04            TEXT,
		text_05            TEXT,
		text_06            TEXT,
		text_07            TEXT,
		text_08            TEXT,
		text_09            TEXT,
		text_10            TEXT,
		smallint_01        SMALLINT,
		smallint_02        SMALLINT,
		smallint_03        SMALLINT,
		integer_01         INTEGER,
		integer_02         INTEGER,
		integer_03         INTEGER,
		bigint_01          BIGINT,
		bigint_02          BIGINT,
		bigint_03          BIGINT,
		bigint_04          BIGINT,
		bigint_05          BIGINT,
		double_01          DOUBLE PRECISION,
		double_02          DOUBLE PRECISION,
		double_03          DOUBLE PRECISION,
		double_04          DOUBLE PRECISION,
		double_05          DOUBLE PRECISION,
		uuid_01            UUID,
		uuid_02            UUID,
		ltbase_created_at  BIGINT NOT NULL,
		ltbase_updated_at  BIGINT NOT NULL,
		ltbase_deleted_at  BIGINT,
		ltbase_created_by  TEXT,
		ltbase_updated_by  TEXT,
		ltbase_deleted_by  TEXT,
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`, entityMain)

	if _, err := tx.Exec(ctx, ddlMain); err != nil {
		return fmt.Errorf("ensure entity main table: %w", err)
	}
	fmt.Printf("Created entity main table: %s", opts.entityMain)

	ddlEAV := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_id      SMALLINT NOT NULL,
		row_id         UUID NOT NULL,
		attr_id        SMALLINT NOT NULL,
		array_indices  TEXT NOT NULL DEFAULT '',
		value_text     TEXT,
		value_numeric  NUMERIC,
		PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
	)`, eavTable)

	if _, err := tx.Exec(ctx, ddlEAV); err != nil {
		return fmt.Errorf("ensure eav table: %w", err)
	}
	fmt.Printf("Created EAV table: %s\n", opts.eavTable)

	ddlChangeLog := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		    schema_id  SMALLINT NOT NULL,
			row_id     UUID     NOT NULL,
			flushed_at BIGINT   NOT NULL DEFAULT 0,
			changed_at BIGINT   NOT NULL,
			deleted_at BIGINT,
			primary key (schema_id, row_id, flushed_at)
		);`, changeLog)

	if _, err := tx.Exec(ctx, ddlChangeLog); err != nil {
		return fmt.Errorf("ensure change log table: %w", err)
	}
	fmt.Printf("Created change log table: %s\n", opts.changeLog)

	idxNumeric := quoteIdentifier(makeIndexName(opts.eavTable, "numeric"))
	createIdxNumeric := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_numeric, row_id) WHERE value_numeric IS NOT NULL`, idxNumeric, eavTable)
	if _, err := tx.Exec(ctx, createIdxNumeric); err != nil {
		return fmt.Errorf("create numeric index: %w", err)
	}

	idxText := quoteIdentifier(makeIndexName(opts.eavTable, "text"))
	createIdxText := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_text, row_id) WHERE value_text IS NOT NULL`, idxText, eavTable)
	if _, err := tx.Exec(ctx, createIdxText); err != nil {
		return fmt.Errorf("create text index: %w", err)
	}

	indexedMainColumns := []string{
		"text_01", "text_02", "text_03",
		"smallint_01",
		"integer_01",
		"bigint_01", "bigint_02",
		"double_01", "double_02",
		"uuid_01",
	}

	for _, col := range indexedMainColumns {
		idx := quoteIdentifier(makeIndexName(opts.entityMain, col))
		stmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (ltbase_schema_id, ltbase_row_id, %s)`, idx, entityMain, quoteIdentifier(col))
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create main index for %s: %w", col, err)
		}
	}

	// Register schemas from schema directory if provided
	if opts.schemaDir != "" {
		if err := registerSchemas(ctx, tx, opts.schemaTable, opts.schemaDir); err != nil {
			return err
		}
	}

	return nil
}

// registerSchemas reads JSON schema files from the directory and inserts them into the schema registry table
func registerSchemas(ctx context.Context, tx pgx.Tx, schemaTable, schemaDir string) error {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return fmt.Errorf("read schema directory(%s): %w", schemaDir, err)
	}

	// Collect schema files (excluding *_attributes.json files)
	var schemaFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, "_attributes.json") {
			schemaFiles = append(schemaFiles, name)
		}
	}

	if len(schemaFiles) == 0 {
		fmt.Printf("No schema files found, dir: %s\n", schemaDir)
		return nil
	}

	// Sort for deterministic schema ID assignment
	sort.Strings(schemaFiles)

	// Insert schemas into the registry table
	quotedTable := quoteIdentifier(schemaTable)
	for idx, file := range schemaFiles {
		schemaName := strings.TrimSuffix(file, ".json")
		schemaID := int16(idx + 100) // Start IDs from 100

		insertSQL := fmt.Sprintf(
			`INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2) ON CONFLICT (schema_name) DO NOTHING`,
			quotedTable,
		)

		result, err := tx.Exec(ctx, insertSQL, schemaName, schemaID)
		if err != nil {
			return fmt.Errorf("insert schema %s: %w", schemaName, err)
		}

		if result.RowsAffected() > 0 {
			fmt.Printf("Registered schema, name: %s, id: %d\n", schemaName, schemaID)
		} else {
			fmt.Printf("Schema already exists, schema name: %s\n", schemaName)
		}
	}

	fmt.Printf("Registered schemas from directory, count: %d, dir: %s\n", len(schemaFiles), schemaDir)
	return nil
}

func withTx(ctx context.Context, conn *pgxpool.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func quoteIdentifier(name string) string {
	return pgx.Identifier(splitIdentifier(name)).Sanitize()
}

func splitIdentifier(name string) []string {
	parts := strings.Split(name, ".")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		return []string{name}
	}
	return result
}

func makeIndexName(table string, suffix string) string {
	base := strings.ReplaceAll(table, ".", "_")
	base = strings.ReplaceAll(base, `"`, "")
	return fmt.Sprintf("%s_%s_idx", base, suffix)
}
