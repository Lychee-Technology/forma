package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
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

	// DB_PORT is --db-port's default, so a set but unparsable value is
	// refused even when the flag would override it (#600). The refusal waits
	// until after flags.Parse so --help still prints usage; EnvInt returns
	// 5432 alongside the error to register the flag with.
	portDefault, portErr := bootstrap.EnvInt("DB_PORT", 5432)

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
		portDefault:     portDefault,
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
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
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

// tableDDL pairs one DDL statement with the line init-db prints on success and
// the label its failure is wrapped with. Splitting the statements out of
// ensureTables (#319) keeps them testable without a live database.
type tableDDL struct {
	statement string
	announce  string
	failure   string
}

// schemaRegistryDDL constructs the schema registry table.
func schemaRegistryDDL(schemaTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_name TEXT PRIMARY KEY,
		schema_id SMALLINT UNIQUE NOT NULL
	)`, schemaTable)
}

// eavTableDDL constructs the EAV data table.
func eavTableDDL(eavTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_id      SMALLINT NOT NULL,
		row_id         UUID NOT NULL,
		attr_id        SMALLINT NOT NULL,
		array_indices  TEXT NOT NULL DEFAULT '',
		value_text     TEXT,
		value_numeric  NUMERIC,
		PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
	)`, eavTable)
}

// changeLogDDL constructs the change log table.
func changeLogDDL(changeLog string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		    schema_id  SMALLINT NOT NULL,
			row_id     UUID     NOT NULL,
			flushed_at BIGINT   NOT NULL DEFAULT 0,
			changed_at BIGINT   NOT NULL,
			deleted_at BIGINT,
			primary key (schema_id, row_id, flushed_at)
		);`, changeLog)
}

// entityMainDDL is the hot-field main table: fixed physical columns that the
// schema metadata maps logical attributes onto.
func entityMainDDL(entityMain string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
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
		double_01          DOUBLE PRECISION,
		double_02          DOUBLE PRECISION,
		double_03          DOUBLE PRECISION,
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
}

func coreTableDDL(opts initDBOptions) []tableDDL {
	schemaTable := quoteIdentifier(opts.schemaTable)
	eavTable := quoteIdentifier(opts.eavTable)
	entityMain := quoteIdentifier(opts.entityMain)
	changeLog := quoteIdentifier(opts.changeLog)

	return []tableDDL{
		{
			statement: schemaRegistryDDL(schemaTable),
			announce:  fmt.Sprintf("Created schema registry table: %s\n", opts.schemaTable),
			failure:   "ensure schema registry table",
		},
		{
			statement: entityMainDDL(entityMain),
			// No trailing newline: pre-existing tool output, preserved verbatim.
			announce: fmt.Sprintf("Created entity main table: %s", opts.entityMain),
			failure:  "ensure entity main table",
		},
		{
			statement: eavTableDDL(eavTable),
			announce:  fmt.Sprintf("Created EAV table: %s\n", opts.eavTable),
			failure:   "ensure eav table",
		},
		{
			statement: changeLogDDL(changeLog),
			announce:  fmt.Sprintf("Created change log table: %s\n", opts.changeLog),
			failure:   "ensure change log table",
		},
	}
}

func eavIndexDDL(opts initDBOptions) []tableDDL {
	eavTable := quoteIdentifier(opts.eavTable)
	idxNumeric := quoteIdentifier(makeIndexName(opts.eavTable, "numeric"))
	idxText := quoteIdentifier(makeIndexName(opts.eavTable, "text"))
	return []tableDDL{
		{
			statement: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_numeric, row_id) WHERE value_numeric IS NOT NULL`, idxNumeric, eavTable),
			failure:   "create numeric index",
		},
		{
			statement: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (schema_id, attr_id, value_text, row_id) WHERE value_text IS NOT NULL`, idxText, eavTable),
			failure:   "create text index",
		},
	}
}

func mainColumnIndexDDL(opts initDBOptions) []tableDDL {
	entityMain := quoteIdentifier(opts.entityMain)
	indexedMainColumns := []string{
		"text_01", "text_02", "text_03",
		"smallint_01",
		"integer_01",
		"bigint_01", "bigint_02",
		"double_01", "double_02",
		"uuid_01",
	}

	ddl := make([]tableDDL, 0, len(indexedMainColumns))
	for _, col := range indexedMainColumns {
		idx := quoteIdentifier(makeIndexName(opts.entityMain, col))
		ddl = append(ddl, tableDDL{
			statement: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (ltbase_schema_id, ltbase_row_id, %s)`, idx, entityMain, quoteIdentifier(col)),
			failure:   "create main index for " + col,
		})
	}
	return ddl
}

func ensureTables(ctx context.Context, tx pgx.Tx, opts initDBOptions) error {
	groups := [][]tableDDL{
		coreTableDDL(opts),
		eavIndexDDL(opts),
		mainColumnIndexDDL(opts),
	}
	for _, group := range groups {
		for _, ddl := range group {
			if _, err := tx.Exec(ctx, ddl.statement); err != nil {
				return fmt.Errorf("%s: %w", ddl.failure, err)
			}
			if ddl.announce != "" {
				fmt.Print(ddl.announce)
			}
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

// firstSchemaID is the schema_id init-db gives the first schema it registers.
const firstSchemaID = 100

// registerSchemas inserts the JSON schema files in schemaDir into the schema
// registry table. A name already registered keeps its schema_id, which keys
// its entity rows and the parquet files written under it; a new name gets an
// id from newSchemaIDs, so re-running init-db after adding a schema file
// succeeds (#643).
func registerSchemas(ctx context.Context, tx pgx.Tx, schemaTable, schemaDir string) error {
	schemaNames, err := listSchemaNames(schemaDir)
	if err != nil {
		return err
	}
	if len(schemaNames) == 0 {
		fmt.Printf("No schema files found, dir: %s\n", schemaDir)
		return nil
	}

	registered, err := loadRegisteredSchemaIDs(ctx, tx, schemaTable)
	if err != nil {
		return err
	}
	newIDs, err := newSchemaIDs(schemaNames, registered)
	if err != nil {
		return err
	}

	// ON CONFLICT absorbs a concurrent init-db registering the same name
	// between the read above and this insert.
	insertSQL := fmt.Sprintf(
		`INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2) ON CONFLICT (schema_name) DO NOTHING`,
		quoteIdentifier(schemaTable),
	)
	for _, schemaName := range schemaNames {
		schemaID, isNew := newIDs[schemaName]
		inserted := false
		if isNew {
			result, err := tx.Exec(ctx, insertSQL, schemaName, schemaID)
			if err != nil {
				return fmt.Errorf("insert schema %s: %w", schemaName, err)
			}
			inserted = result.RowsAffected() > 0
		}

		if inserted {
			fmt.Printf("Registered schema, name: %s, id: %d\n", schemaName, schemaID)
		} else {
			fmt.Printf("Schema already exists, schema name: %s\n", schemaName)
		}
	}

	fmt.Printf("Registered schemas from directory, count: %d, dir: %s\n", len(schemaNames), schemaDir)
	return nil
}

// listSchemaNames returns the names of the JSON schema files in schemaDir,
// leaving out the *_attributes.json metadata files. The order is that of the
// file names, which on an empty registry fixes the schema ids.
func listSchemaNames(schemaDir string) ([]string, error) {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("read schema directory(%s): %w", schemaDir, err)
	}

	var schemaFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, "_attributes.json") {
			continue
		}
		schemaFiles = append(schemaFiles, name)
	}
	sort.Strings(schemaFiles)

	names := make([]string, len(schemaFiles))
	for i, file := range schemaFiles {
		names[i] = strings.TrimSuffix(file, ".json")
	}
	return names, nil
}

// loadRegisteredSchemaIDs reads the registry table's schema_name -> schema_id
// rows.
func loadRegisteredSchemaIDs(ctx context.Context, tx pgx.Tx, schemaTable string) (map[string]int16, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT schema_name, schema_id FROM %s`, quoteIdentifier(schemaTable)))
	if err != nil {
		return nil, fmt.Errorf("read registered schemas from %s: %w", schemaTable, err)
	}

	registered := make(map[string]int16)
	var name string
	var id int16
	if _, err := pgx.ForEachRow(rows, []any{&name, &id}, func() error {
		registered[name] = id
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan registered schemas from %s: %w", schemaTable, err)
	}
	return registered, nil
}

// newSchemaIDs assigns a schema_id to each of names (in order) that registered
// lacks, counting up from one past the highest registered id, and from
// firstSchemaID on an empty registry. That is the positional 100, 101, ... of
// a first run, and the ids a re-run gives names sorting after every registered
// one. A gap is never refilled: it may be a deleted registration whose id
// still keys entity rows or parquet files.
func newSchemaIDs(names []string, registered map[string]int16) (map[string]int16, error) {
	next := firstSchemaID
	for _, id := range registered {
		next = max(next, int(id)+1)
	}

	ids := make(map[string]int16)
	for _, name := range names {
		if _, ok := registered[name]; ok {
			continue
		}
		if next > math.MaxInt16 {
			return nil, fmt.Errorf("assign schema_id to schema %s: next id %d exceeds the SMALLINT maximum %d", name, next, math.MaxInt16)
		}
		ids[name] = int16(next)
		next++
	}
	return ids, nil
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
