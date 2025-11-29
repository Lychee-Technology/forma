package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type attributeSpec struct {
	AttributeID int    `json:"attributeID"`
	ValueType   string `json:"valueType"`
	InsideArray bool   `json:"insideArray"`
}

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate-attributes":
		if err := runGenerateAttributes(os.Args[2:]); err != nil {
			log.Fatalf("generate-attributes: %v", err)
		}
	case "init-db":
		if err := runInitDB(os.Args[2:]); err != nil {
			log.Fatalf("init-db: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: forma-tools <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  generate-attributes   Generate <schema>_attributes.json from a JSON schema file")
	fmt.Println("  init-db               Create PostgreSQL tables and indexes for Forma")
}

func runGenerateAttributes(args []string) error {
	flags := flag.NewFlagSet("generate-attributes", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools generate-attributes [options]")
		fmt.Println()
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	schemaDir := flags.String("schema-dir", "cmd/server/schemas", "Directory containing JSON schema files")
	schemaName := flags.String("schema", "", "Schema name without extension (mutually exclusive with -schema-file)")
	schemaFile := flags.String("schema-file", "", "Path to the JSON schema file (overrides -schema and -schema-dir)")
	outputFile := flags.String("out", "", "Path to write the generated attributes JSON (defaults next to schema file)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	resolvedSchemaPath := *schemaFile
	if resolvedSchemaPath == "" {
		if *schemaName == "" {
			return fmt.Errorf("either -schema or -schema-file must be provided")
		}
		resolvedSchemaPath = filepath.Join(*schemaDir, *schemaName+".json")
	}

	resolvedOutputPath := *outputFile
	if resolvedOutputPath == "" {
		base := strings.TrimSuffix(filepath.Base(resolvedSchemaPath), filepath.Ext(resolvedSchemaPath))
		resolvedOutputPath = filepath.Join(filepath.Dir(resolvedSchemaPath), base+"_attributes.json")
	}

	if err := generateAttributesJSON(resolvedSchemaPath, resolvedOutputPath); err != nil {
		return err
	}

	fmt.Printf("Generated attributes for %s -> %s\n", resolvedSchemaPath, resolvedOutputPath)
	return nil
}

func generateAttributesJSON(schemaPath, outputPath string) error {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema file: %w", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("parse schema JSON: %w", err)
	}

	attributes := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	attributeNames := make([]string, 0, len(attributes))
	for name := range attributes {
		attributeNames = append(attributeNames, name)
	}
	sort.Strings(attributeNames)

	sorted := make(map[string]attributeSpec, len(attributes))
	for idx, name := range attributeNames {
		spec := attributes[name]
		spec.AttributeID = idx + 1
		sorted[name] = spec
	}

	if err := writeAttributes(outputPath, sorted); err != nil {
		return err
	}

	fmt.Printf("Generated %d attributes.\n", len(sorted))
	return nil
}

func traverseSchema(schema map[string]any, path string, insideArray bool, attributes map[string]attributeSpec) map[string]attributeSpec {
	if properties, ok := schema["properties"].(map[string]any); ok {
		for key, raw := range properties {
			child, ok := raw.(map[string]any)
			if !ok {
				continue
			}

			var newPath string
			if path == "" {
				newPath = key
			} else {
				newPath = path + "." + key
			}
			traverseSchema(child, newPath, insideArray, attributes)
		}
		return attributes
	}

	switch schemaType := getSchemaType(schema); schemaType {
	case "array":
		if items, ok := schema["items"].(map[string]any); ok {
			switch getSchemaType(items) {
			case "object":
				if _, ok := items["properties"]; ok {
					return traverseSchema(items, path, true, attributes)
				}
			case "string", "integer", "number", "boolean":
				attributes[path] = attributeSpec{
					ValueType:   getValueType(items),
					InsideArray: true,
				}
				return attributes
			}
		}
	default:
		attributes[path] = attributeSpec{
			ValueType:   getValueType(schema),
			InsideArray: insideArray,
		}
	}

	return attributes
}

func getSchemaType(node map[string]any) string {
	switch t := node["type"].(type) {
	case string:
		return t
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func getValueType(schema map[string]any) string {
	schemaType := getSchemaType(schema)
	formatType, _ := schema["format"].(string)

	switch schemaType {
	case "string":
		if formatType == "date" || formatType == "date-time" {
			return "date"
		}
		return "text"
	case "integer", "number":
		return "numeric"
	case "boolean":
		return "bool"
	default:
		return "text"
	}
}

func writeAttributes(path string, attributes map[string]attributeSpec) error {
	encoded, err := json.MarshalIndent(attributes, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal attributes: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write attributes file: %w", err)
	}

	return nil
}

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
}

func runInitDB(args []string) error {
	flags := flag.NewFlagSet("init-db", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools init-db [options]")
		fmt.Println()
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	opts := initDBOptions{}
	flags.StringVar(&opts.host, "db-host", getenvDefault("DB_HOST", "localhost"), "database host")
	flags.IntVar(&opts.port, "db-port", getenvDefaultInt("DB_PORT", 5432), "database port")
	flags.StringVar(&opts.database, "db-name", getenvDefault("DB_NAME", "forma"), "database name")
	flags.StringVar(&opts.user, "db-user", getenvDefault("DB_USER", "postgres"), "database user")
	flags.StringVar(&opts.password, "db-password", getenvDefault("DB_PASSWORD", "postgres"), "database password")
	flags.StringVar(&opts.sslMode, "db-ssl-mode", getenvDefault("DB_SSL_MODE", "disable"), "database sslmode")
	flags.StringVar(&opts.schemaTable, "schema-table", getenvDefault("SCHEMA_TABLE", "schema_registry"), "schema registry table name")
	flags.StringVar(&opts.eavTable, "eav-table", getenvDefault("EAV_TABLE", "eav_dev"), "EAV data table name")
	flags.StringVar(&opts.entityMain, "entity-main-table", getenvDefault("ENTITY_MAIN_TABLE", "entity_main_dev"), "Entity main table name")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	return initDatabase(opts)
}

func initDatabase(opts initDBOptions) error {
	ctx := context.Background()

	connString := buildConnString(opts)
	pool, err := pgxpool.New(ctx, connString)
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

func buildConnString(opts initDBOptions) string {
	hostPort := fmt.Sprintf("%s:%d", opts.host, opts.port)

	var userInfo *url.Userinfo
	if opts.password != "" {
		userInfo = url.UserPassword(opts.user, opts.password)
	} else {
		userInfo = url.User(opts.user)
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   userInfo,
		Host:   hostPort,
		Path:   "/" + opts.database,
	}

	q := url.Values{}
	if opts.sslMode != "" {
		q.Set("sslmode", opts.sslMode)
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func ensureTables(ctx context.Context, tx pgx.Tx, opts initDBOptions) error {
	schemaTable := quoteIdentifier(opts.schemaTable)
	eavTable := quoteIdentifier(opts.eavTable)
	entityMain := quoteIdentifier(opts.entityMain)

	ddlSchema := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_name TEXT PRIMARY KEY,
		schema_id SMALLINT UNIQUE NOT NULL
	)`, schemaTable)

	if _, err := tx.Exec(ctx, ddlSchema); err != nil {
		return fmt.Errorf("ensure schema registry table: %w", err)
	}
	fmt.Printf("Created %s\n", opts.schemaTable)

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
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`, entityMain)

	if _, err := tx.Exec(ctx, ddlMain); err != nil {
		return fmt.Errorf("ensure entity main table: %w", err)
	}
	fmt.Printf("Created %s\n", opts.entityMain)

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
	fmt.Printf("Created %s\n", opts.eavTable)

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

func getenvDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func getenvDefaultInt(key string, def int) int {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return def
}
