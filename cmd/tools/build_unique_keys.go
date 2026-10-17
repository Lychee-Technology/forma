package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/model"
)

// buildUniqueKeysOptions is what build-unique-keys builds, and where.
type buildUniqueKeysOptions struct {
	schema      string
	key         string
	schemaTable string
	eavTable    string
	entityMain  string
	schemaDir   string
	rebuild     bool
}

// runBuildUniqueKeys builds a schema's x-forma-unique-keys from its live rows
// (#651). It is offline: every writer must be stopped while it runs. The table
// flags and their environment defaults are init-db's, so one environment
// serves both.
func runBuildUniqueKeys(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("build-unique-keys", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools build-unique-keys --schema <name> [--key <name>] [--rebuild] [options]")
		fmt.Println("")
		fmt.Println("Builds the unique keys a schema declares from its live rows. Stop every writer first;")
		fmt.Println("the server refuses to start until every declared key is built.")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	portDefault, portErr := bootstrap.EnvInt("DB_PORT", 5432)
	var pg postgresFlags
	pg.register(flags, postgresFlagOptions{
		hostFlag: "db-host", portFlag: "db-port", userFlag: "db-user",
		passwordFlag: "db-password", databaseFlag: "db-name", sslModeFlag: "db-ssl-mode",
		hostDefault:     bootstrap.Env("DB_HOST", "localhost"),
		portDefault:     portDefault,
		userDefault:     bootstrap.Env("DB_USER", "postgres"),
		passwordDefault: bootstrap.Env("DB_PASSWORD", "postgres"),
		databaseDefault: bootstrap.Env("DB_NAME", "forma"),
		sslModeDefault:  bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage:       "database host", portUsage: "database port", userUsage: "database user",
		passwordUsage: "database password", databaseUsage: "database name", sslModeUsage: "database sslmode",
	})
	var opts buildUniqueKeysOptions
	flags.StringVar(&opts.schema, "schema", "", "Schema whose unique keys to build (required)")
	flags.StringVar(&opts.key, "key", "", "Unique key to build (default: every key the schema declares, dropping keys it no longer declares)")
	flags.BoolVar(&opts.rebuild, "rebuild", false, "Build keys again although they are built, for entries a writer that did not maintain them left short")
	flags.StringVar(&opts.schemaTable, "schema-table", bootstrap.Env("SCHEMA_TABLE", "schema_registry"), "schema registry table name")
	flags.StringVar(&opts.eavTable, "eav-table", bootstrap.Env("EAV_TABLE", "eav_dev"), "EAV data table name")
	flags.StringVar(&opts.entityMain, "entity-main-table", bootstrap.Env("ENTITY_MAIN_TABLE", "entity_main_dev"), "Entity main table name")
	flags.StringVar(&opts.schemaDir, "schema-dir", bootstrap.Env("SCHEMA_DIR", ""), "Directory containing the JSON schema files (required)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if opts.schema == "" || opts.schemaDir == "" {
		return fmt.Errorf("--schema and --schema-dir are required")
	}
	return buildUniqueKeys(ctx, opts, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}), os.Stdout)
}

func buildUniqueKeys(ctx context.Context, opts buildUniqueKeysOptions, dbConfig forma.DatabaseConfig, out io.Writer) error {
	pool, err := buildToolPostgresPool(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	registry, err := buildToolSchemaRegistry(ctx, pool, opts.schemaTable, opts.schemaDir)
	if err != nil {
		return fmt.Errorf("load schemas from %s: %w", opts.schemaDir, err)
	}
	catalog, err := internal.NewUniqueKeyCatalog(registry)
	if err != nil {
		return fmt.Errorf("resolve the unique keys of %s: %w", opts.schemaDir, err)
	}
	tables := model.StorageTables{EntityMain: opts.entityMain, EAVData: opts.eavTable}
	report, err := catalog.BuildUniqueKeys(ctx, pool, tables, internal.UniqueKeyBuildRequest{Schema: opts.schema, Key: opts.key, Rebuild: opts.rebuild})
	printUniqueKeyBuildReport(out, opts.schema, report)
	if err != nil {
		return fmt.Errorf("build the unique keys of schema %s: %w", opts.schema, err)
	}
	return nil
}

func printUniqueKeyBuildReport(out io.Writer, schema string, report internal.UniqueKeyBuildReport) {
	for _, k := range report.Keys {
		if k.AlreadyBuilt {
			fmt.Fprintf(out, "Unique key %s of schema %s is already built\n", k.Key, k.Schema)
			continue
		}
		verb := "Built"
		if k.Rebuilt {
			verb = "Rebuilt"
		}
		fmt.Fprintf(out, "%s unique key %s of schema %s: %d entries\n", verb, k.Key, k.Schema, k.Entries)
	}
	for _, id := range report.DroppedKeyIDs {
		fmt.Fprintf(out, "Dropped unique key id %d, which schema %s no longer declares\n", id, schema)
	}
}
