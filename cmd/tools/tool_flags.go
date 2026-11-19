package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
)

type toolPostgresPoolSettings struct {
	maxConnections int
	maxIdleConns   int
	timeout        time.Duration
}

type postgresFlags struct {
	host     string
	port     int
	user     string
	password string
	database string
	sslMode  string
	useIAM   bool
	engine   string
}

type postgresFlagOptions struct {
	hostFlag        string
	portFlag        string
	userFlag        string
	passwordFlag    string
	databaseFlag    string
	sslModeFlag     string
	hostDefault     string
	portDefault     int
	userDefault     string
	passwordDefault string
	databaseDefault string
	sslModeDefault  string
	hostUsage       string
	portUsage       string
	userUsage       string
	passwordUsage   string
	databaseUsage   string
	sslModeUsage    string
	includeUseIAM   bool
	useIAMFlag      string
	useIAMDefault   bool
	useIAMUsage     string
	// engineFlag, when set, registers the database engine flag (#633) of a
	// tool that writes entity rows or takes the CDC schema lock, the two
	// things the engines do differently.
	engineFlag    string
	engineDefault string
}

func (f *postgresFlags) register(fs *flag.FlagSet, opts postgresFlagOptions) {
	fs.StringVar(&f.host, opts.hostFlag, opts.hostDefault, opts.hostUsage)
	fs.IntVar(&f.port, opts.portFlag, opts.portDefault, opts.portUsage)
	fs.StringVar(&f.user, opts.userFlag, opts.userDefault, opts.userUsage)
	fs.StringVar(&f.password, opts.passwordFlag, opts.passwordDefault, opts.passwordUsage)
	fs.StringVar(&f.database, opts.databaseFlag, opts.databaseDefault, opts.databaseUsage)
	fs.StringVar(&f.sslMode, opts.sslModeFlag, opts.sslModeDefault, opts.sslModeUsage)
	if opts.includeUseIAM {
		fs.BoolVar(&f.useIAM, opts.useIAMFlag, opts.useIAMDefault, opts.useIAMUsage)
	}
	if opts.engineFlag != "" {
		fs.StringVar(&f.engine, opts.engineFlag, opts.engineDefault,
			fmt.Sprintf("database engine: %q or %q", forma.EnginePostgreSQL, forma.EngineAuroraDSQL))
	}
}

func (f postgresFlags) resolvedPassword(fallbackEnv string) string {
	if f.password != "" {
		return f.password
	}
	return bootstrap.Env(fallbackEnv, "")
}

// cdcEngine resolves the CDC tools' --pg-engine (#633), which decides their
// per-schema lock. --pg-use-iam authenticates with an Aurora DSQL token, so
// it implies aurora-dsql, and another engine beside it is refused: either
// reading would take a lock the other tools of the database do not see.
func (f postgresFlags) cdcEngine(engineFlag string) (forma.DatabaseEngine, error) {
	engine := forma.DatabaseEngine(f.engine)
	if f.useIAM {
		if engine != "" && engine != forma.EngineAuroraDSQL {
			return "", fmt.Errorf("--pg-use-iam authenticates to Aurora DSQL, but --%s is %q", engineFlag, engine)
		}
		return forma.EngineAuroraDSQL, nil
	}
	if err := engine.Validate(); err != nil {
		return "", fmt.Errorf("validate --%s: %w", engineFlag, err)
	}
	return engine.Resolved(), nil
}

func (f postgresFlags) databaseConfig(passwordEnv string, settings toolPostgresPoolSettings) forma.DatabaseConfig {
	timeout := settings.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	maxConnections := settings.maxConnections
	if maxConnections <= 0 {
		maxConnections = 4
	}

	return forma.DatabaseConfig{
		Host:            f.host,
		Port:            f.port,
		Database:        f.database,
		Username:        f.user,
		Password:        f.resolvedPassword(passwordEnv),
		SSLMode:         f.sslMode,
		Engine:          forma.DatabaseEngine(f.engine),
		MaxConnections:  maxConnections,
		MaxIdleConns:    settings.maxIdleConns,
		ConnMaxLifetime: 0,
		ConnMaxIdleTime: 0,
		Timeout:         timeout,
	}
}

type s3Flags struct {
	bucket   string
	prefix   string
	endpoint string
	region   string
	useSSL   bool
	usePath  bool
}

type s3FlagOptions struct {
	includePrefix  bool
	prefixFlag     string
	prefixDefault  string
	prefixUsage    string
	bucketUsage    string
	bucketRequired bool
}

func (f *s3Flags) register(fs *flag.FlagSet, opts s3FlagOptions) {
	fs.StringVar(&f.bucket, "s3-bucket", "", opts.bucketUsage)
	if opts.includePrefix {
		fs.StringVar(&f.prefix, opts.prefixFlag, opts.prefixDefault, opts.prefixUsage)
	}
	fs.StringVar(&f.endpoint, "s3-endpoint", "", "S3 endpoint (for MinIO)")
	fs.StringVar(&f.region, "s3-region", "us-east-1", "S3 region")
	fs.BoolVar(&f.useSSL, "s3-use-ssl", true, "Use SSL for S3")
	fs.BoolVar(&f.usePath, "s3-use-path", false, "Use path-style S3 addressing")
}

func (f s3Flags) validate(bucketRequired bool) error {
	if bucketRequired && f.bucket == "" {
		return fmt.Errorf("--s3-bucket is required")
	}
	return nil
}

type schemaRegistryFlags struct {
	table string
	dir   string
}

func (f *schemaRegistryFlags) register(fs *flag.FlagSet, required bool) {
	helpSuffix := "optional"
	if required {
		helpSuffix = "required"
	}
	fs.StringVar(&f.table, "schema-registry-table", "", fmt.Sprintf("Schema registry table name (%s)", helpSuffix))
	fs.StringVar(&f.dir, "schema-dir", "", fmt.Sprintf("Directory with *_attributes.json files (%s)", helpSuffix))
}

func (f schemaRegistryFlags) validate(required bool) error {
	if required {
		if f.table == "" {
			return fmt.Errorf("--schema-registry-table is required")
		}
		if f.dir == "" {
			return fmt.Errorf("--schema-dir is required")
		}
		return nil
	}

	if (f.table == "") != (f.dir == "") {
		return fmt.Errorf("both --schema-registry-table and --schema-dir are required together")
	}
	return nil
}

type duckExportFlags struct {
	duckDBPath              string
	duckThreads             int
	duckMemLimit            string
	queryTimeout            time.Duration
	parquetCompression      string
	parquetCompressionLevel int
}

type duckExportFlagOptions struct {
	memLimitDefault string
	queryTimeout    time.Duration
}

func (f *duckExportFlags) register(fs *flag.FlagSet, opts duckExportFlagOptions) {
	fs.StringVar(&f.duckDBPath, "duckdb-path", "", "DuckDB path (empty for :memory:)")
	fs.IntVar(&f.duckThreads, "duck-threads", 4, "DuckDB thread count")
	fs.StringVar(&f.duckMemLimit, "duck-mem-limit", opts.memLimitDefault, "DuckDB memory limit")
	fs.DurationVar(&f.queryTimeout, "query-timeout", opts.queryTimeout, "Query timeout")
	fs.StringVar(&f.parquetCompression, "parquet-compression", "zstd", "Parquet compression codec")
	fs.IntVar(&f.parquetCompressionLevel, "parquet-compression-level", 3, "Parquet compression level")
}

// validateManifestTemplateFlag runs the read path's two-probe manifest
// template check (#300) on a V1 manifest tool's --manifest-template (the
// compactor, manifest-reconcile and baseline-schema), so a constant path, a
// {{.SchemaId}} typo or a printf verb is a flag error before any connection
// is opened (#518, #520). An empty template is left alone:
// manifest.PathResolver substitutes its default. cdc-flush and cdc-init take
// no template: their V2 manifests live under the -s3-prefix layout.
func validateManifestTemplateFlag(tmpl string) error {
	if tmpl == "" {
		return nil
	}
	if err := forma.ValidateManifestPathTemplate("manifest-template", tmpl); err != nil {
		return fmt.Errorf("validate --manifest-template: %w", err)
	}
	return nil
}

// validateParquetPrefixFlag applies the server's duckdb.s3DataPrefix rule to
// a tool's parquet prefix flag (#527): a glob metacharacter is a flag error
// before any connection is opened. The writers store the prefix as a literal
// S3 key while the reader's read_parquet would expand it, so a writer must
// never produce keys the reader cannot address; forma.ValidateParquetPrefix
// is the one rule both sides share.
func validateParquetPrefixFlag(flagName, prefix string) error {
	if err := forma.ValidateParquetPrefix(flagName, prefix); err != nil {
		return fmt.Errorf("validate --%s: %w", flagName, err)
	}
	return nil
}

// registerDatabaseFlags registers the connection and table flags init-db and
// drop-schema share on flags, into opts and pg. It answers the error of an
// unparsable DB_PORT, which the caller refuses after flags.Parse, so --help
// still prints usage.
func registerDatabaseFlags(flags *flag.FlagSet, opts *initDBOptions, pg *postgresFlags) error {
	// DB_PORT is --db-port's default, so a set but unparsable value is
	// refused even when the flag would override it (#600). The refusal waits
	// until after flags.Parse so --help still prints usage; EnvInt returns
	// 5432 alongside the error to register the flag with.
	portDefault, portErr := bootstrap.EnvInt("DB_PORT", 5432)

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
	return portErr
}
