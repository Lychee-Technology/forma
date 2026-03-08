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
}

func (f postgresFlags) resolvedPassword(fallbackEnv string) string {
	if f.password != "" {
		return f.password
	}
	return bootstrap.Env(fallbackEnv, "")
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
