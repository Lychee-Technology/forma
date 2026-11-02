package federated

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	fedengine "github.com/lychee-technology/forma/internal/federated"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	externalPGDSNEnv       = "FEDERATED_E2E_EXTERNAL_PG_DSN"
	externalPGUserEnv      = "FEDERATED_E2E_EXTERNAL_PG_USER"
	externalPGPasswordEnv  = "FEDERATED_E2E_EXTERNAL_PG_PASSWORD"
	externalPGDBEnv        = "FEDERATED_E2E_EXTERNAL_PG_DB"
	externalPGSSLModeEnv   = "FEDERATED_E2E_EXTERNAL_PG_SSLMODE"
	externalS3EndpointEnv  = "FEDERATED_E2E_EXTERNAL_S3_ENDPOINT"
	externalS3BucketEnv    = "FEDERATED_E2E_EXTERNAL_S3_BUCKET"
	externalS3PrefixEnv    = "FEDERATED_E2E_EXTERNAL_S3_PREFIX"
	externalS3RegionEnv    = "FEDERATED_E2E_EXTERNAL_S3_REGION"
	externalS3AccessKeyEnv = "FEDERATED_E2E_EXTERNAL_S3_ACCESS_KEY"
	externalS3SecretKeyEnv = "FEDERATED_E2E_EXTERNAL_S3_SECRET_KEY"
)

type externalFederatedConfig struct {
	Enabled       bool
	UseExternalPG bool
	UseExternalS3 bool
	PGDSN         string
	PGHost        string
	PGPort        string
	PGUser        string
	PGPassword    string
	PGDatabase    string
	PGSSLMode     string
	S3Endpoint    string
	S3Bucket      string
	S3Prefix      string
	S3Region      string
	S3AccessKey   string
	S3SecretKey   string
}

// toFloat64 converts numeric types to float64.
func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}

// parsePGDSN extracts host and port from a Postgres DSN.
// DSN format: postgres://user:password@host:port/database?sslmode=disable
func parsePGDSN(dsn string) (host, port string) {
	// Default values
	host = "localhost"
	port = "5432"

	// Remove protocol prefix
	dsn = strings.TrimPrefix(dsn, "postgres://")
	dsn = strings.TrimPrefix(dsn, "postgresql://")

	// Find the @ separator (after credentials)
	atIdx := strings.Index(dsn, "@")
	if atIdx == -1 {
		return
	}
	dsn = dsn[atIdx+1:]

	// Find the / separator (before database name)
	slashIdx := strings.Index(dsn, "/")
	if slashIdx != -1 {
		dsn = dsn[:slashIdx]
	}

	// Find the ? separator (before query params)
	questionIdx := strings.Index(dsn, "?")
	if questionIdx != -1 {
		dsn = dsn[:questionIdx]
	}

	// Now dsn should be "host:port"
	colonIdx := strings.LastIndex(dsn, ":")
	if colonIdx != -1 {
		host = dsn[:colonIdx]
		port = dsn[colonIdx+1:]
	} else {
		host = dsn
	}

	return
}

// loadExternalFederatedConfigFromEnv reads the external-infrastructure
// settings. Neither a Postgres DSN nor an S3 endpoint means the harness starts
// its own containers; either one switches to external mode, which always needs
// the S3 endpoint and uses external Postgres only when the DSN is set.
func loadExternalFederatedConfigFromEnv() (*externalFederatedConfig, error) {
	pgDSN := strings.TrimSpace(os.Getenv(externalPGDSNEnv))
	s3Endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(externalS3EndpointEnv)), "/")
	if pgDSN == "" && s3Endpoint == "" {
		return &externalFederatedConfig{}, nil
	}
	if s3Endpoint == "" {
		return nil, fmt.Errorf("external federated harness requires %s when external configuration is enabled", externalS3EndpointEnv)
	}

	cfg := &externalFederatedConfig{
		Enabled:       true,
		UseExternalPG: pgDSN != "",
		UseExternalS3: true,
		PGDSN:         pgDSN,
		S3Endpoint:    s3Endpoint,
	}
	if err := loadExternalPGSettings(cfg); err != nil {
		return nil, fmt.Errorf("load external postgres settings: %w", err)
	}
	if err := loadExternalS3Settings(cfg); err != nil {
		return nil, fmt.Errorf("load external s3 settings: %w", err)
	}
	return cfg, nil
}

// loadExternalPGSettings fills the Postgres connection settings of cfg. Each
// comes from its environment variable first; when external Postgres is in use
// and the user or database is unset, the unset ones are taken from the DSN;
// whatever is still empty falls back to the local-container default.
func loadExternalPGSettings(cfg *externalFederatedConfig) error {
	cfg.PGUser = strings.TrimSpace(os.Getenv(externalPGUserEnv))
	cfg.PGPassword = os.Getenv(externalPGPasswordEnv)
	cfg.PGDatabase = strings.TrimSpace(os.Getenv(externalPGDBEnv))
	cfg.PGSSLMode = strings.TrimSpace(os.Getenv(externalPGSSLModeEnv))
	if cfg.UseExternalPG && (cfg.PGUser == "" || cfg.PGDatabase == "") {
		parsedURL, err := url.Parse(cfg.PGDSN)
		if err != nil {
			return fmt.Errorf("parse external postgres dsn: %w", err)
		}
		fillPGSettingsFromDSN(cfg, parsedURL)
	}
	cfg.PGUser = valueOrDefault(cfg.PGUser, "postgres")
	cfg.PGDatabase = valueOrDefault(cfg.PGDatabase, "postgres")
	cfg.PGSSLMode = valueOrDefault(cfg.PGSSLMode, "disable")
	cfg.PGHost, cfg.PGPort = parsePGDSN(cfg.PGDSN)
	return nil
}

// fillPGSettingsFromDSN sets every Postgres setting of cfg that is still empty
// from the matching part of the parsed DSN, leaving the ones already set alone.
func fillPGSettingsFromDSN(cfg *externalFederatedConfig, parsedURL *url.URL) {
	if parsedURL.User != nil {
		if cfg.PGUser == "" {
			cfg.PGUser = parsedURL.User.Username()
		}
		if password, ok := parsedURL.User.Password(); ok && cfg.PGPassword == "" {
			cfg.PGPassword = password
		}
	}
	if cfg.PGDatabase == "" {
		cfg.PGDatabase = strings.TrimPrefix(parsedURL.Path, "/")
	}
	if cfg.PGSSLMode == "" {
		cfg.PGSSLMode = parsedURL.Query().Get("sslmode")
	}
}

// loadExternalS3Settings fills the S3 bucket, prefix, region and credentials of
// cfg. The first three default to the layout the local MinIO container uses;
// the credentials have no default, so external mode fails without both.
func loadExternalS3Settings(cfg *externalFederatedConfig) error {
	cfg.S3Bucket = valueOrDefault(strings.TrimSpace(os.Getenv(externalS3BucketEnv)), "test-bucket")
	cfg.S3Prefix = valueOrDefault(strings.TrimSpace(os.Getenv(externalS3PrefixEnv)), "test-project")
	cfg.S3Region = valueOrDefault(strings.TrimSpace(os.Getenv(externalS3RegionEnv)), "us-east-1")
	cfg.S3AccessKey = strings.TrimSpace(os.Getenv(externalS3AccessKeyEnv))
	cfg.S3SecretKey = os.Getenv(externalS3SecretKeyEnv)
	if cfg.S3AccessKey == "" || cfg.S3SecretKey == "" {
		return fmt.Errorf("external federated harness requires both %s and %s", externalS3AccessKeyEnv, externalS3SecretKeyEnv)
	}
	return nil
}

// valueOrDefault returns value, or fallback when value is empty.
func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// GetS3Client returns the S3 client for direct access.
func (h *FederatedTestHarness) GetS3Client() *s3.Client {
	return h.s3Client
}

// GetDuckDB returns the DuckDB client for direct access.
func (h *FederatedTestHarness) GetDuckDB() *fedengine.DuckDBClient {
	return h.Duck
}

// GetPostgresDB returns the Postgres connection for direct access.
func (h *FederatedTestHarness) GetPostgresDB() *sql.DB {
	return h.PGDB
}
