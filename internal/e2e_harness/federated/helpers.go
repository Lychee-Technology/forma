package federated

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal"
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

func loadExternalFederatedConfigFromEnv() (*externalFederatedConfig, error) {
	pgDSN := strings.TrimSpace(os.Getenv(externalPGDSNEnv))
	s3Endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(externalS3EndpointEnv)), "/")
	if pgDSN == "" && s3Endpoint == "" {
		return &externalFederatedConfig{}, nil
	}
	if s3Endpoint == "" {
		return nil, fmt.Errorf("external federated harness requires %s when external configuration is enabled", externalS3EndpointEnv)
	}

	useExternalPG := pgDSN != ""

	pgUser := strings.TrimSpace(os.Getenv(externalPGUserEnv))
	pgPassword := os.Getenv(externalPGPasswordEnv)
	pgDatabase := strings.TrimSpace(os.Getenv(externalPGDBEnv))
	pgSSLMode := strings.TrimSpace(os.Getenv(externalPGSSLModeEnv))
	if useExternalPG && (pgUser == "" || pgDatabase == "") {
		parsedURL, err := url.Parse(pgDSN)
		if err != nil {
			return nil, fmt.Errorf("parse external postgres dsn: %w", err)
		}
		if pgUser == "" && parsedURL.User != nil {
			pgUser = parsedURL.User.Username()
		}
		if pgPassword == "" && parsedURL.User != nil {
			if password, ok := parsedURL.User.Password(); ok {
				pgPassword = password
			}
		}
		if pgDatabase == "" {
			pgDatabase = strings.TrimPrefix(parsedURL.Path, "/")
		}
		if pgSSLMode == "" {
			pgSSLMode = parsedURL.Query().Get("sslmode")
		}
	}
	if pgUser == "" {
		pgUser = "postgres"
	}
	if pgDatabase == "" {
		pgDatabase = "postgres"
	}
	if pgSSLMode == "" {
		pgSSLMode = "disable"
	}

	pgHost, pgPort := parsePGDSN(pgDSN)
	s3Bucket := strings.TrimSpace(os.Getenv(externalS3BucketEnv))
	if s3Bucket == "" {
		s3Bucket = "test-bucket"
	}
	s3Prefix := strings.TrimSpace(os.Getenv(externalS3PrefixEnv))
	if s3Prefix == "" {
		s3Prefix = "test-project"
	}
	s3Region := strings.TrimSpace(os.Getenv(externalS3RegionEnv))
	if s3Region == "" {
		s3Region = "us-east-1"
	}
	s3AccessKey := strings.TrimSpace(os.Getenv(externalS3AccessKeyEnv))
	s3SecretKey := os.Getenv(externalS3SecretKeyEnv)
	if s3AccessKey == "" || s3SecretKey == "" {
		return nil, fmt.Errorf("external federated harness requires both %s and %s", externalS3AccessKeyEnv, externalS3SecretKeyEnv)
	}

	return &externalFederatedConfig{
		Enabled:       true,
		UseExternalPG: useExternalPG,
		UseExternalS3: true,
		PGDSN:         pgDSN,
		PGHost:        pgHost,
		PGPort:        pgPort,
		PGUser:        pgUser,
		PGPassword:    pgPassword,
		PGDatabase:    pgDatabase,
		PGSSLMode:     pgSSLMode,
		S3Endpoint:    s3Endpoint,
		S3Bucket:      s3Bucket,
		S3Prefix:      s3Prefix,
		S3Region:      s3Region,
		S3AccessKey:   s3AccessKey,
		S3SecretKey:   s3SecretKey,
	}, nil
}

// GetS3Client returns the S3 client for direct access.
func (h *FederatedTestHarness) GetS3Client() *s3.Client {
	return h.s3Client
}

// GetDuckDB returns the DuckDB client for direct access.
func (h *FederatedTestHarness) GetDuckDB() *internal.DuckDBClient {
	return h.Duck
}

// GetPostgresDB returns the Postgres connection for direct access.
func (h *FederatedTestHarness) GetPostgresDB() *sql.DB {
	return h.PGDB
}
