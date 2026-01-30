package federated

import (
	"database/sql"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal"
)

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
