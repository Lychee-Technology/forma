package internal

import (
	"context"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Basic config validation tests for Postgres and S3 helpers.
func TestValidatePostgresAndS3Config(t *testing.T) {
	// Postgres config validation
	pg := forma.DatabaseConfig{
		Host:           "",
		Port:           5432,
		MaxConnections: 5,
	}
	err := ValidatePostgresConfig(pg)
	require.Error(t, err, "empty host should fail validation")

	pg.Host = "localhost"
	err = ValidatePostgresConfig(pg)
	require.NoError(t, err, "valid postgres config should pass")

	// S3 config validation
	s3 := forma.DuckDBConfig{EnableS3: true}
	err = ValidateS3Config(s3)
	require.Error(t, err, "enableS3 without endpoint or creds should fail")

	s3.S3Endpoint = "http://localhost:9000"
	err = ValidateS3Config(s3)
	require.NoError(t, err, "endpoint-only S3 config allowed for basic checks")

	// credential mismatch
	s3 = forma.DuckDBConfig{EnableS3: true, S3AccessKey: "k"}
	err = ValidateS3Config(s3)
	require.Error(t, err, "access key without secret should fail")
}

// Postgres health check should error on empty DSN.
func TestPostgresHealthCheck_EmptyDSN(t *testing.T) {
	err := PostgresHealthCheck(context.Background(), "", 0)
	require.Error(t, err)
}

// Ensure BuildDuckDBQuery injects PgMainClause and Anchor.Condition when dual clauses are provided.
func TestBuildDuckDBQuery_Injection(t *testing.T) {
	tpl := template.Must(template.New("test").Parse("PGCLAUSE:{{.PgMainClause}}|COND:{{.Anchor.Condition}}"))
	dual := &DualClauses{
		DuckClause:   "age > ?",
		DuckArgs:     []any{18},
		PgMainClause: "integer_01 > 18",
		PgMainArgs:   []any{},
	}

	sql, args, err := BuildDuckDBQuery(tpl, map[string]any{}, nil, nil, dual)
	require.NoError(t, err)
	require.Contains(t, sql, "PGCLAUSE:integer_01 > 18")
	require.Contains(t, sql, "COND:age > ?")
	require.Len(t, args, 1)
	require.Equal(t, 18, args[0])
}
