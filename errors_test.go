package forma

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParquetSetInconsistentErrorIsPublic pins the #301 promotion: the carrier
// and its sentinel live in the root package so internal/httpapi can classify
// them for redaction without importing internal/federated, which would pull
// DuckDB CGO into a pure-Go test build. Same rationale as #299's promotion of
// ErrNoParquetPaths / ErrManifestSchemaMismatch.
func TestParquetSetInconsistentErrorIsPublic(t *testing.T) {
	inner := &ParquetSetInconsistentError{
		SchemaID:    22,
		MissingKeys: []string{"base/schema_22/a.parquet", "base/schema_22/b.parquet"},
	}
	err := fmt.Errorf("execute duckdb query: %w", inner)

	require.ErrorIs(t, err, ErrParquetSetInconsistent)

	var typed *ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(22), typed.SchemaID)
	require.Equal(t, []string{"base/schema_22/a.parquet", "base/schema_22/b.parquet"}, typed.MissingKeys)

	require.ErrorContains(t, err, "schema 22 manifest lists 2 parquet object(s) missing from storage")
	require.ErrorContains(t, err, "base/schema_22/a.parquet")
}
