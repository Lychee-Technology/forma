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

// TestManifestUnstampedErrorIsASchemaMismatch pins the #522 dual unwrap: a
// zero-stamped manifest that lists entries cannot prove which schema owns
// them, so it is a manifest whose identity does not match the requested
// schema — every classification that keys on ErrManifestSchemaMismatch
// (httpapi redaction class, non-degradable federated read) applies unchanged,
// while ErrManifestUnstamped still tells the remedy apart (set schema_id on
// the object, rather than fix the template).
func TestManifestUnstampedErrorIsASchemaMismatch(t *testing.T) {
	inner := &ManifestUnstampedError{RequestedSchemaID: 4, Path: "manifest/shared.json", Entries: 3}
	err := fmt.Errorf("load manifest: %w", inner)

	require.ErrorIs(t, err, ErrManifestUnstamped)
	require.ErrorIs(t, err, ErrManifestSchemaMismatch)

	var typed *ManifestUnstampedError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(4), typed.RequestedSchemaID)
	require.Equal(t, "manifest/shared.json", typed.Path)
	require.Equal(t, 3, typed.Entries)

	require.ErrorContains(t, err, "manifest/shared.json")
	require.ErrorContains(t, err, "3 entries listed under schema_id 0")
	require.ErrorContains(t, err, "schema 4")
	require.ErrorContains(t, err, "set schema_id")
}
