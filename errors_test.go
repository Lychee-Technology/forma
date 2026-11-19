package forma

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublicationConsistencyErrorIsPublic pins the carrier of a published
// read's consistency fault (#759) in the root package: an embedder can match
// it, and internal/httpapi can classify it without importing
// internal/federated, which would pull DuckDB CGO into a pure-Go test build.
func TestPublicationConsistencyErrorIsPublic(t *testing.T) {
	fault := errors.New("the generation lists objects the store does not hold")
	inner := &PublicationConsistencyError{
		SchemaID: 22, Epoch: 3, Generation: 9,
		MissingKeys: []string{"data/22/e3/base/a.parquet", "data/22/e3/base/b.parquet"},
		Err:         fault,
	}
	err := fmt.Errorf("federated read: %w", inner)

	require.ErrorIs(t, err, ErrPublicationInconsistent)
	require.ErrorIs(t, err, fault)

	var typed *PublicationConsistencyError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(22), typed.SchemaID)
	require.Equal(t, int64(3), typed.Epoch)
	require.Equal(t, int64(9), typed.Generation)

	require.ErrorContains(t, err, "schema 22 epoch 3 generation 9")
	require.ErrorContains(t, err, "2 listed object(s) missing from storage")
	require.ErrorContains(t, err, "data/22/e3/base/a.parquet")
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
