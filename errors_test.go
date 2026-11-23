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
