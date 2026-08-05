package schemameta

import (
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ID-keyed lookups withhold the internal int16 from the published message —
// the caller never supplied it — while Error() keeps it for the operator log
// (#313). Name-keyed lookups publish the name: it is the caller's own input.
func TestRegistryNotFoundPublications(t *testing.T) {
	registry, err := NewFileSchemaRegistryFromDirectory("../../cmd/server/schemas")
	require.NoError(t, err)

	t.Run("id-keyed withholds the internal id", func(t *testing.T) {
		_, _, err := registry.GetSchemaAttributeCacheByID(404)
		require.Error(t, err)
		require.ErrorIs(t, err, forma.ErrNotFound)

		var pub forma.PublicError
		require.True(t, errors.As(err, &pub), "registry miss publishes no client message: %v", err)
		require.Equal(t, "schema not found", pub.PublicMessage())
		require.True(t, forma.HasOperatorDetail(err))
		require.Contains(t, err.Error(), "schema id 404", "the operator copy must keep the id")
	})

	t.Run("name-keyed publishes the caller's name", func(t *testing.T) {
		_, _, err := registry.GetSchemaAttributeCacheByName("nosuchschema")
		require.Error(t, err)
		require.ErrorIs(t, err, forma.ErrNotFound)

		var pub forma.PublicError
		require.True(t, errors.As(err, &pub))
		require.Equal(t, "schema not found: nosuchschema", pub.PublicMessage())
		require.False(t, strings.Contains(pub.PublicMessage(), "schema id"))
	})
}
