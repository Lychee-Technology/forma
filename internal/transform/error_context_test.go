package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// notFoundRegistry answers every id lookup with the registry's own 404
// carrier, so a test can see whether a wrap above it keeps the publication.
type notFoundRegistry struct {
	*stubSchemaRegistry
}

func (notFoundRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, forma.NotFoundf("schema not found")
}

// The wraps #625 added name the operation that failed for the operator and
// leave the published message as the callee built it: a plain fmt.Errorf
// wrap is stepped over by forma.ResolvePublicMessage.
func TestErrorContextKeepsPublishedMessage(t *testing.T) {
	ctx := context.Background()

	t.Run("metadata lookup of a write", func(t *testing.T) {
		registry := notFoundRegistry{newStubSchemaRegistry().(*stubSchemaRegistry)}
		_, err := NewTransformer(registry).ToAttributes(ctx, 7, uuid.Must(uuid.NewV7()), map[string]any{"name": "a"})
		require.ErrorContains(t, err, "load metadata of schema 7: ")
		require.True(t, errors.Is(err, forma.ErrNotFound))
		msg, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok)
		require.Equal(t, "schema not found", msg)
	})

	t.Run("walk of the merged document", func(t *testing.T) {
		tr := NewPersistentRecordTransformer(newStubSchemaRegistry())
		rowID := uuid.Must(uuid.NewV7())
		stored, err := tr.ToPersistentRecord(ctx, 100, rowID, map[string]any{"name": "a"})
		require.NoError(t, err)

		// The merge walk returns one error of its own, the nesting cap.
		_, err = tr.MergeUpdate(ctx, stored, setting(cyclicMapPayload()))
		require.ErrorContains(t, err, "walk the merged document of row "+rowID.String()+": ")
		requireTooDeep(t, err, "self")
	})
}
