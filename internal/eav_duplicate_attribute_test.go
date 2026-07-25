package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// TestUpdateWithDottedAttributeProducesOneEAVRecord is the #312 regression at the
// update path's real seam. mergeMaps is key-literal while FromPersistentRecord
// re-nests stored attributes, so PUT {"person.name":"new"} on an entity already
// holding that attribute merges to both spellings at once. Before the dedupe
// this produced two eav_data rows sharing
// (schema_id, row_id, attr_id, array_indices) and the INSERT failed on 23505.
func TestUpdateWithDottedAttributeProducesOneEAVRecord(t *testing.T) {
	registry := newStubSchemaRegistry()
	recordTransformer := transform.NewPersistentRecordTransformer(registry)
	rowID := uuid.Must(uuid.NewV7())

	// What FromPersistentRecord hands back for a stored "person.name".
	stored := map[string]any{
		"person": map[string]any{"name": "old"},
	}
	merged := mergeMaps(stored, map[string]any{"person.name": "new"})
	require.Equal(t, map[string]any{
		"person":      map[string]any{"name": "old"},
		"person.name": "new",
	}, merged, "mergeMaps is key-literal, so both spellings must reach the transformer")

	record, err := recordTransformer.ToPersistentRecord(context.Background(), 100, rowID, merged)
	require.NoError(t, err)

	require.Len(t, record.OtherAttributes, 1)
	require.Equal(t, int16(3), record.OtherAttributes[0].AttrID)
	require.NotNil(t, record.OtherAttributes[0].ValueText)
	require.Equal(t, "new", *record.OtherAttributes[0].ValueText)
}
