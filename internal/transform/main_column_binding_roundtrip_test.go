package transform_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

// #470 refuses an attribute named after an entity_main column; a correctly
// named attribute bound to one must still register through the file registry
// and round-trip through that column.
func TestCorrectlyNamedBoundAttributeRoundTrips(t *testing.T) {
	dir := t.TempDir()
	writeJSON := func(name string, v any) {
		data, err := json.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}
	writeJSON("lead.json", map[string]any{"type": "object", "properties": map[string]any{
		"status": map[string]any{"type": "string"},
		"score":  map[string]any{"type": "integer"},
	}})
	writeJSON("lead_attributes.json", map[string]any{
		"status": map[string]any{"attributeID": 1, "valueType": "text",
			"column_binding": map[string]any{"col_name": "text_01"}},
		"score": map[string]any{"attributeID": 2, "valueType": "integer",
			"column_binding": map[string]any{"col_name": "integer_03"}},
	})
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)

	prt := transform.NewPersistentRecordTransformer(registry)
	record, err := prt.ToPersistentRecord(t.Context(), schemaID, uuid.Must(uuid.NewV7()),
		map[string]any{"status": "open", "score": 42})
	require.NoError(t, err)
	assert.Equal(t, "open", record.TextItems["text_01"])
	assert.Equal(t, int32(42), record.Int32Items["integer_03"])
	assert.Empty(t, record.OtherAttributes, "bound attributes must not spill into EAV")

	rebuilt, err := prt.FromPersistentRecord(t.Context(), record)
	require.NoError(t, err)
	assert.Equal(t, "open", rebuilt["status"])
	assert.EqualValues(t, 42, rebuilt["score"])
}
