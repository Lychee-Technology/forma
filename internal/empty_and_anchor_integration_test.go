package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyAndReachesEntitiesWithoutEAVRows pins the anchor an empty and
// group takes. It renders 1=1 and holds for every entity, so like is_null
// it forces the entity_main anchor: an eav_data anchor never visits an
// entity with no eav_data row (#649 review). CrossSchemaSearch without a
// condition searches with exactly that group, so before the fix it left out
// every entity whose attributes are all column-bound.
func TestEmptyAndReachesEntitiesWithoutEAVRows(t *testing.T) {
	e := newCondPG(t)
	withEAV := e.create(t, map[string]any{"version": int64(1), "name": "a"})
	withoutEAV := e.create(t, map[string]any{"version": int64(2)})
	want := map[uuid.UUID]bool{withEAV: true, withoutEAV: true}

	assert.Equal(t, want, e.matching(t, and()), "Query with an empty and")

	result, err := e.em.CrossSchemaSearch(e.ctx, &forma.CrossSchemaRequest{
		SchemaNames: []string{condSchemaName}, SearchTerm: "any", Page: 1, ItemsPerPage: 10,
	})
	require.NoError(t, err)
	got := make(map[uuid.UUID]bool, len(result.Data))
	for _, record := range result.Data {
		got[record.RowID] = true
	}
	assert.Equal(t, want, got, "CrossSchemaSearch without a condition")
}
