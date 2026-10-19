package internal

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// A lead's requirement.areas is an array of objects whose members are all
// optional, so an area sent as {} stores no record. Before #626 the read
// padded it as null, and every update that did not replace the areas was
// refused as a null element the caller never sent. It reads as {} on create,
// Get, Query and update alike, and every update surface writes back exactly
// the stored rows beside its own change.
//
// The lead's property interests nest an object, the snapshot, inside an
// array of objects. The file registry derives the lead's array ancestry from
// its schema document, so they read as written (#623): before, the array
// read as an object of per-field arrays. The first interest holds a snapshot
// and the second notes, the case no per-field reading kept apart.
func TestUnrelatedUpdateOverObjectArrayGapIntegration(t *testing.T) {
	areas := []any{map[string]any{}, map[string]any{"city": "Tokyo"}}
	interests := []any{
		map[string]any{"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"title": "Flat A"}},
		map[string]any{"propertyId": "p2", "status": "viewed", "notes": "near the station"},
	}
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			env := setupIntegrationEnv(t)
			schemaID, cache, err := env.registry.GetSchemaAttributeCacheByName("lead")
			require.NoError(t, err)

			created, err := env.manager.Create(env.ctx, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead"},
				Type:             forma.OperationCreate,
				Data: map[string]any{
					"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
					"pipeline": "buy", "stage": "new", "status": "open",
					"contact":           map[string]any{"isAnonymous": false},
					"requirement":       map[string]any{"areas": areas},
					"propertyInterests": interests,
					"createdAt":         "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
				},
			})
			require.NoError(t, err)
			require.Equal(t, areas, valueAtPath(created.Attributes, "requirement.areas"), "create response")
			require.Equal(t, interests, created.Attributes["propertyInterests"], "create response")
			requireLeadAreas(t, env, created.RowID, "get after create", areas, interests)

			stageID := cache["stage"].AttributeID
			before := storedLeadRows(t, env, schemaID, created.RowID)
			require.Contains(t, before, fmt.Sprintf("%d[]=new", stageID), "stage must be stored in eav_data")

			updated, err := surface.update(env.ctx, env.manager, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead", RowID: created.RowID},
				Type:             forma.OperationUpdate,
				Updates:          map[string]any{"stage": "contacted"},
			})
			require.NoError(t, err)
			require.Equal(t, "contacted", updated["stage"])
			require.Equal(t, areas, valueAtPath(updated, "requirement.areas"), "update response")
			require.Equal(t, interests, updated["propertyInterests"], "update response")

			want := make([]string, 0, len(before))
			for _, row := range before {
				if row == fmt.Sprintf("%d[]=new", stageID) {
					row = fmt.Sprintf("%d[]=contacted", stageID)
				}
				want = append(want, row)
			}
			require.ElementsMatch(t, want, storedLeadRows(t, env, schemaID, created.RowID), "eav_data rows after the update")

			requireLeadAreas(t, env, created.RowID, "get after update", areas, interests)
			list, err := env.manager.Query(env.ctx, &forma.QueryRequest{SchemaName: "lead", Page: 1, ItemsPerPage: 10})
			require.NoError(t, err)
			require.Len(t, list.Data, 1)
			require.Equal(t, areas, valueAtPath(list.Data[0].Attributes, "requirement.areas"), "query")
			require.Equal(t, interests, list.Data[0].Attributes["propertyInterests"], "query")
		})
	}
}

func requireLeadAreas(t *testing.T, env *integrationEnv, rowID uuid.UUID, step string, areas, interests []any) {
	t.Helper()
	fetched, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: "lead", RowID: &rowID})
	require.NoError(t, err, step)
	require.Equal(t, areas, valueAtPath(fetched.Attributes, "requirement.areas"), step)
	require.Equal(t, interests, fetched.Attributes["propertyInterests"], step)
}

// storedLeadRows names each eav_data row of the lead by attribute id, array
// indices and value.
func storedLeadRows(t *testing.T, env *integrationEnv, schemaID int16, rowID uuid.UUID) []string {
	t.Helper()
	rows, err := env.postgresPool.Query(env.ctx, fmt.Sprintf(
		"SELECT attr_id || '[' || array_indices || ']=' || COALESCE(value_text, value_numeric::text, '<none>') "+
			"FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(env.tables.EAVData)), schemaID, rowID)
	require.NoError(t, err)
	defer rows.Close()
	var stored []string
	for rows.Next() {
		var row string
		require.NoError(t, rows.Scan(&row))
		stored = append(stored, row)
	}
	require.NoError(t, rows.Err())
	return stored
}
