package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The shape rule (#696) over the shipped lead schema, on real PostgreSQL and
// with no validator: contact.memo is a nested scalar, contact.phones a list,
// and propertyInterests an array of objects whose propertyId is a scalar
// member. A container at a scalar is refused in the nested and literal
// spellings and inside the array of objects, and the row's tables are left
// as they were. A list's array, its empty-list marker and an array of objects
// are written as before.
func TestScalarShapeOverTheShippedLeadSchema(t *testing.T) {
	env := &txEnv{integrationEnv: setupIntegrationEnv(t)}
	created, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead"},
		Type:             forma.OperationCreate,
		Data: map[string]any{
			"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
			"pipeline": "buy", "stage": "new", "status": "open",
			"contact":           map[string]any{"isAnonymous": false, "memo": "m", "phones": []any{"1"}},
			"propertyInterests": []any{map[string]any{"propertyId": "p1", "status": "viewed"}},
			"createdAt":         "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
		},
	})
	require.NoError(t, err)
	rowID := created.RowID

	for name, refused := range map[string]struct {
		attr, reason string
		updates      map[string]any
	}{
		"nested":                {"contact.memo", "got an empty array", map[string]any{"contact": map[string]any{"memo": []any{}}}},
		"literal":               {"contact.memo", "got an empty object", map[string]any{"contact.memo": map[string]any{}}},
		"singleton":             {"contact.memo", "got an array", map[string]any{"contact": map[string]any{"memo": []any{"m2"}}}},
		"array-of-objects":      {"propertyInterests.propertyId", "got an array", map[string]any{"propertyInterests": []any{map[string]any{"propertyId": []any{"p2"}, "status": "viewed"}}}},
		"literal across arrays": {"propertyInterests.propertyId", "got an array", map[string]any{"propertyInterests.propertyId": []any{"p2"}}},
		"object in a list":      {"contact.phones", "got an empty object", map[string]any{"contact": map[string]any{"phones": []any{map[string]any{}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			before := env.snapshot(t, "lead", rowID)
			_, err := env.manager.Update(env.ctx, txUpdateOp("lead", rowID, refused.updates))
			assertShapeRefused(t, err, refused.attr, refused.reason)
			require.Equal(t, before, env.snapshot(t, "lead", rowID))
		})
	}

	for _, accepted := range []struct {
		updates map[string]any
		path    string
		want    any
	}{
		{map[string]any{"contact": map[string]any{"phones": []any{"2", "3"}}}, "contact.phones", []any{"2", "3"}},
		{map[string]any{"contact.phones": []any{}}, "contact.phones", []any{}},
		{map[string]any{"propertyInterests": []any{map[string]any{"propertyId": "p3", "status": "offered"}}},
			"propertyInterests", []any{map[string]any{"propertyId": "p3", "status": "offered"}}},
		{map[string]any{"contact": map[string]any{"memo": "m3"}}, "contact.memo", "m3"},
	} {
		_, err := env.manager.Update(env.ctx, txUpdateOp("lead", rowID, accepted.updates))
		require.NoError(t, err, "updates %v", accepted.updates)
		require.Equal(t, accepted.want, valueAtPath(env.attrs(t, "lead", rowID), accepted.path), "updates %v", accepted.updates)
	}
}
