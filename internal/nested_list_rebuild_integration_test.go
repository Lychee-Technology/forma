package internal

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// A lead's contact.phones (a list) has a lower attribute id than every other
// contact member, including the required contact.isAnonymous, so a stored
// lead's phones are read before any of their siblings. Before #619 the read
// rebuilt them as an array of objects that the first sibling then replaced:
// a Get dropped the phones, and an update that did not touch them deleted
// them from eav_data.
func TestNestedListBesideHigherIDSiblingIntegration(t *testing.T) {
	env := setupIntegrationEnv(t)
	schemaID, cache, err := env.registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	phonesID, isAnonymousID := cache["contact.phones"].AttributeID, cache["contact.isAnonymous"].AttributeID
	require.Less(t, phonesID, isAnonymousID, "the shipped lead schema must keep contact.phones below its sibling")

	phones := []any{"03-1234-5678", "090-0000-0000"}
	created, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead"},
		Type:             forma.OperationCreate,
		Data: map[string]any{
			"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
			"pipeline": "buy", "stage": "new", "status": "open",
			"contact":   map[string]any{"isAnonymous": false, "phones": phones},
			"createdAt": "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
		},
	})
	require.NoError(t, err)
	requireLeadPhones(t, "create response", created.Attributes, phones)

	get := func(step string) {
		t.Helper()
		fetched, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: "lead", RowID: &created.RowID})
		require.NoError(t, err, step)
		requireLeadPhones(t, step, fetched.Attributes, phones)
	}
	get("get after create")

	updated, err := env.manager.Update(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead", RowID: created.RowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"stage": "contacted"},
	})
	require.NoError(t, err)
	require.Equal(t, "contacted", updated.Attributes["stage"])
	requireLeadPhones(t, "update response", updated.Attributes, phones)
	get("get after update")

	list, err := env.manager.Query(env.ctx, &forma.QueryRequest{SchemaName: "lead", Page: 1, ItemsPerPage: 10})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)
	requireLeadPhones(t, "query", list.Data[0].Attributes, phones)

	var stored int
	require.NoError(t, env.postgresPool.QueryRow(env.ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices <> ''",
		sanitizeIdentifier(env.tables.EAVData)), schemaID, created.RowID, phonesID).Scan(&stored))
	require.Equal(t, len(phones), stored, "phone rows in eav_data after the update")
}

func requireLeadPhones(t *testing.T, step string, attributes map[string]any, phones []any) {
	t.Helper()
	contact, ok := attributes["contact"].(map[string]any)
	require.True(t, ok, "%s: contact is %T, want an object", step, attributes["contact"])
	require.Equal(t, false, contact["isAnonymous"], step)
	require.Equal(t, phones, contact["phones"], step)
}
