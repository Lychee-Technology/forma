package internal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// leadManagerIn is a manager over env's tables and registry with a
// validator of its own: report-only update validation, or strict.
func leadManagerIn(t *testing.T, env *txEnv, strict bool) forma.EntityManager {
	t.Helper()
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), "../cmd/server/schemas")
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable, EntityMain: env.tables.EntityMain,
			EAVData: env.tables.EAVData, ChangeLog: env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: "../cmd/server/schemas", ValidateUpdatesStrict: strict},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	return mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry),
		NewDBPersistentRecordRepository(env.postgresPool, env.metadata), nil, env.registry, config, validator)
}

// createLead creates a lead with a populated contact.phones and one
// property interest.
func createLead(t *testing.T, env *txEnv) uuid.UUID {
	t.Helper()
	created, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead"},
		Type:             forma.OperationCreate,
		Data: map[string]any{
			"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
			"pipeline": "buy", "stage": "new", "status": "open",
			"contact":           map[string]any{"isAnonymous": false, "memo": "m", "phones": []any{"1", "2"}},
			"propertyInterests": []any{map[string]any{"propertyId": "p1", "status": "viewed"}},
			"createdAt":         "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
		},
	})
	require.NoError(t, err)
	return created.RowID
}

// On the shipped lead schema, propertyInterests is an array of objects, so
// propertyInterests.propertyId takes one array index. An extra array around
// that array of objects used to store propertyId at "0,0", which reads back
// as an array of its own; it is refused, as invalid input naming the
// attribute, under report-only, strict and no validation, and nothing is
// persisted. So is an object nested in an array inside contact.phones, which
// used to write neither a value nor the empty-list marker and so removed the
// list (#696).
func TestScalarShapeRefusesPositionsTheLeadSchemaDoesNotDeclare(t *testing.T) {
	env := &txEnv{integrationEnv: setupIntegrationEnv(t)}
	managers := map[string]forma.EntityManager{
		"no validator": env.manager, "report-only": leadManagerIn(t, env, false), "strict": leadManagerIn(t, env, true),
	}
	rowID := createLead(t, env)
	for mode, manager := range managers {
		for name, refused := range map[string]struct {
			attr, reason string
			updates      map[string]any
		}{
			"extra array around an array of objects": {"propertyInterests.propertyId",
				`array indices "0,0" is nested in more arrays than the attribute's arrays of objects (propertyInterests)`,
				map[string]any{"propertyInterests": []any{[]any{map[string]any{"propertyId": "p2", "status": "viewed"}}}}},
			"object nested in an array in a list": {"contact.phones",
				`list element at array indices "0,0" takes a single text value, got an empty object`,
				map[string]any{"contact": map[string]any{"phones": []any{[]any{map[string]any{}}}}}},
		} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				before := env.snapshot(t, "lead", rowID)
				_, err := manager.Update(env.ctx, txUpdateOp("lead", rowID, refused.updates))
				assertShapeRefused(t, err, refused.attr, refused.reason)
				require.Equal(t, before, env.snapshot(t, "lead", rowID))
			})
		}
	}
	attrs := env.attrs(t, "lead", rowID)
	require.Equal(t, "p1", valueAtPath(attrs, "propertyInterests").([]any)[0].(map[string]any)["propertyId"])
	require.Equal(t, []any{"1", "2"}, valueAtPath(attrs, "contact.phones"))
}

// Under strict update validation a nil Go slice at contact.phones is JSON
// null, which the schema refuses, and the list is left as it was. An empty
// one is the empty-list marker and clears the list, as before #696.
func TestStrictUpdateKeepsANilListApartFromAnEmptyOne(t *testing.T) {
	env := &txEnv{integrationEnv: setupIntegrationEnv(t)}
	strict := leadManagerIn(t, env, true)
	rowID := createLead(t, env)

	before := env.snapshot(t, "lead", rowID)
	_, err := strict.Update(env.ctx, txUpdateOp("lead", rowID, map[string]any{"contact": map[string]any{"phones": []any(nil)}}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Equal(t, before, env.snapshot(t, "lead", rowID))
	require.Equal(t, []any{"1", "2"}, valueAtPath(env.attrs(t, "lead", rowID), "contact.phones"))

	_, err = strict.Update(env.ctx, txUpdateOp("lead", rowID, map[string]any{"contact": map[string]any{"phones": []any{}}}))
	require.NoError(t, err)
	require.Equal(t, []any{}, valueAtPath(env.attrs(t, "lead", rowID), "contact.phones"))
	var markers int
	require.NoError(t, env.postgresPool.QueryRow(env.ctx,
		"SELECT COUNT(*) FROM "+sanitizeIdentifier(env.tables.EAVData)+
			" WHERE row_id = $1 AND value_text IS NULL AND value_numeric IS NULL", rowID).Scan(&markers))
	require.Equal(t, 1, markers, "the empty list writes its marker row")
}
