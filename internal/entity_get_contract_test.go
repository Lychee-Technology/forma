package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// Get's half of the read contract (#635): Get reads PostgreSQL and nothing
// else. A deployment that runs Query through the federated engine, with the
// degraded fallback on, does not route it (#715), a row PostgreSQL does not
// hold is not found, and a failed PostgreSQL read fails the Get instead of
// falling back to the lakehouse tiers.

// failingGetRepository stands in for an unreachable PostgreSQL on the
// single-row read.
type failingGetRepository struct {
	*mockPersistentRecordRepository
	err error
}

func (r *failingGetRepository) GetPersistentRecord(context.Context, model.StorageTables, int16, uuid.UUID) (*model.PersistentRecord, error) {
	r.getCalls++
	return nil, r.err
}

// getRequest asks for one visit row.
func getRequest(rowID uuid.UUID) *forma.QueryRequest {
	return &forma.QueryRequest{SchemaName: "visit", RowID: &rowID}
}

func newGetContractManager(t *testing.T, repo model.PersistentRecordRepository, engine model.FederatedQueryEngine) (forma.EntityManager, model.PersistentRecordTransformer, int16) {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	require.NoError(t, err)
	transformer := transform.NewPersistentRecordTransformer(registry)
	config := createTestConfig()
	config.DuckDB.Enabled = true
	config.DuckDB.AllowPartialDegradedMode = true
	return mustNewEntityManager(t, transformer, repo, engine, registry, config, nil), transformer, schemaID
}

func TestGetReadsPostgresOnAFederatedDeployment(t *testing.T) {
	ctx := context.Background()
	repo := newMockPersistentRecordRepository()
	engine := &countingEngine{}
	em, transformer, schemaID := newGetContractManager(t, repo, engine)
	rowID := uuid.New()
	record, err := transformer.ToPersistentRecord(ctx, schemaID, rowID, visitPayload("get-contract-1"))
	require.NoError(t, err)
	repo.storeRecord(record)

	got, err := em.Get(ctx, getRequest(rowID))

	require.NoError(t, err)
	require.Equal(t, rowID, got.RowID)
	require.Equal(t, "get-contract-1", got.Attributes["id"])
	require.Equal(t, 1, repo.getCalls)
	require.Zero(t, engine.calls, "Get must never reach the federated engine")
}

func TestGetOfARowPostgresDoesNotHoldIsNotFound(t *testing.T) {
	repo := newMockPersistentRecordRepository()
	engine := &countingEngine{}
	em, _, _ := newGetContractManager(t, repo, engine)

	got, err := em.Get(context.Background(), getRequest(uuid.New()))

	require.ErrorIs(t, err, forma.ErrNotFound)
	require.Nil(t, got)
	require.Zero(t, engine.calls, "a row missing from PostgreSQL must not be looked up in the lakehouse tiers")
}

func TestGetFailsWhenPostgresFails(t *testing.T) {
	down := errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
	repo := &failingGetRepository{mockPersistentRecordRepository: newMockPersistentRecordRepository(), err: down}
	engine := &countingEngine{}
	em, _, _ := newGetContractManager(t, repo, engine)

	got, err := em.Get(context.Background(), getRequest(uuid.New()))

	require.ErrorIs(t, err, down)
	require.NotErrorIs(t, err, forma.ErrNotFound, "an unreachable PostgreSQL is a failure, not a missing row")
	require.Nil(t, got)
	require.Equal(t, 1, repo.getCalls)
	require.Zero(t, engine.calls, "Get must not fall back to the federated engine")
}
