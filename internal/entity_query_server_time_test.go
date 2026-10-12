package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// $now reads the PostgreSQL transaction clock, so a federated request naming
// it is refused before routing — the engine is never consulted, whichever
// route it would have taken (#646). The same condition on the Postgres route
// reaches the repository.
func TestQueryRefusesServerTimeOnFederatedRequest(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	engine := &mockFederatedQueryEngine{}
	engine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{CurrentPage: 1}, nil
	}
	repo := newMockPersistentRecordRepository()
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, engine, registry, createTestConfig(), nil)

	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "status", Value: "is_not_null:true"},
		&forma.KvCondition{Attr: "scheduledStartAt", Value: "lte:$now-1h"},
	}}
	_, err = em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: cond,
		Federated: &forma.FederatedQueryRequest{Enabled: true},
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok)
	require.Equal(t,
		"the $now operand on date attribute 'scheduledStartAt' is not supported on a federated query: it is the PostgreSQL transaction time, which the DuckDB-evaluated tiers cannot read; send an absolute timestamp instead",
		msg)
	require.Nil(t, engine.lastQuery, "the engine must not be consulted")

	_, err = em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: cond,
	})
	require.NoError(t, err)
	require.NotNil(t, repo.lastQuery)
	require.Equal(t, cond, repo.lastQuery.Condition)
}
