package internal

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// clockRepository is the mock repository with a PostgreSQL clock: it
// answers nowMs, or err, and counts the reads.
type clockRepository struct {
	*mockPersistentRecordRepository
	nowMs int64
	err   error
	reads int
}

func (r *clockRepository) ServerTimeMillis(context.Context) (int64, error) {
	r.reads++
	return r.nowMs, r.err
}

// serverTimeFixture builds a manager over the visit schema with a recording
// engine and a clock repository, on a deployment with DuckDB as given.
func serverTimeFixture(t *testing.T, duckdb bool) (forma.EntityManager, *clockRepository, *mockFederatedQueryEngine) {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	engine := &mockFederatedQueryEngine{}
	engine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{CurrentPage: 1}, nil
	}
	repo := &clockRepository{mockPersistentRecordRepository: newMockPersistentRecordRepository(), nowMs: 1_763_351_234_567}
	config := createTestConfig()
	config.DuckDB.Enabled = duckdb
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, engine, registry, config, nil)
	return em, repo, engine
}

func serverTimeCondition() forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "status", Value: "is_not_null:true"},
		&forma.KvCondition{Attr: "scheduledStartAt", Value: "lte:$now-1h"},
	}}
}

// On a federated deployment a $now leaf is pinned before routing to one
// instant read from PostgreSQL (#715): the engine receives the instant, not
// the operand, so its PostgreSQL and DuckDB legs compare against one value.
// scheduledStartAt is a date attribute, so the operand is the start of the
// UTC day of the shifted instant.
func TestQueryPinsServerTimeOnFederatedDeployment(t *testing.T) {
	em, repo, engine := serverTimeFixture(t, true)
	cond := serverTimeCondition()

	_, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: cond,
	})
	require.NoError(t, err)
	require.Equal(t, 1, repo.reads, "the clock is read once per request")
	require.NotNil(t, engine.lastQuery)
	shifted := repo.nowMs - 3_600_000
	day := shifted - shifted%86_400_000
	require.Equal(t, &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "status", Value: "is_not_null:true"},
		&forma.KvCondition{Attr: "scheduledStartAt", Value: "lte:" + strconv.FormatInt(day, 10)},
	}}, engine.lastQuery.Condition)
	require.Equal(t, serverTimeCondition(), cond, "the caller's condition is not modified")
}

// A condition without $now pays no clock read.
func TestQueryReadsNoClockWithoutServerTime(t *testing.T) {
	em, repo, engine := serverTimeFixture(t, true)
	cond := &forma.KvCondition{Attr: "status", Value: "equals:scheduled"}

	_, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: cond,
	})
	require.NoError(t, err)
	require.Zero(t, repo.reads)
	require.Same(t, cond, engine.lastQuery.Condition)
}

// If PostgreSQL cannot answer the clock read, the query fails: it could not
// have been answered without PostgreSQL anyway (#635).
func TestQueryFailsWhenTheClockCannotBeRead(t *testing.T) {
	em, repo, engine := serverTimeFixture(t, true)
	repo.err = errors.New("connection refused")

	_, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: serverTimeCondition(),
	})
	require.ErrorIs(t, err, repo.err)
	require.Nil(t, engine.lastQuery, "the engine must not be consulted")
}

// On a deployment without DuckDB every Query runs on PostgreSQL, where $now
// renders as the statement's own now(): the condition reaches the
// repository unchanged and no separate clock read is made.
func TestQueryKeepsServerTimeOnPostgresDeployment(t *testing.T) {
	em, repo, engine := serverTimeFixture(t, false)
	cond := serverTimeCondition()

	_, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, Condition: cond,
	})
	require.NoError(t, err)
	require.Zero(t, repo.reads)
	require.Nil(t, engine.lastQuery)
	require.NotNil(t, repo.lastQuery)
	require.Equal(t, cond, repo.lastQuery.Condition)
}
