package federatedv2

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
)

// An otherwise UNPUBLISHED record at epoch 0 and generation -1 is a row no
// protocol transition writes. A process that starts while the row is there
// pins it at the epoch it holds, and the pass's own read then refuses it:
// the query fails closed with the typed, non-degradable error, even when
// degraded mode is allowed, the breaker hears nothing, and the hot rows are
// never served as if the schema were unpublished.
func TestAnInvalidRecordPinnedAtStartupFailsEveryReadClosed(t *testing.T) {
	f := newFixture(t)
	f.initialize(t, publication.Unpublished)
	f.write(t, allRows["a"], 100)
	_, err := f.pool.Exec(f.ctx, "UPDATE "+f.pub.Record+" SET epoch = 0, generation = -1 WHERE schema_id = $1", schemaID)
	require.NoError(t, err)

	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	pins, err := LoadEpochPins(f.ctx, tx, f.tables.EntityMain, []int16{schemaID})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(f.ctx))
	require.Equal(t, map[int16]int64{schemaID: 0}, pins, "the invalid row is pinned at the epoch it holds")

	breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
	e := f.engine(t, f.source(t, pins[schemaID]), closedDuckDB(t), breaker)

	for _, opts := range []*model.FederatedQueryOptions{nil, {AllowPartialDegradedMode: true}} {
		page, err := e.Query(f.ctx, f.tables, query(), opts)
		require.Nil(t, page)
		require.ErrorIs(t, err, federated.ErrPublicationInconsistent)
		require.ErrorIs(t, err, publication.ErrRecordInvalid)
		require.NotErrorIs(t, err, federated.ErrPostgresReadFailed)
		require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)
		var typed *federated.PublicationConsistencyError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, [3]int64{int64(schemaID), 0, -1}, [3]int64{int64(typed.SchemaID), typed.Epoch, typed.Generation})
		require.False(t, breaker.IsOpen())
	}
	require.Zero(t, f.store.count(), "no manifest fetched")
}
