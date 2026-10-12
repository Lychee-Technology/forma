package internal

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/testutil/faultproxy"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL proofs of #645's COMMIT classification. A fault proxy sits
// between the repository and the server and breaks the connection at the
// COMMIT. The server's own state (is the row there?) is read over a direct
// connection, so each test shows what actually happened beside what the
// writer was told.

// proxiedPools returns a pool that reaches the integration database through a
// fault proxy, the proxy, and a direct pool for setup and checks.
func proxiedPools(t *testing.T, ctx context.Context) (*pgxpool.Pool, *faultproxy.Proxy, *pgxpool.Pool) {
	t.Helper()
	direct := testdb.Connect(t, ctx)

	cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
	require.NoError(t, err)
	if strings.HasPrefix(cfg.ConnConfig.Host, "/") {
		if testdb.InCI() {
			t.Fatalf("the fault proxy needs a TCP database address, got socket %s", cfg.ConnConfig.Host)
		}
		t.Skipf("skipping: the fault proxy needs a TCP database address, got socket %s", cfg.ConnConfig.Host)
	}
	upstream := net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port)))
	proxy := faultproxy.Start(t, upstream)

	host, port, err := net.SplitHostPort(proxy.Addr())
	require.NoError(t, err)
	portNum, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	cfg.ConnConfig.Host, cfg.ConnConfig.Port = host, uint16(portNum)
	cfg.ConnConfig.TLSConfig, cfg.ConnConfig.Fallbacks = nil, nil

	proxied, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(proxied.Close)
	return proxied, proxy, direct
}

// commitTestRecord is a fresh row with one typed column set.
func commitTestRecord() *model.PersistentRecord {
	return &model.PersistentRecord{SchemaID: 1, RowID: uuid.New(), TextItems: map[string]string{"text_01": "written"}}
}

// awaitFault fails the test if the armed fault never fires.
func awaitFault(t *testing.T, fired <-chan struct{}) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("the armed fault never fired: no COMMIT reached the proxy")
	}
}

func requireUnknownCommit(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
	require.False(t, forma.IsRetryable(err), "an unknown commit is never retryable")
	require.NotErrorIs(t, err, forma.ErrRetryableConflict)
	require.NotErrorIs(t, err, forma.ErrConflict)
	_, published := forma.ResolvePublicMessage(err)
	require.False(t, published, "an unknown commit publishes nothing")
}

// requireKnownNotCommitted pins a COMMIT error that proves the transaction did
// not commit: no outcome sentinel at all, so no caller reads it as unknown or
// as safe to retry.
func requireKnownNotCommitted(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
	require.NotErrorIs(t, err, forma.ErrRetryableConflict)
	require.False(t, forma.IsRetryable(err))
	require.Contains(t, err.Error(), "transaction not committed")
}

// TestCommitReplyDroppedIsUnknownAndCommitted_PostgresOnly is Given/When/Then
// 4: the server commits, the reply is lost, and the writer reports unknown,
// not "rolled back", because the row is there.
func TestCommitReplyDroppedIsUnknownAndCommitted_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proxied, proxy, direct := proxiedPools(t, ctx)
	tables := createTempPersistentTables(t, ctx, direct)
	repo := NewDBPersistentRecordRepository(proxied, concurrencyTestCache(t, 1))

	record := commitTestRecord()
	fired := proxy.Arm(faultproxy.DropCommitReply)
	err := repo.InsertPersistentRecord(ctx, tables, record)
	awaitFault(t, fired)

	requireUnknownCommit(t, err)
	got, found := mainText(ctx, t, direct, tables, record.RowID)
	require.True(t, found, "the server committed: retrying blindly would duplicate the write")
	require.Equal(t, "written", got)
}

// TestCommitSwallowedIsUnknownAndNotCommitted_PostgresOnly is the other half:
// the COMMIT never reaches the server, which rolls back, yet the writer sees
// exactly what it saw above, a COMMIT written and no reply. It must report
// the same unknown outcome.
func TestCommitSwallowedIsUnknownAndNotCommitted_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proxied, proxy, direct := proxiedPools(t, ctx)
	tables := createTempPersistentTables(t, ctx, direct)
	cache := concurrencyTestCache(t, 1)
	repo := NewDBPersistentRecordRepository(proxied, cache)

	record := commitTestRecord()
	fired := proxy.Arm(faultproxy.SwallowCommit)
	err := repo.InsertPersistentRecord(ctx, tables, record)
	awaitFault(t, fired)

	requireUnknownCommit(t, err)
	_, found := mainText(ctx, t, direct, tables, record.RowID)
	require.False(t, found, "the COMMIT never arrived")
	// The abandoned transaction held the row's advisory lock. A create of the
	// same row can only succeed once the server has rolled it back, which
	// shows it was not left open to commit later.
	again := commitTestRecord()
	again.RowID = record.RowID
	require.NoError(t, NewDBPersistentRecordRepository(direct, cache).InsertPersistentRecord(ctx, tables, again))
}

// TestCommitNotSentIsNotUnknown_PostgresOnly pins the boundary of "unknown":
// when pgx proves the COMMIT never left the client, the writer reports a
// known not-committed error, and the row is absent.
func TestCommitNotSentIsNotUnknown_PostgresOnly(t *testing.T) {
	t.Run("connection lost before the COMMIT was sent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		proxied, proxy, direct := proxiedPools(t, ctx)
		tables := createTempPersistentTables(t, ctx, direct)
		hooked := &hookedPool{Pool: proxied, beforeCommit: func(ctx context.Context, tx pgx.Tx) {
			proxy.Sever()
			// The ping finds the reset connection, so pgx marks it closed and
			// the COMMIT that follows is refused before any byte is written.
			if err := tx.Conn().Ping(ctx); err == nil {
				t.Error("the ping succeeded on a severed connection")
			}
		}}
		record := commitTestRecord()
		err := NewDBPersistentRecordRepository(hooked, concurrencyTestCache(t, 1)).InsertPersistentRecord(ctx, tables, record)

		requireKnownNotCommitted(t, err)
		_, found := mainText(ctx, t, direct, tables, record.RowID)
		require.False(t, found)
	})

	t.Run("context done before the COMMIT was sent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pool := testdb.Connect(t, ctx)
		tables := createTempPersistentTables(t, ctx, pool)
		writeCtx, cancelWrite := context.WithCancel(ctx)
		defer cancelWrite()
		hooked := &hookedPool{Pool: pool, beforeCommit: func(context.Context, pgx.Tx) { cancelWrite() }}
		record := commitTestRecord()
		err := NewDBPersistentRecordRepository(hooked, concurrencyTestCache(t, 1)).InsertPersistentRecord(writeCtx, tables, record)

		requireKnownNotCommitted(t, err)
		require.ErrorIs(t, err, context.Canceled)
		_, found := mainText(ctx, t, pool, tables, record.RowID)
		require.False(t, found)
	})
}

// TestCommitInFlightCancellationIsUnknown_PostgresOnly covers a context that
// ends while COMMIT is in flight: the server has committed, the reply is held
// back, and the writer's context is cancelled. pgx answers the cancellation,
// and the writer must still report unknown, with the context's cause in the
// chain.
func TestCommitInFlightCancellationIsUnknown_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proxied, proxy, direct := proxiedPools(t, ctx)
	tables := createTempPersistentTables(t, ctx, direct)
	repo := NewDBPersistentRecordRepository(proxied, concurrencyTestCache(t, 1))

	record := commitTestRecord()
	fired := proxy.Arm(faultproxy.HoldCommitReply)
	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()
	done := make(chan error, 1)
	go func() { done <- repo.InsertPersistentRecord(writeCtx, tables, record) }()
	awaitFault(t, fired)
	cancelWrite()

	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the write did not return after its context was cancelled")
	}
	requireUnknownCommit(t, err)
	require.ErrorIs(t, err, context.Canceled)
	_, found := mainText(ctx, t, direct, tables, record.RowID)
	require.True(t, found, "the server committed before the cancellation")
}
