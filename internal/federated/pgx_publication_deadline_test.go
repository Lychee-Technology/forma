package federated

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// R_max through the whole request: a pass its deadline aborted is retried on
// a fresh snapshot, is never charged to the breaker, and never becomes a
// degradable failure, not even when the retry is refused.

// openingScan blocks the parquet leg until its context ends, as blockingScan
// does, and opens the breaker once the first scan ends: another request's
// failure, landing between the pass and its retry.
type openingScan struct {
	*countingDuck
	e    *DBFederatedQueryEngine
	once sync.Once
}

func (o *openingScan) queryStaged(ctx context.Context, _ []uuid.UUID, _ string, _ ...any) (duckDBRowsIterator, error) {
	<-ctx.Done()
	o.once.Do(func() { o.e.breaker.RecordFailure() })
	return nil, ctx.Err()
}

// failingScan fails the parquet leg at once, with a read error that names no
// deadline.
type failingScan struct {
	*countingDuck
}

func (f *failingScan) queryStaged(context.Context, []uuid.UUID, string, ...any) (duckDBRowsIterator, error) {
	return nil, errors.New("IO Error: could not read the object")
}

// A pass deadline at a breaker that one failure opens, through Query under
// AllowPartialDegradedMode: the aborted pass is not charged to the breaker,
// so the retry is admitted and reads a fresh snapshot; when it too runs past
// R_max the request fails with the deadline, and nothing degrades.
func TestAPassDeadlineIsNotChargedToTheBreakerAndNeverDegrades(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	e, duck, _ := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
	e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
	rec := recordDegradedFallbacks(e)
	scan := &blockingScan{countingDuck: duck, causes: make(chan error, 2)}
	e.duck = scan

	_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, ErrPassDeadline)
	require.False(t, degradableFederatedError(err))
	require.Equal(t, 2, f.leg.begins, "the retry was admitted, and read a fresh snapshot")
	for range 2 {
		require.ErrorIs(t, <-scan.causes, ErrPassDeadline)
	}
	require.Empty(t, rec.take(), "no degraded fallback")
	require.Empty(t, rec.transitions, "no breaker transition")
	require.False(t, e.breaker.IsOpen())
}

// A retry the breaker refuses, opened by another request between the pass
// and its retry, keeps the first pass's deadline: the request fails, and
// the refusal, degradable on its own, does not turn it into a degraded
// answer. The retry still read a fresh snapshot: a published pass meets
// admission only once its generation lists objects.
func TestARetryRefusedAfterAPassDeadlineKeepsTheDeadline(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	e, duck, _ := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
	e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
	rec := recordDegradedFallbacks(e)
	e.duck = &openingScan{countingDuck: duck, e: e}

	_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, ErrPassDeadline)
	require.ErrorIs(t, err, errBreakerRejected, "the retry was refused")
	require.False(t, degradableFederatedError(err))
	require.Equal(t, 2, f.leg.begins)
	require.Equal(t, 2, pub.reads)
	require.Empty(t, rec.take(), "no degraded fallback")
}

// A read that failed for its own reason, whose missing-object probe then ran
// out of time: the expired deadline stays in the error, so the read is not
// degradable. When it is the request's deadline, the pass is not retried;
// when it is R_max, the pass is retried once, and the breaker hears of
// neither pass.
func TestAProbeThatRunsOutOfTimeKeepsTheDeadline(t *testing.T) {
	t.Run("the request's deadline", func(t *testing.T) {
		f := newPgxFixture(t, tieRunRows())
		pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
		pub.probeBlocks = true
		e, duck, _ := publishedEngine(t, f, pub)
		rec := recordDegradedFallbacks(e)
		e.duck = &failingScan{countingDuck: duck}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := e.Query(ctx, tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "missing-object probe stopped")
		require.NotErrorIs(t, err, ErrPassDeadline)
		require.False(t, degradableFederatedError(err))
		require.Equal(t, 1, f.leg.begins)
		require.Equal(t, 1, pub.probes)
		require.Empty(t, rec.take(), "no degraded fallback")
	})
	t.Run("the pass deadline", func(t *testing.T) {
		f := newPgxFixture(t, tieRunRows())
		pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
		pub.probeBlocks = true
		e, duck, _ := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
		e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
		rec := recordDegradedFallbacks(e)
		e.duck = &failingScan{countingDuck: duck}

		_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
		require.ErrorIs(t, err, ErrPassDeadline)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, degradableFederatedError(err))
		require.Equal(t, 2, f.leg.begins, "retried once")
		require.Equal(t, 2, pub.probes)
		require.Empty(t, rec.take(), "no degraded fallback")
		require.Empty(t, rec.transitions, "no breaker transition")
	})
}

// A read that failed for its own reason, whose missing-object probe found a
// listed key lost and then blocked on another object until R_max: the loss
// is confirmed, so it is the request's answer. The pass is not retried, the
// typed error keeps the missing key, nothing degrades, and the breaker hears
// of nothing.
func TestAConfirmedLossSurvivesAPassDeadlineDuringTheProbe(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	pub.missing = []string{"base.parquet"}
	pub.probeBlocks = true
	e, duck, _ := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
	e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
	rec := recordDegradedFallbacks(e)
	e.duck = &failingScan{countingDuck: duck}

	_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, ErrPublicationInconsistent)
	require.NotErrorIs(t, err, ErrPassDeadline, "a confirmed loss is not a retryable deadline")
	var typed *PublicationConsistencyError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, []string{"base.parquet"}, typed.MissingKeys)
	require.ErrorContains(t, err, "other objects could not be probed")
	require.False(t, degradableFederatedError(err))
	require.Equal(t, 1, f.leg.begins, "not retried")
	require.Equal(t, 1, pub.probes)
	require.Empty(t, rec.take(), "no degraded fallback")
	require.Empty(t, rec.transitions, "no breaker transition")
	require.False(t, e.breaker.IsOpen())
}

// ErrPassDeadline is not degradable on its own, without the context error
// beside it.
func TestThePassDeadlineIsNotDegradableOnItsOwn(t *testing.T) {
	require.False(t, degradableFederatedError(&passDeadlineError{SchemaID: 1, Deadline: time.Second, cause: errors.New("scan failed")}))
	require.False(t, degradableFederatedError(retryFailure("the published pass ran past its deadline",
		&passDeadlineError{SchemaID: 1, Deadline: time.Second, cause: errors.New("scan failed")},
		errors.Join(errBreakerRejected, ErrDuckDBUnavailable))))
}
