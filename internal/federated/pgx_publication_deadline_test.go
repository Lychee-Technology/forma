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
	e, duck := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
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
	e, duck := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
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
		e, duck := publishedEngine(t, f, pub)
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
		e, duck := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
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
	e, duck := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
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

// deadlineThenFault blocks the first parquet scan until R_max ends it, and
// then arms fault, the publication fault the retry meets. A later scan fails
// at once, with a read error that names no deadline, as failingScan's does.
type deadlineThenFault struct {
	*countingDuck
	fault func(*fakePublication)
	pub   *fakePublication
	scans int
}

func (d *deadlineThenFault) queryStaged(ctx context.Context, _ []uuid.UUID, _ string, _ ...any) (duckDBRowsIterator, error) {
	d.scans++
	if d.scans > 1 {
		return nil, errors.New("IO Error: could not read the object")
	}
	<-ctx.Done()
	d.fault(d.pub)
	return nil, ctx.Err()
}

// A first pass whose scan R_max ended, a failed read, and a retry that meets
// a publication fault: the fault is the request's answer. The first pass's
// failed-read classification does not reach it, so the typed error is not
// ErrFederatedReadFailed, nothing degrades, and the breaker hears of neither
// pass.
func TestAPublicationFaultTheRetryMeetsIsNotAFailedRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault func(*fakePublication)
		lost  []string
	}{
		{name: "no record", fault: func(pub *fakePublication) {
			pub.recErr = &PublicationConsistencyError{SchemaID: parquetOnlySchemaID, Epoch: 1, Generation: 4, Err: errors.New("publication record missing")}
		}},
		{name: "manifest missing or invalid", fault: func(pub *fakePublication) {
			pub.genErr = &PublicationConsistencyError{SchemaID: parquetOnlySchemaID, Epoch: 1, Generation: 3, Err: errors.New("manifest object missing")}
		}},
		{name: "a listed object missing", lost: []string{"base.parquet"}, fault: func(pub *fakePublication) {
			pub.missing = []string{"base.parquet"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
			e, duck := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
			e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
			rec := recordDegradedFallbacks(e)
			e.duck = &deadlineThenFault{countingDuck: duck, fault: tc.fault, pub: pub}

			_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.ErrorIs(t, err, ErrPublicationInconsistent)
			var typed *PublicationConsistencyError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, tc.lost, typed.MissingKeys)
			require.NotErrorIs(t, err, ErrFederatedReadFailed, "the first pass's failed read does not reclassify the fault")
			require.ErrorContains(t, err, "the first pass", "the first pass stays in the message")
			require.False(t, degradableFederatedError(err))
			require.Equal(t, 2, f.leg.begins, "the retry read a fresh snapshot")
			require.Empty(t, rec.take(), "no degraded fallback")
			require.Empty(t, rec.transitions, "no breaker transition")
			require.False(t, e.breaker.IsOpen())
		})
	}
}

// ErrPassDeadline is not degradable on its own, without the context error
// beside it.
func TestThePassDeadlineIsNotDegradableOnItsOwn(t *testing.T) {
	require.False(t, degradableFederatedError(&passDeadlineError{SchemaID: 1, Deadline: time.Second, cause: errors.New("scan failed")}))
	require.False(t, degradableFederatedError(retryFailure("the published pass ran past its deadline",
		&passDeadlineError{SchemaID: 1, Deadline: time.Second, cause: errors.New("scan failed")},
		errors.Join(errBreakerRejected, ErrDuckDBUnavailable))))
}
