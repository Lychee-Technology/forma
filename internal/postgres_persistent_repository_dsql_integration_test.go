package internal

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The aurora-dsql engine's write invariants (#633), on the target
// openDSQLTarget opens. Under the PostgreSQL emulation a guard-row write
// conflict is a wait and then 40001; on a cluster it is 40001 at COMMIT
// without the wait. #274, #457 and #554 hold under both, because each rests
// only on two transactions that write one guard row never both committing.

// #274 on the aurora-dsql engine: a recreate that raced a delete of the row
// can only commit if its snapshot holds the tombstone. The delete is stopped
// at its COMMIT with the row's guard written; the recreate writes the same
// guard, so it cannot commit beside the delete and fails retryable. Retried,
// it reads the tombstone and stamps above it.
func TestDSQLEngineRecreateCannotCommitBesideTheDeleteItRaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	d := openDSQLTarget(t, ctx)
	if d.cluster {
		t.Skip("the recreate's wait on the guard row is the emulation's; on a cluster the sweep below covers #274")
	}
	tables := d.createDSQLTables(t, ctx)
	mc := concurrencyTestCache(t, 1)

	frozen := time.Date(2026, 10, 16, 9, 10, 11, 0, time.UTC)
	repo := newDSQLModeRepo(t, d.pool, mc)
	repo.withClock(func() time.Time { return frozen })
	atCommit, release := make(chan struct{}), make(chan struct{})
	deleter := newDSQLModeRepo(t, &hookedPool{Pool: d.pool, beforeCommit: func(context.Context, pgx.Tx) {
		close(atCommit)
		<-release
	}}, mc)
	deleter.withClock(func() time.Time { return frozen })

	rowID := uuid.New()
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: rowID}))
	deleted := make(chan error, 1)
	go func() { deleted <- deleter.DeletePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{}) }()
	<-atCommit

	recreated := make(chan error, 1)
	go func() {
		recreated <- repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: rowID})
	}()
	d.waitForLockWaiter(ctx, t)
	close(release)
	require.NoError(t, <-deleted)

	err := <-recreated
	require.ErrorIs(t, err, forma.ErrRetryableConflict, "the recreate wrote the guard the in-flight delete wrote")
	require.True(t, forma.IsRetryable(err))

	again := &model.PersistentRecord{SchemaID: 1, RowID: rowID}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, again))
	require.Equal(t, frozen.UnixMilli()+2, again.UpdatedAt, "the retried recreate stamps above the tombstone at T+1")
	require.Equal(t, frozen.UnixMilli()+2, d.liveVersion(ctx, t, tables, rowID))
	require.EqualValues(t, 3, d.guardGeneration(ctx, t, tables, rowID),
		"create, delete and recreate each committed one guard write; the aborted recreate none")
}

// #274's sweep on the aurora-dsql engine: a delete and a recreate race on
// fresh rows, each retried on the conflicts it may answer, and the final
// live version is create, then tombstone, then recreate: T+2 under a frozen
// clock, never a tie with the tombstone or the bare clock.
func TestDSQLEngineDeleteRecreatePairsStayOrdered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := openDSQLTarget(t, ctx)
	tables := d.createDSQLTables(t, ctx)
	repo := newDSQLModeRepo(t, d.pool, concurrencyTestCache(t, 1))
	frozen := time.Date(2026, 10, 16, 9, 10, 11, 0, time.UTC)
	repo.withClock(func() time.Time { return frozen })

	for i := range 20 {
		id := uuid.New()
		require.NoError(t, repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: id}))
		errs := race(2, func(w int) error {
			if w == 0 {
				return retryConflicts(func() error { return repo.DeletePersistentRecord(ctx, tables, 1, id, model.RowGuard{}) })
			}
			return retryConflicts(func() error {
				return repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: id})
			})
		})
		require.NoError(t, errors.Join(errs...), "pair %d", i)
		require.Equal(t, frozen.UnixMilli()+2, d.liveVersion(ctx, t, tables, id),
			"pair %d: the recreate lands strictly above the concurrent tombstone", i)
	}
}

// retryConflicts runs write until it answers neither a retryable conflict
// nor, for a recreate that reached the row before the delete, its primary
// key: what a caller of the aurora-dsql engine does.
func retryConflicts(write func() error) error {
	for {
		err := write()
		if err == nil || !(forma.IsRetryable(err) || errors.Is(err, forma.ErrConflict)) {
			return err
		}
	}
}

// retryRetryable runs write until it answers anything but a retryable
// conflict, counting the retries.
func retryRetryable(retries *int, write func() error) error {
	for {
		err := write()
		if !forma.IsRetryable(err) {
			return err
		}
		*retries++
	}
}

// #457 on the aurora-dsql engine: writers that each set one attribute and
// carry every other one they read forward lose nothing. A writer whose
// snapshot predates another's commit wrote the same guard row, so it fails
// retryable, and its retry merges onto the commit it missed.
func TestDSQLEngineConcurrentDisjointUpdatesAllSurvive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := openDSQLTarget(t, ctx)
	tables := d.createDSQLTables(t, ctx)
	const writers = 8
	repo := newDSQLModeRepo(t, d.pool, concurrencyTestCache(t, writers))
	rowID := uuid.New()
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: rowID}))

	retries := make([]int, writers)
	errs := race(writers, func(i int) error {
		return retryRetryable(&retries[i], func() error {
			_, err := repo.MergePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{}, mergeOneAttribute(rowID, int16(i+1)))
			return err
		})
	})
	require.NoError(t, errors.Join(errs...))
	t.Logf("%d writers committed after %v retryable conflicts", writers, retries)

	requireAllAttributes(t, ctx, repo, tables, rowID, writers)
	require.EqualValues(t, 1+writers, d.guardGeneration(ctx, t, tables, rowID), "one committed guard write per committed write")
}

// mergeOneAttribute is the service's merge in miniature: carry every field
// read forward, then set attrID's attribute and main column.
func mergeOneAttribute(rowID uuid.UUID, attrID int16) model.PersistentRecordMerge {
	return func(_ context.Context, existing *model.PersistentRecord, _ model.TxClock) (*model.PersistentRecord, error) {
		if existing == nil {
			return nil, fmt.Errorf("merge base missing for %s", rowID)
		}
		value := fmt.Sprintf("value-%02d", attrID)
		merged := &model.PersistentRecord{SchemaID: 1, RowID: rowID, CreatedAt: existing.CreatedAt,
			TextItems: maps.Clone(existing.TextItems), OtherAttributes: append([]model.EAVRecord(nil), existing.OtherAttributes...)}
		if merged.TextItems == nil {
			merged.TextItems = map[string]string{}
		}
		merged.TextItems[fmt.Sprintf("text_%02d", attrID)] = value
		merged.OtherAttributes = append(merged.OtherAttributes, model.EAVRecord{SchemaID: 1, RowID: rowID, AttrID: attrID, ValueText: &value})
		return merged, nil
	}
}

// #554 on the aurora-dsql engine: atomic batches that name a shared row and
// a private one, in either order, lose no update, and the guard rows are
// written in one order, so no two batches deadlock in the emulation's waits.
// The rows are created and deleted in batches too.
func TestDSQLEngineBatchWritesLoseNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := openDSQLTarget(t, ctx)
	tables := d.createDSQLTables(t, ctx)
	const writers = 8
	repo := newDSQLModeRepo(t, d.pool, concurrencyTestCache(t, writers))

	shared, private := uuid.New(), make([]uuid.UUID, writers)
	seeds := []*model.PersistentRecord{{SchemaID: 1, RowID: shared}}
	for i := range private {
		private[i] = uuid.New()
		seeds = append(seeds, &model.PersistentRecord{SchemaID: 1, RowID: private[i]})
	}
	require.NoError(t, repo.BatchInsertPersistentRecords(ctx, tables, seeds))

	retries := make([]int, writers)
	errs := race(writers, func(i int) error {
		attrID := int16(i + 1)
		keys := keysOf(shared, private[i])
		if i%2 == 1 {
			keys = keysOf(private[i], shared)
		}
		return retryRetryable(&retries[i], func() error {
			_, err := repo.BatchMergePersistentRecords(ctx, tables, keys, batchOf(carryForwardAndSet(attrID, fmt.Sprintf("value-%02d", attrID))))
			if isDeadlock(err) {
				return fmt.Errorf("batch %d deadlocked: the guard rows were written out of order: %w", i, err)
			}
			return err
		})
	})
	require.NoError(t, errors.Join(errs...))
	requireAllAttributes(t, ctx, repo, tables, shared, writers)
	require.EqualValues(t, 1+writers, d.guardGeneration(ctx, t, tables, shared))

	all := append([]uuid.UUID{shared}, private...)
	require.NoError(t, repo.BatchDeletePersistentRecords(ctx, tables, keysOf(all...)))
	for _, row := range all {
		record, err := repo.GetPersistentRecord(ctx, tables, 1, row)
		require.NoError(t, err)
		require.Nil(t, record, "row %s is deleted", row)
	}
	require.EqualValues(t, 2+writers, d.guardGeneration(ctx, t, tables, shared), "the batch delete wrote the guard too")
}
