package internal

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Real-Postgres regressions for #554: atomic BatchUpdate is a read-modify-
// write over N rows, so only real concurrent transactions can show whether
// its merge bases are read under the locks and whether two batches that
// name the same rows in opposite order can deadlock.

// carryForwardAndSet is the service's merge in miniature: every field read
// under the lock is carried forward, then one attribute is set (replacing
// the base's value for it, if any).
func carryForwardAndSet(attrID int16, value string) model.PersistentRecordMerge {
	return func(_ context.Context, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
		if existing == nil {
			return nil, forma.NotFoundf("merge base missing")
		}
		merged := &model.PersistentRecord{
			SchemaID:  existing.SchemaID,
			RowID:     existing.RowID,
			CreatedAt: existing.CreatedAt,
			TextItems: maps.Clone(existing.TextItems),
		}
		for _, attr := range existing.OtherAttributes {
			if attr.AttrID != attrID {
				merged.OtherAttributes = append(merged.OtherAttributes, attr)
			}
		}
		if merged.TextItems == nil {
			merged.TextItems = map[string]string{}
		}
		merged.TextItems[fmt.Sprintf("text_%02d", attrID)] = value
		v := value
		merged.OtherAttributes = append(merged.OtherAttributes, model.EAVRecord{
			SchemaID: existing.SchemaID, RowID: existing.RowID, AttrID: attrID, ValueText: &v,
		})
		return merged, nil
	}
}

func batchOf(merge model.PersistentRecordMerge) model.PersistentRecordBatchMerge {
	return func(ctx context.Context, _ int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
		return merge(ctx, existing)
	}
}

func keysOf(rows ...uuid.UUID) []model.PersistentRecordKey {
	keys := make([]model.PersistentRecordKey, len(rows))
	for i, row := range rows {
		keys[i] = model.PersistentRecordKey{SchemaID: 1, RowID: row}
	}
	return keys
}

func seedRows(t *testing.T, ctx context.Context, repo *DBPersistentRecordRepository, tables model.StorageTables, rows ...uuid.UUID) {
	t.Helper()
	for _, row := range rows {
		require.NoError(t, repo.InsertPersistentRecord(ctx, tables, &model.PersistentRecord{SchemaID: 1, RowID: row}))
	}
}

func requireAllAttributes(t *testing.T, ctx context.Context, repo *DBPersistentRecordRepository, tables model.StorageTables, row uuid.UUID, count int) {
	t.Helper()
	final, err := repo.GetPersistentRecord(ctx, tables, 1, row)
	require.NoError(t, err)
	require.NotNil(t, final)
	for i := range count {
		attrID := int16(i + 1)
		want := fmt.Sprintf("value-%02d", attrID)
		require.Equal(t, want, attrValue(final, attrID),
			"attribute %d was lost: a concurrent batch replaced the whole document (#554)", attrID)
		require.Equal(t, want, final.TextItems[fmt.Sprintf("text_%02d", attrID)],
			"main column text_%02d was lost (#554)", attrID)
	}
}

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// TestBatchMergeConcurrentDisjointUpdatesAllSurvive: N atomic batches each
// update one shared row (a field nobody else touches) plus a private row.
// Before #554 every batch read its base on the pool before the transaction
// opened, so the last committer's delete-all-then-reinsert dropped the
// others' fields.
func TestBatchMergeConcurrentDisjointUpdatesAllSurvive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	const writers = 8
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, writers))

	shared := uuid.New()
	private := make([]uuid.UUID, writers)
	for i := range private {
		private[i] = uuid.New()
	}
	seedRows(t, ctx, repo, tables, append([]uuid.UUID{shared}, private...)...)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attrID := int16(i + 1)
			_, err := repo.BatchMergePersistentRecords(ctx, tables, keysOf(shared, private[i]),
				batchOf(carryForwardAndSet(attrID, fmt.Sprintf("value-%02d", attrID))))
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "batch %d failed", i)
	}
	requireAllAttributes(t, ctx, repo, tables, shared, writers)
}

// TestBatchMergeRacingSingleRowMergeNeitherLoses: half the writers are
// atomic batches, half single-row updates, all on one row. Since #553 the
// single-row path holds the per-row lock across its read; a batch that
// took no lock could still clobber it.
func TestBatchMergeRacingSingleRowMergeNeitherLoses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	const writers = 8
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, writers))

	shared := uuid.New()
	seedRows(t, ctx, repo, tables, shared)

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attrID := int16(i + 1)
			merge := carryForwardAndSet(attrID, fmt.Sprintf("value-%02d", attrID))
			if i%2 == 0 {
				_, errs[i] = repo.BatchMergePersistentRecords(ctx, tables, keysOf(shared), batchOf(merge))
				return
			}
			_, errs[i] = repo.MergePersistentRecord(ctx, tables, 1, shared, merge)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "writer %d failed", i)
	}
	requireAllAttributes(t, ctx, repo, tables, shared, writers)
}

// TestBatchMergeOppositeInputOrderBothCommit is the assertion the sorted
// lock order exists for: two batches naming the same rows in opposite input
// order must both commit, never abort with 40P01. Each round runs the pair
// concurrently; the merge pauses under its locks so an input-order lock
// pass would reliably interleave into a deadlock.
func TestBatchMergeOppositeInputOrderBothCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 2))

	rowA, rowB := uuid.New(), uuid.New()
	seedRows(t, ctx, repo, tables, rowA, rowB)

	pausing := func(attrID int16) model.PersistentRecordBatchMerge {
		inner := carryForwardAndSet(attrID, fmt.Sprintf("value-%02d", attrID))
		return func(ctx context.Context, i int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			time.Sleep(5 * time.Millisecond)
			return inner(ctx, existing)
		}
	}

	for round := range 20 {
		var wg sync.WaitGroup
		var errAB, errBA error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errAB = repo.BatchMergePersistentRecords(ctx, tables, keysOf(rowA, rowB), pausing(1))
		}()
		go func() {
			defer wg.Done()
			_, errBA = repo.BatchMergePersistentRecords(ctx, tables, keysOf(rowB, rowA), pausing(2))
		}()
		wg.Wait()
		require.NoError(t, errAB, "round %d: batch [A,B] failed (deadlock=%v)", round, isDeadlock(errAB))
		require.NoError(t, errBA, "round %d: batch [B,A] failed (deadlock=%v)", round, isDeadlock(errBA))
	}
	for _, row := range []uuid.UUID{rowA, rowB} {
		requireAllAttributes(t, ctx, repo, tables, row, 2)
	}
}

// TestBatchMergeVsBatchDeleteOppositeOrderNoDeadlock: an atomic batch update
// over [A,B] races a batch delete over [B,A]. Either the update commits
// first and the delete removes its result, or the delete commits first and
// the update's merge sees no row and answers not-found — never 40P01.
func TestBatchMergeVsBatchDeleteOppositeOrderNoDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 1))

	pausing := func(ctx context.Context, _ int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
		time.Sleep(5 * time.Millisecond)
		return carryForwardAndSet(1, "value-01")(ctx, existing)
	}

	for round := range 20 {
		rowA, rowB := uuid.New(), uuid.New()
		seedRows(t, ctx, repo, tables, rowA, rowB)

		var wg sync.WaitGroup
		var errUpdate, errDelete error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, errUpdate = repo.BatchMergePersistentRecords(ctx, tables, keysOf(rowA, rowB), pausing)
		}()
		go func() {
			defer wg.Done()
			errDelete = repo.BatchDeletePersistentRecords(ctx, tables, keysOf(rowB, rowA))
		}()
		wg.Wait()

		require.NoError(t, errDelete, "round %d: batch delete failed (deadlock=%v)", round, isDeadlock(errDelete))
		if errUpdate != nil {
			require.False(t, isDeadlock(errUpdate), "round %d: batch update deadlocked", round)
			require.ErrorIs(t, errUpdate, forma.ErrNotFound, "round %d: the only allowed update failure is a delete-first not-found", round)
		}
		for _, row := range []uuid.UUID{rowA, rowB} {
			gone, err := repo.GetPersistentRecord(ctx, tables, 1, row)
			require.NoError(t, err)
			require.Nil(t, gone, "round %d: the delete must win cleanly", round)
		}
	}
}

// TestBatchMergeSameMillisecondAnswersStoredVersion pins the echo fix: two
// batch updates under a frozen clock make the second row version
// GREATEST(now, prev + 1) = now + 1, and the answered record must carry
// that stored version, not the clock read the caller could compute itself.
func TestBatchMergeSameMillisecondAnswersStoredVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 1))
	fixed := time.Now()
	repo.withClock(func() time.Time { return fixed })

	row := uuid.New()
	seedRows(t, ctx, repo, tables, row)

	first, err := repo.BatchMergePersistentRecords(ctx, tables, keysOf(row), batchOf(carryForwardAndSet(1, "first")))
	require.NoError(t, err)
	require.Equal(t, fixed.UnixMilli()+1, first[0].UpdatedAt, "the seed already holds the clock read; the update must rank above it")

	second, err := repo.BatchMergePersistentRecords(ctx, tables, keysOf(row), batchOf(carryForwardAndSet(1, "second")))
	require.NoError(t, err)

	stored, err := repo.GetPersistentRecord(ctx, tables, 1, row)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, fixed.UnixMilli()+2, stored.UpdatedAt)
	require.Equal(t, stored.UpdatedAt, second[0].UpdatedAt,
		"the answered ltbase_updated_at must be the stored version, not the pre-write clock read (#554)")
}
