// Package federated: #508 regression for the live-to-hard-delete transition.
//
// TestRecord.NoCreationStamp seeds a hard-delete tombstone's final shape for
// a fresh row id; it cannot say that an existing row was deleted. These tests
// drive the transition through HardDeleteRecord and follow it across flush
// and compaction, with the flush carrying change_log.changed_at through as
// the production exporter does, so the tombstone outranks the live version it
// deletes by the row's own version stamps rather than by the harness clock.
//
//go:build e2e

package federated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// parquetVersion is one stored version of a row in some parquet object.
type parquetVersion struct {
	createdAtNull bool
	changedAt     int64
	deletedAt     int64
}

// TestCreationStamp_LiveRowHardDeleteSurvivesFlushAndCompaction is the full
// live → flush → hard-delete → flush → compaction path. The live version
// reaches the delta first, so the tombstone must win the parquet LWW fold
// against it on version stamps alone once both are flushed.
func TestCreationStamp_LiveRowHardDeleteSurvivesFlushAndCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	target, targetRecord := newCreationStampFixture(h.SchemaID, 30)
	sibling, siblingRecord := newCreationStampFixture(h.SchemaID, 31)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{targetRecord, siblingRecord}))

	// Live versions reach the delta with both stamps intact.
	flush, err := h.RunCDCFlush(ctx)
	require.NoError(t, err, "first flush failed")
	require.True(t, flush.Flushed, "first flush should have written a delta file")
	requireRowStamps(t, h, target, "target after first flush")
	requireRowStamps(t, h, sibling, "sibling after first flush")

	// The transition removes real state: the live row has a main row and
	// attribute rows, so the post-delete zero counts are not vacuous.
	before := h.hotStateCounts(ctx, t, target.rowID)
	require.Equal(t, 1, before[0], "the target must have an entity_main row before the hard delete")
	require.Positive(t, before[1], "the target must have eav_data rows before the hard delete")

	// The fixture's version stamp is ahead of the wall clock, so the
	// tombstone must be stamped strictly after it, not at the clock.
	stamp, err := h.HardDeleteRecord(ctx, target.rowID)
	require.NoError(t, err, "hard delete failed")
	require.Equal(t, target.changedAt+1, stamp,
		"a tombstone must outrank the live version it deletes even when that version is ahead of the clock (#274)")
	require.Equal(t, [2]int{0, 0}, h.hotStateCounts(ctx, t, target.rowID),
		"a hard delete must remove the row's entity_main and eav_data state")
	require.Equal(t, []int64{stamp, stamp}, h.unflushedChangeLog(ctx, t, target.rowID),
		"the tombstone is one unflushed change_log row with changed_at = deleted_at")
	_, err = h.HardDeleteRecord(ctx, target.rowID)
	require.Error(t, err, "hard-deleting a row that is no longer live must fail, not mint a second tombstone")

	requireOnlySibling(t, h, sibling, "while the tombstone is hot")

	// The tombstone exports with a NULL creation stamp and its own version
	// stamp carried through: the delta now holds both versions of the row.
	flush, err = h.RunCDCFlush(ctx)
	require.NoError(t, err, "second flush failed")
	require.True(t, flush.Flushed, "second flush should have exported the tombstone")
	wantVersions := []parquetVersion{
		{createdAtNull: false, changedAt: target.changedAt, deletedAt: 0},
		{createdAtNull: true, changedAt: stamp, deletedAt: stamp},
	}
	require.ElementsMatch(t, wantVersions, h.parquetVersions(ctx, t, target.rowID),
		"the flushed tombstone must carry its change_log stamps and a NULL creation stamp (#173/#460/#508)")
	requireOnlySibling(t, h, sibling, "after the tombstone is flushed")

	compaction, err := h.RunCompaction(ctx)
	require.NoError(t, err, "compaction failed")
	require.Positive(t, compaction.RowsMerged, "compaction should have merged the delta")
	require.ElementsMatch(t, wantVersions, h.parquetVersions(ctx, t, target.rowID),
		"compaction must rewrite both versions unchanged")
	requireOnlySibling(t, h, sibling, "after compaction")
}

// requireRowStamps asserts one row reads back live with the fixture's stamps.
func requireRowStamps(t *testing.T, h *FederatedTestHarness, want creationStampFixture, stage string) {
	t.Helper()
	got, err := h.ExecuteFederatedQuery(context.Background(),
		&QueryOptions{Limit: 10, Filter: &Filter{RowID: want.rowID}})
	require.NoError(t, err, "%s: federated query failed", stage)
	requireStamps(t, got, want, stage)
}

// requireOnlySibling asserts the hard-deleted row is invisible and its
// sibling is the only row, with both stamps intact.
func requireOnlySibling(t *testing.T, h *FederatedTestHarness, sibling creationStampFixture, stage string) {
	t.Helper()
	got, err := h.ExecuteFederatedQuery(context.Background(), &QueryOptions{Limit: 10})
	require.NoError(t, err, "%s: federated query failed", stage)
	requireStamps(t, got, sibling, stage+": the hard-deleted row must not surface")
}

// hotStateCounts returns how many entity_main and eav_data rows the row has.
func (h *FederatedTestHarness) hotStateCounts(ctx context.Context, t *testing.T, rowID uuid.UUID) [2]int {
	t.Helper()
	var counts [2]int
	require.NoError(t, h.PGDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM entity_main WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		h.SchemaID, rowID).Scan(&counts[0]))
	require.NoError(t, h.PGDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM eav_data WHERE schema_id = $1 AND row_id = $2",
		h.SchemaID, rowID).Scan(&counts[1]))
	return counts
}

// unflushedChangeLog returns the row's unflushed change_log changed_at and
// deleted_at, failing unless exactly one such row exists.
func (h *FederatedTestHarness) unflushedChangeLog(ctx context.Context, t *testing.T, rowID uuid.UUID) []int64 {
	t.Helper()
	var changedAt int64
	var deletedAt sql.NullInt64
	require.NoError(t, h.PGDB.QueryRowContext(ctx, `
		SELECT changed_at, deleted_at FROM change_log
		WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0
	`, h.SchemaID, rowID).Scan(&changedAt, &deletedAt))
	return []int64{changedAt, deletedAt.Int64}
}

// parquetVersions reads every stored version of the row across the delta and
// base objects, so the assertion does not depend on which tier holds it.
func (h *FederatedTestHarness) parquetVersions(ctx context.Context, t *testing.T, rowID uuid.UUID) []parquetVersion {
	t.Helper()
	var out []parquetVersion
	for _, tier := range []string{"delta", "base"} {
		files, err := h.ListParquetFiles(ctx, tier)
		require.NoError(t, err)
		for _, f := range files {
			rows, err := h.Duck.DB.QueryContext(ctx, fmt.Sprintf(
				"SELECT ltbase_created_at, changed_at, deleted_at FROM read_parquet('s3://%s/%s') WHERE CAST(row_id AS VARCHAR) = '%s'",
				h.S3Bucket, f, rowID))
			require.NoError(t, err)
			for rows.Next() {
				var created sql.NullInt64
				var v parquetVersion
				require.NoError(t, rows.Scan(&created, &v.changedAt, &v.deletedAt))
				v.createdAtNull = !created.Valid
				out = append(out, v)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
		}
	}
	return out
}
