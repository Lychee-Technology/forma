// Package federated: #460 regression for the harness's own creation-stamp
// contract.
//
// TestRecord carries CreatedAt (the row's creation time) separately from
// ChangedAt (the LWW version stamp). Before #460 this harness collapsed them:
// the writer emitted only changed_at, the tier queries projected only
// changed_at, and scanQueryResults assigned it to both PersistentRecord
// fields. A fixture written with CreatedAt != ChangedAt therefore read back
// with CreatedAt == ChangedAt, and the simulated flush and compaction each
// reinvented the stamp instead of preserving it.
//
// These tests pin the whole loop: direct query, flush, and compaction.
//
//go:build e2e

package federated

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// creationStampFixture is one row whose creation stamp and version stamp are
// far apart and in the OPPOSITE order to each other, so any code path that
// substitutes one for the other is caught by value rather than by luck.
type creationStampFixture struct {
	rowID     uuid.UUID
	createdAt int64
	changedAt int64
}

func newCreationStampFixture(schemaID int16, n int) (creationStampFixture, TestRecord) {
	f := creationStampFixture{
		rowID:     uuid.Must(uuid.NewV7()),
		createdAt: 1_700_000_000_000 + int64(n),
		changedAt: 1_900_000_000_000 - int64(n),
	}
	return f, TestRecord{
		RowID:      f.rowID,
		SchemaID:   schemaID,
		Attributes: map[string]any{"name": "creation-stamp", "version": n},
		CreatedAt:  f.createdAt,
		ChangedAt:  f.changedAt,
	}
}

// requireStamps asserts one queried record carries the fixture's creation
// stamp in CreatedAt and its version stamp in UpdatedAt.
func requireStamps(t *testing.T, got *QueryResult, want creationStampFixture, stage string) {
	t.Helper()
	require.Len(t, got.Records, 1, "%s: expected exactly one record", stage)
	rec := got.Records[0]
	require.Equal(t, want.rowID, rec.RowID, "%s: wrong row", stage)
	require.Equal(t, want.createdAt, rec.CreatedAt,
		"%s: created_at must be the row's creation stamp, not its version stamp (#460)", stage)
	require.Equal(t, want.changedAt, rec.UpdatedAt,
		"%s: updated_at must be the LWW version stamp (#460)", stage)
}

// TestCreationStamp_SurvivesDirectQuery is the base case: a parquet fixture
// written with CreatedAt != ChangedAt must read back with both intact. Before
// #460 the tier projections carried only changed_at and CreatedAt came back
// equal to ChangedAt.
func TestCreationStamp_SurvivesDirectQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	want, record := newCreationStampFixture(h.SchemaID, 1)
	require.NoError(t, h.WriteParquet(ctx, "delta", "creation_stamp_delta.parquet", []TestRecord{record}))

	got, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	requireStamps(t, got, want, "parquet tier")
}

// TestCreationStamp_SurvivesHotTier pins the hot leg of the same union: it
// sources the stamp from entity_main rather than aliasing change_log's
// changed_at, so a hot row reports the same quantity a parquet row does.
func TestCreationStamp_SurvivesHotTier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	want, record := newCreationStampFixture(h.SchemaID, 2)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{record}))

	got, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	requireStamps(t, got, want, "hot tier")
}

// TestCreationStamp_SurvivesFlushAndCompaction is the lifecycle case: a flush
// writes a NEW version of an EXISTING row and compaction rewrites it, so
// neither may invent a creation stamp. Before #460 RunCDCFlush stamped the
// flush instant and readDeltaFiles dropped the column, so a row's reported
// created_at moved on every tier transition.
func TestCreationStamp_SurvivesFlushAndCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	want, record := newCreationStampFixture(h.SchemaID, 3)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{record}))

	hot, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	requireStamps(t, hot, want, "hot tier")

	// The flush mints its own version stamp, so only the creation stamp is
	// expected to survive verbatim.
	flush, err := h.RunCDCFlush(ctx)
	require.NoError(t, err, "flush failed")
	require.True(t, flush.Flushed, "flush should have written a delta file")

	flushed, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	require.Len(t, flushed.Records, 1)
	require.Equal(t, want.createdAt, flushed.Records[0].CreatedAt,
		"a flush writes a new version of an existing row; it must not move the row's creation time (#460)")

	compaction, err := h.RunCompaction(ctx)
	require.NoError(t, err, "compaction failed")
	require.Positive(t, compaction.RowsMerged, "compaction should have merged the delta")

	compacted, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	require.Len(t, compacted.Records, 1)
	require.Equal(t, want.createdAt, compacted.Records[0].CreatedAt,
		"compaction rewrites a row in place; it must not move the row's creation time (#460)")
}

// TestCreationStamp_HardDeleteTombstoneSurvivesFlushAndCompaction is the
// production-shaped tombstone case (#460). A hard-deleted row has NO
// entity_main record, so the delta export's LEFT JOIN (#173) writes a NULL
// ltbase_created_at — the one legitimate absence, which the production scan
// guard permits by predicating its presence check on deleted_at.
//
// The harness lifecycle must be able to carry that shape end to end. Before
// this, readDeltaFiles scanned the column into a bare int64 and compaction
// failed outright on a real exported tombstone with "converting NULL to int64
// is unsupported"; the flush also back-filled a stamp, so the harness could
// not even produce the shape to fail on.
func TestCreationStamp_HardDeleteTombstoneSurvivesFlushAndCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	// A live row keeps the delta non-degenerate: compaction must handle a
	// mixed batch, and its stamp proves the tombstone's NULL is not simply
	// swallowing every value.
	live, liveRecord := newCreationStampFixture(h.SchemaID, 10)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{liveRecord}))

	// The hard-delete tombstone: change_log only, no entity_main row.
	tombstoneID := uuid.Must(uuid.NewV7())
	deletedAt := int64(1_900_000_000_500)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:           tombstoneID,
		SchemaID:        h.SchemaID,
		NoCreationStamp: true,
		ChangedAt:       deletedAt,
		DeletedAt:       deletedAt,
	}}))

	flush, err := h.RunCDCFlush(ctx)
	require.NoError(t, err, "flush failed")
	require.True(t, flush.Flushed, "flush should have written a delta file")

	// The exported tombstone must carry a NULL creation stamp, exactly as the
	// production exporter writes it.
	require.Equal(t, []bool{true}, h.tombstoneCreationStampIsNull(ctx, t, tombstoneID),
		"a hard-delete tombstone must export with a NULL ltbase_created_at (#173/#460)")

	// Compaction reads that object back. Before the nullable representation
	// this call failed on the NULL, so the assertion is that it works at all.
	compaction, err := h.RunCompaction(ctx)
	require.NoError(t, err,
		"compaction must be able to read a production-shaped hard-delete tombstone")
	require.Positive(t, compaction.RowsMerged, "compaction should have merged the delta")

	// And the rewritten base file must still carry the NULL, not a
	// back-filled version stamp.
	require.Equal(t, []bool{true}, h.tombstoneCreationStampIsNull(ctx, t, tombstoneID),
		"compaction must rewrite the tombstone's creation stamp as NULL, not back-fill it")

	// The live row survives with its creation stamp intact and the tombstone
	// stays invisible to queries. Only the creation stamp is asserted: the
	// simulated flush mints a fresh version stamp by design, exactly as in
	// TestCreationStamp_SurvivesFlushAndCompaction.
	got, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")
	require.Len(t, got.Records, 1, "the hard-deleted row must not surface")
	require.Equal(t, live.rowID, got.Records[0].RowID)
	require.Equal(t, live.createdAt, got.Records[0].CreatedAt,
		"the live row's creation stamp must be untouched by a sibling tombstone's flush and compaction (#460)")
}

// tombstoneCreationStampIsNull reports, per parquet object holding the row,
// whether its ltbase_created_at is NULL. It reads every object the harness has
// written so the assertion does not depend on which tier currently holds it.
func (h *FederatedTestHarness) tombstoneCreationStampIsNull(ctx context.Context, t *testing.T, rowID uuid.UUID) []bool {
	t.Helper()
	var out []bool
	for _, tier := range []string{"delta", "base"} {
		files, err := h.ListParquetFiles(ctx, tier)
		require.NoError(t, err)
		for _, f := range files {
			rows, err := h.Duck.DB.QueryContext(ctx, fmt.Sprintf(
				"SELECT ltbase_created_at FROM read_parquet('s3://%s/%s') WHERE CAST(row_id AS VARCHAR) = '%s'",
				h.S3Bucket, f, rowID))
			require.NoError(t, err)
			for rows.Next() {
				var v sql.NullInt64
				require.NoError(t, rows.Scan(&v))
				out = append(out, !v.Valid)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
		}
	}
	return out
}

// TestCompactionFailsClosedOnUnreadableDelta makes the #460 fail-closed
// behaviour durable. readDeltaFiles used to swallow a QueryContext error and
// return nil, silently dropping the whole delta object — so compaction would
// write a base file missing those rows and report success. Adding
// ltbase_created_at to that SELECT made a schema mismatch a reachable failure
// on exactly that line, so the swallow became a data-loss channel and now
// returns a wrapped error instead.
//
// Without this test the behaviour change is invisible and a future refactor
// could restore the silent skip without anything going red.
func TestCompactionFailsClosedOnUnreadableDelta(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}
	ctx := context.Background()
	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)
	require.NoError(t, h.ClearAllData(ctx))

	// A healthy delta so compaction has real work and the failure cannot be
	// mistaken for an empty-input no-op.
	_, healthy := newCreationStampFixture(h.SchemaID, 20)
	require.NoError(t, h.WriteParquet(ctx, "delta", "healthy_delta.parquet", []TestRecord{healthy}))

	// A delta object whose columns are nothing like the export schema.
	local := filepath.Join(h.tmpDir, "wrong_schema.parquet")
	_, err = h.Duck.DB.ExecContext(ctx, fmt.Sprintf(
		"COPY (SELECT 1 AS wrong_col, 'x' AS other_col) TO '%s' (FORMAT PARQUET)", local))
	require.NoError(t, err)
	require.NoError(t, h.uploadToS3(ctx,
		local, fmt.Sprintf("%s/%d/delta/wrong_schema.parquet", h.S3Prefix, h.SchemaID)))

	_, err = h.RunCompaction(ctx)
	require.Error(t, err,
		"compaction must fail closed on an unreadable delta object, not silently drop its rows (#460)")
	require.Contains(t, err.Error(), "read delta parquet",
		"the failure must name the operation and the object, per the error-wrapping contract")
}
