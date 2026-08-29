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
