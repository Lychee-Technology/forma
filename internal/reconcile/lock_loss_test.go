package reconcile

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// lostLocker grants the lock with a held context that has already ended
// because the lock was lost, as a DSQL lease does when renewals fail.
type lostLocker struct{ released []int16 }

func (l *lostLocker) TryLock(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
	held, cancel := context.WithCancelCause(ctx)
	cancel(fmt.Errorf("%w: schema %d's lease was not renewed within 45s", cdc.ErrSchemaLockLost, schemaID))
	return held, func() { l.released = append(l.released, schemaID) }, true, nil
}

// ctxManifests fails a Load whose context has ended, as the S3 store does,
// and records the context it was given.
type ctxManifests struct {
	*fakeManifests
	loadCtx context.Context
}

func (m *ctxManifests) Load(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	m.loadCtx = ctx
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("get manifest: %w", err)
	}
	return m.fakeManifests.Load(ctx, schemaID)
}

// TestReconcileSchema_NamesALostLock pins that reconciliation runs on the
// lock's held context, and that work cut short by a lost lease says so
// rather than reporting a bare context.Canceled (#633).
func TestReconcileSchema_NamesALostLock(t *testing.T) {
	lister := &fakeLister{}
	manifests := &ctxManifests{fakeManifests: newFakeManifests()}
	locker := &lostLocker{}
	r := newTestReconciler(lister, manifests.fakeManifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Manifests, r.Locker = manifests, locker

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Schemas) != 1 {
		t.Fatalf("expected one schema report, got %+v", report.Schemas)
	}
	s := report.Schemas[0]
	if !errors.Is(s.Err, cdc.ErrSchemaLockLost) || !errors.Is(s.Err, context.Canceled) {
		t.Fatalf("schema 7 error = %v, want ErrSchemaLockLost wrapping context.Canceled", s.Err)
	}
	if manifests.loadCtx == nil || !errors.Is(context.Cause(manifests.loadCtx), cdc.ErrSchemaLockLost) {
		t.Fatal("reconciliation must run on the lock's held context")
	}
	if len(lister.calls) != 0 {
		t.Fatalf("work after the lost lock must not run, got list calls %v", lister.calls)
	}
	if len(locker.released) != 1 || locker.released[0] != 7 {
		t.Fatalf("released = %v, want [7]", locker.released)
	}
}

// losingLocker grants the lock with a live held context, and lose ends it
// as a lost lease does, so a test can lose the lock partway through the work.
type losingLocker struct {
	lose     func()
	released int
}

func (l *losingLocker) TryLock(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
	held, cancel := context.WithCancelCause(ctx)
	l.lose = func() {
		cancel(fmt.Errorf("%w: schema %d's lease was not renewed within 45s", cdc.ErrSchemaLockLost, schemaID))
	}
	return held, func() { cancel(nil); l.released++ }, true, nil
}

// probeLosesLock loses the lock while the repair guard probes an orphan, and
// fails the probe as the cancelled S3 read does.
type probeLosesLock struct {
	*fakeStats
	locker *losingLocker
}

func (s *probeLosesLock) UncoveredRows(ctx context.Context, _ string, _ []string) ([]compaction.UncoveredRow, error) {
	s.locker.lose()
	return nil, fmt.Errorf("probe uncovered rows: %w", ctx.Err())
}

// TestReconcileSchema_NamesALockLostDuringRepair pins that a lease lost while
// --repair probes a delta orphan fails the schema. planRepair logs the
// cancelled probe and skips the orphan, so the work returns no error; were
// the loss dropped, the run would report a residual discrepancy (exit 2)
// instead of a failed run (exit 1).
func TestReconcileSchema_NamesALockLostDuringRepair(t *testing.T) {
	orphan := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: orphan, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7})
	locker := &losingLocker{}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, &probeLosesLock{fakeStats: &fakeStats{}, locker: locker})
	r.Locker = locker

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ErrorIs(t, report.Schemas[0].Err, cdc.ErrSchemaLockLost)
	require.Empty(t, manifests.saves, "an orphan the cancelled probe could not classify is not appended")
	require.Equal(t, 1, locker.released)
}

// deleteLosesLock loses the lock during a GC delete, and fails that delete
// as the cancelled S3 request does.
type deleteLosesLock struct {
	*fakeDeleter
	locker *losingLocker
}

func (d *deleteLosesLock) DeleteObject(ctx context.Context, _ string) error {
	d.locker.lose()
	return fmt.Errorf("delete object: %w", ctx.Err())
}

// TestReconcileSchema_NamesALockLostDuringGC pins the same for --gc, whose
// deletes and sighting-state save are best-effort: each logs its failure and
// goes on, so the loss is the only sign the pass was cut short.
func TestReconcileSchema_NamesALockLostDuringGC(t *testing.T) {
	old := testClock().Add(-16 * time.Minute)
	baseKey := "data/7/base-" + uuidA + ".parquet"
	listedDelta := "data/7/" + uuidC + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: listedDelta, LastModified: old}, {Key: baseKey, LastModified: old}},
	}}
	locker := &losingLocker{}
	r := gcReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}}), &fakeDeleter{})
	r.Locker = locker
	r.Deleter = &deleteLosesLock{fakeDeleter: &fakeDeleter{}, locker: locker}
	seedGCSighting(t, r, 7, old, baseKey)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ErrorIs(t, report.Schemas[0].Err, cdc.ErrSchemaLockLost)
	require.Empty(t, report.Schemas[0].Deleted)
	require.Equal(t, 1, locker.released)
}
