package reconcile

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/cdc"
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
