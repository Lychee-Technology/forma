//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/compaction"
)

// TestUnsetAbsenceSurvivesAFailedPublication is #941's failure criterion: a
// publication that fails after the clear, at one of cdcv2's failpoints, and
// the recovery after it never bring the cleared value back. Rows clear a
// bound scalar, an EAV scalar and a list together, beside a row whose list is
// set to [] instead, after their old image was promoted into base parquet.
//
// Each case requires the failpoint to have been reached, and a failure before
// COMMIT to carry the injected fault. A flush that fails before COMMIT
// publishes nothing and marks nothing: the cleared rows stay dirty and the
// dirty set keeps hiding the old image the published generation still lists.
// A compaction that fails before COMMIT publishes nothing either, and leaves
// the clear its flush already published and marked as it was. A COMMIT whose
// answer was lost is resolved to the generation it committed: the flush's
// image of absence is published, and the compaction's merge is checked as a
// clean pass's is. Either way the next flush and compaction converge on
// absence.
func TestUnsetAbsenceSurvivesAFailedPublication(t *testing.T) {
	t.Parallel()
	for _, point := range []cdcv2.Point{cdcv2.PointPublish, cdcv2.PointMarks, cdcv2.PointCommit, cdcv2.PointCommitAnswer} {
		t.Run("flush at "+string(point), func(t *testing.T) {
			t.Parallel()
			runAbsenceFlushFailure(t, point)
		})
	}
	for _, point := range []cdcv2.Point{cdcv2.PointPublish, cdcv2.PointCommit, cdcv2.PointCommitAnswer} {
		t.Run("compaction at "+string(point), func(t *testing.T) {
			t.Parallel()
			runAbsenceCompactionFailure(t, point)
		})
	}
}

var absenceAll = absenceSubject{
	name:      "bound, eav and list",
	attrs:     []absenceAttr{absenceBound, absenceEAV, absenceList},
	emptyList: true,
}

// newPublishedAbsenceLifecycle creates the rows, publishes their old image
// in base parquet and clears it.
func newPublishedAbsenceLifecycle(ctx context.Context, t *testing.T) *absenceLifecycle {
	t.Helper()
	lc := newAbsenceLifecycle(ctx, t, NewEnv(t, SharedCluster(t)), absenceAll)
	lc.flush(ctx, t)
	lc.compact(ctx, t, absencePromote, compaction.PromotionApplied)
	lc.clear(ctx, t)
	return lc
}

func runAbsenceFlushFailure(t *testing.T, point cdcv2.Point) {
	ctx := context.Background()
	lc := newPublishedAbsenceLifecycle(ctx, t)
	stage := "flush failed at " + string(point)

	before := loadSchemaManifest(ctx, t, lc.env, lc.schema)
	fault := &absenceFault{point: point}
	cfg := lc.env.CDC
	cfg.PublishFailpoint = fault.hook
	_, err := lc.env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	lc.requireFailureOutcome(ctx, t, stage, fault, before, err)
	if point == cdcv2.PointCommitAnswer {
		lc.published()
		lc.requireDirtyRows(ctx, t, stage, 0)
	} else {
		lc.requireDirty(ctx, t, stage)
	}
	lc.check(ctx, t, stage)

	lc.flush(ctx, t)
	lc.check(ctx, t, stage+", flushed again")
	lc.recoverWithRewrite(ctx, t, stage)
}

func runAbsenceCompactionFailure(t *testing.T, point cdcv2.Point) {
	ctx := context.Background()
	lc := newPublishedAbsenceLifecycle(ctx, t)
	lc.flush(ctx, t)
	stage := "compaction failed at " + string(point)

	before := loadSchemaManifest(ctx, t, lc.env, lc.schema)
	fault := &absenceFault{point: point}
	res, err := lc.env.RunCompactionWith(ctx, lc.schema, CompactionOverrides{PublishFailpoint: fault.hook})
	lc.requireFailureOutcome(ctx, t, stage, fault, before, err)
	if point == cdcv2.PointCommitAnswer {
		lc.checkCompaction(ctx, t, before, res)
	}
	// The clear was published and marked by the flush before: a compaction
	// pass, failed or not, leaves it so.
	lc.requireDirtyRows(ctx, t, stage, 0)
	lc.check(ctx, t, stage)
	lc.recoverWithRewrite(ctx, t, stage)
}

// errAbsenceFault is the fault an absenceFault injects.
var errAbsenceFault = errors.New("injected publication fault")

// absenceFault is a publication failpoint that fails the first call at
// point with errAbsenceFault and counts every call there, so a test can
// prove the boundary it names was reached and that a failure came from it.
type absenceFault struct {
	point cdcv2.Point
	hits  atomic.Int32
}

func (f *absenceFault) hook(_ context.Context, p string) error {
	if p != string(f.point) || f.hits.Add(1) != 1 {
		return nil
	}
	return fmt.Errorf("%w at %s", errAbsenceFault, p)
}

// requireFailureOutcome requires the fault's point to have been reached, a
// fault before COMMIT to fail the pass with the injected error and leave the
// published generation where it was, and a lost COMMIT answer to resolve to
// the next generation without an error. A publication that never reached
// the point would answer as an ordinary pass does, so the hit is what makes
// the commit-answer case evidence of a resolved outcome.
func (lc *absenceLifecycle) requireFailureOutcome(ctx context.Context, t *testing.T, stage string, fault *absenceFault, before *Manifest, err error) {
	t.Helper()
	if fault.hits.Load() == 0 {
		t.Fatalf("%s: the publication never reached the failpoint (err = %v)", stage, err)
	}
	after := loadSchemaManifest(ctx, t, lc.env, lc.schema).Generation
	if fault.point == cdcv2.PointCommitAnswer {
		if err != nil || after != before.Generation+1 {
			t.Fatalf("%s: err = %v, generation %d -> %d; want the landed COMMIT resolved to generation %d",
				stage, err, before.Generation, after, before.Generation+1)
		}
		return
	}
	if !errors.Is(err, errAbsenceFault) || after != before.Generation {
		t.Fatalf("%s: err = %v, generation %d -> %d; want the injected fault to fail a pass that publishes nothing",
			stage, err, before.Generation, after)
	}
}

// recoverWithRewrite writes a newer image of every row, flushes it and
// rewrites every listed object, older base images included, into one.
func (lc *absenceLifecycle) recoverWithRewrite(ctx context.Context, t *testing.T, stage string) {
	t.Helper()
	lc.touch(ctx, t, 3)
	lc.flush(ctx, t)
	lc.check(ctx, t, stage+", touched and flushed")
	lc.compact(ctx, t, CompactionOverrides{}, compaction.RewriteApplied)
	lc.check(ctx, t, stage+", rewritten")
}
