//go:build e2e

package production

import (
	"context"
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
// A failure before COMMIT publishes nothing and marks nothing: the rows stay
// dirty and the dirty set keeps hiding the old image the published
// generation still lists. A COMMIT whose answer was lost is resolved to
// committed, and the image of absence is published. Either way the next
// flush and compaction converge on absence.
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

	before := loadSchemaManifest(ctx, t, lc.env, lc.schema).Generation
	cfg := lc.env.CDC
	cfg.PublishFailpoint = crashOnceAt(string(point))
	_, err := lc.env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	lc.requireFailureOutcome(ctx, t, stage, point, before, err)
	if point == cdcv2.PointCommitAnswer {
		lc.published()
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

	before := loadSchemaManifest(ctx, t, lc.env, lc.schema).Generation
	_, err := lc.env.RunCompactionWith(ctx, lc.schema, CompactionOverrides{PublishFailpoint: crashOnceAt(string(point))})
	lc.requireFailureOutcome(ctx, t, stage, point, before, err)
	lc.check(ctx, t, stage)
	lc.recoverWithRewrite(ctx, t, stage)
}

// requireFailureOutcome requires a failure before COMMIT to fail the pass and
// leave the published generation where it was, and a lost COMMIT answer to
// resolve to the next generation without an error.
func (lc *absenceLifecycle) requireFailureOutcome(ctx context.Context, t *testing.T, stage string, point cdcv2.Point, before int64, err error) {
	t.Helper()
	after := loadSchemaManifest(ctx, t, lc.env, lc.schema).Generation
	if point == cdcv2.PointCommitAnswer {
		if err != nil || after != before+1 {
			t.Fatalf("%s: err = %v, generation %d -> %d; want the landed COMMIT resolved to generation %d", stage, err, before, after, before+1)
		}
		return
	}
	if err == nil || after != before {
		t.Fatalf("%s: err = %v, generation %d -> %d; want a failed pass that publishes nothing", stage, err, before, after)
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
