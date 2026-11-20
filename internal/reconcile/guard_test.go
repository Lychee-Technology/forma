package reconcile

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists is R12-F1's last
// guard: a plan that deletes an object not yet below the orphan cutoff is
// refused unless a manifest the pass verified with the reader's full check
// lists it, so a fault in the planner that trusted a manifest the pass found
// invalid never reaches S3. An object below the cutoff is an orphan and
// passes, as one a verified displaced manifest lists does.
func TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists(t *testing.T) {
	layout := manifestv2.Layout{Prefix: "data"}
	young, err := layout.NewParquetKey(testSchema, 3, manifestv2.TierBase)
	require.NoError(t, err)
	old, err := layout.NewParquetKey(testSchema, 2, manifestv2.TierBase)
	require.NoError(t, err)
	r := &Reconciler{Layout: layout, Opts: Options{ClockMargin: 50 * time.Millisecond}}
	horizon := int64(10_000_000)
	cutoff := publication.OrphanCutoff(horizon, r.Opts.ClockMargin)
	p := &pass{id: testSchema, horizon: horizon, record: &publication.Snapshot{},
		listed:   []publication.Object{{Key: young, LastModified: cutoff}, {Key: old, LastModified: cutoff - 1}},
		verified: map[string]*manifestv2.Manifest{}, displacedFiles: map[string][]string{}}

	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old}}), "an orphan below the cutoff")
	err = r.guardPlan(p, publication.Plan{Objects: []string{old, young}})
	require.True(t, errors.Is(err, errPlanUnverified), "%v", err)
	p.displacedFiles["data/7/manifests/e2/g00000000000000000002-x.json"] = []string{young}
	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old, young}}), "a verified displaced manifest lists it")
}
