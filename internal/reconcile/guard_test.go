package reconcile

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists is R12-F1's last
// guard: a plan that deletes an object not yet below the orphan cutoff is
// refused unless a history manifest the pass verified with the reader's
// full check lists it, so a fault in the planner that trusted a manifest
// the pass found invalid never reaches S3. An object below the cutoff is an
// orphan and passes. A verified displaced manifest is no such proof
// (R13-F1): it shows which keys a row written outside the protocol named,
// not that no publication can still commit one, so a young object only it
// lists is refused, and so is its manifest while it is young.
func TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists(t *testing.T) {
	layout := manifestv2.Layout{Prefix: "data"}
	young, err := layout.NewParquetKey(testSchema, 3, manifestv2.TierBase)
	require.NoError(t, err)
	old, err := layout.NewParquetKey(testSchema, 2, manifestv2.TierBase)
	require.NoError(t, err)
	displaced := layout.ManifestKey(testSchema, 3, 2, uuid.New())
	expired := layout.ManifestKey(testSchema, 3, 1, uuid.New())
	r := &Reconciler{Layout: layout, Opts: Options{ClockMargin: 50 * time.Millisecond}}
	horizon := int64(10_000_000)
	cutoff := publication.OrphanCutoff(horizon, r.Opts.ClockMargin)
	p := &pass{id: testSchema, horizon: horizon, record: &publication.Snapshot{},
		listed: []publication.Object{{Key: young, LastModified: cutoff}, {Key: old, LastModified: cutoff - 1},
			{Key: displaced, LastModified: cutoff}, {Key: expired, LastModified: cutoff}},
		expired:  []publication.Generation{{Generation: 1, Epoch: 3, ManifestKey: expired}},
		verified: map[string]*manifestv2.Manifest{}, displacedFiles: map[string][]string{}}

	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old}}), "an orphan below the cutoff")
	err = r.guardPlan(p, publication.Plan{Objects: []string{old, young}})
	require.True(t, errors.Is(err, errPlanUnverified), "%v", err)
	p.displacedFiles[displaced] = []string{young}
	err = r.guardPlan(p, publication.Plan{Objects: []string{old, young}})
	require.True(t, errors.Is(err, errPlanUnverified), "a verified displaced manifest is no publication fence: %v", err)
	err = r.guardPlan(p, publication.Plan{Manifests: []string{displaced}})
	require.True(t, errors.Is(err, errPlanUnverified), "a young displaced manifest is kept too: %v", err)
	require.NoError(t, r.guardPlan(p, publication.Plan{Manifests: []string{expired}}), "an expired generation's manifest, whatever its age")
	p.verified[expired] = &manifestv2.Manifest{Files: []manifestv2.File{{Key: young}}}
	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old, young}}), "a verified history manifest lists it")
}
