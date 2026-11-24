package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists verifies the last-line
// guard before S3 deletion: a young key not yet below the orphan cutoff must
// be proven by a verified expired history manifest.
func TestGuardPlanRefusesAYoungKeyNoVerifiedManifestLists(t *testing.T) {
	layout := manifestv2.Layout{Prefix: "data"}
	young, err := layout.NewParquetKey(testSchema, 3, manifestv2.TierBase)
	require.NoError(t, err)
	old, err := layout.NewParquetKey(testSchema, 2, manifestv2.TierBase)
	require.NoError(t, err)
	otherManifest := layout.ManifestKey(testSchema, 3, 2, uuid.New())
	expired := layout.ManifestKey(testSchema, 3, 1, uuid.New())
	r := &Reconciler{Layout: layout, Opts: Options{ClockMargin: 50 * time.Millisecond}}
	horizon := int64(10_000_000)
	cutoff := publication.OrphanCutoff(horizon, r.Opts.ClockMargin)
	p := &pass{id: testSchema, horizon: horizon, record: &publication.Snapshot{},
		listed: []publication.Object{{Key: young, LastModified: cutoff}, {Key: old, LastModified: cutoff - 1},
			{Key: otherManifest, LastModified: cutoff}, {Key: expired, LastModified: cutoff}},
		expired:  []publication.Generation{{Generation: 1, Epoch: 3, ManifestKey: expired}},
		verified: map[string]*manifestv2.Manifest{}}

	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old}}), "an orphan below the cutoff")
	err = r.guardPlan(p, publication.Plan{Objects: []string{old, young}})
	require.ErrorIs(t, err, errPlanUnverified)
	err = r.guardPlan(p, publication.Plan{Manifests: []string{otherManifest}})
	require.ErrorIs(t, err, errPlanUnverified, "a young non-expired manifest is refused")
	require.NoError(t, r.guardPlan(p, publication.Plan{Manifests: []string{expired}}), "an expired generation's manifest, whatever its age")
	p.verified[expired] = &manifestv2.Manifest{Files: []manifestv2.File{{Key: young}}}
	require.NoError(t, r.guardPlan(p, publication.Plan{Objects: []string{old, young}}), "a verified history manifest lists it")
}

// TestHashedGonePreservesConfirmingHeadError verifies R15-F2: when Hash
// reports ErrObjectNotFound and the confirming Exists (HEAD) probe fails with
// an error (such as ErrRedirectRefused or a 503), hashedGone returns the
// confirming HEAD error instead of returning the stale NotFound error.
func TestHashedGonePreservesConfirmingHeadError(t *testing.T) {
	ctx := context.Background()
	wrappedNotFound := manifestv2.ErrObjectNotFound
	entries := []listing{{g: publication.Generation{Epoch: 1, Generation: 3}}}

	t.Run("HEAD confirms absent", func(t *testing.T) {
		p := &pass{id: testSchema}
		r := &Reconciler{Objects: stubHeadStore{exists: false}}
		gone, err := r.hashedGone(ctx, p, "data/7/k", entries, 3, wrappedNotFound)
		require.NoError(t, err)
		require.True(t, gone)
		require.Equal(t, []FindingKind{LostObject}, kinds(p.findings))
	})
	t.Run("HEAD shows present", func(t *testing.T) {
		p := &pass{id: testSchema}
		r := &Reconciler{Objects: stubHeadStore{exists: true}}
		gone, err := r.hashedGone(ctx, p, "data/7/k", entries, 3, wrappedNotFound)
		require.False(t, gone)
		require.ErrorContains(t, err, "HEAD found it present")
		require.Empty(t, p.findings)
	})
	t.Run("HEAD fails with redirect refusal", func(t *testing.T) {
		p := &pass{id: testSchema}
		r := &Reconciler{Objects: stubHeadStore{err: ErrRedirectRefused}}
		gone, err := r.hashedGone(ctx, p, "data/7/k", entries, 3, wrappedNotFound)
		require.False(t, gone)
		require.ErrorIs(t, err, ErrRedirectRefused)
		require.NotErrorIs(t, err, manifestv2.ErrObjectNotFound)
		require.Empty(t, p.findings)
	})
	t.Run("non-NotFound hash error propagates directly", func(t *testing.T) {
		p := &pass{id: testSchema}
		r := &Reconciler{Objects: stubHeadStore{exists: false}}
		gone, err := r.hashedGone(ctx, p, "data/7/k", entries, 3, ErrRedirectRefused)
		require.False(t, gone)
		require.ErrorIs(t, err, ErrRedirectRefused)
		require.Empty(t, p.findings)
	})
}

type stubHeadStore struct {
	ObjectStore
	exists bool
	err    error
}

func (s stubHeadStore) Exists(context.Context, string) (bool, error) {
	return s.exists, s.err
}
