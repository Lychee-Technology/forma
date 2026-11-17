package publication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// collection is a pass over schema 100: generation 3 is current, 2 is
// retained inside its grace, and 1 expired. Each manifest lists the objects
// named after it; "shared" is listed by 1 and 2.
func collection() Collection {
	return Collection{
		Horizon:     10_000,
		ClockMargin: time.Second,
		Retained: []Generation{
			{Generation: 2, ManifestKey: "m2"},
			{Generation: 3, ManifestKey: "m3"},
		},
		Expired: []Generation{{Generation: 1, ManifestKey: "m1"}},
		Files: map[string][]string{
			"m1": {"only1", "shared"},
			"m2": {"shared", "only2"},
			"m3": {},
		},
		Listed: []Object{
			{Key: "m1", LastModified: 1}, {Key: "m2", LastModified: 1}, {Key: "m3", LastModified: 1},
			{Key: "only1", LastModified: 1}, {Key: "shared", LastModified: 1}, {Key: "only2", LastModified: 1},
			{Key: "orphan", LastModified: 8_999},
			{Key: "staged", LastModified: 9_000},
		},
	}
}

// TestPlanCollectionProtectsRetainedManifests pins the retained set: each
// retained generation's manifest object and every key it lists. The current
// manifest m3, old and listed in no files list, is kept; so is an object an
// expired generation shares with a retained one. An orphan goes only once its
// LastModified plus the clock margin is before the horizon.
func TestPlanCollectionProtectsRetainedManifests(t *testing.T) {
	plan, err := PlanCollection(collection())
	require.NoError(t, err)
	require.Equal(t, Plan{
		Objects:   []string{"only1", "orphan"},
		Manifests: []string{"m1"},
		Forget:    []int64{1},
	}, plan)
}

// TestPlanCollectionRecoversAfterDeletingAnExpiredManifest replays the pass
// after a crash between deleting m1 and forgetting generation 1. The objects
// only m1 listed were deleted in the step before, so the missing manifest does
// not stop the pass: generation 1 is forgotten, and an object that should
// have gone with it but is still there is an orphan the horizon decides on.
func TestPlanCollectionRecoversAfterDeletingAnExpiredManifest(t *testing.T) {
	c := collection()
	delete(c.Files, "m1")
	c.Listed = []Object{
		{Key: "m2", LastModified: 1}, {Key: "m3", LastModified: 1},
		{Key: "shared", LastModified: 1}, {Key: "only2", LastModified: 1},
		{Key: "leftover", LastModified: 1},
	}
	plan, err := PlanCollection(c)
	require.NoError(t, err)
	require.Equal(t, Plan{Objects: []string{"leftover"}, Forget: []int64{1}}, plan)
}

// TestPlanCollectionStopsOnAMissingRetainedManifest pins the one missing
// manifest that stops the pass: a retained generation's, without which the
// collector cannot tell what readers may still scan.
func TestPlanCollectionStopsOnAMissingRetainedManifest(t *testing.T) {
	c := collection()
	delete(c.Files, "m2")
	_, err := PlanCollection(c)
	require.ErrorIs(t, err, ErrRetainedManifestMissing)
}
