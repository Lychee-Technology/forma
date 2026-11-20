package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// collection is a pass over schema 100: generation 3 is current, 2 is
// retained inside its grace, and 1 expired. Each manifest lists the objects
// named after it; "shared" is listed by 1 and 2. "staged" is an object of a
// publication still in flight: staged after the horizon, listed by no
// committed manifest.
func collection(t *testing.T) Collection {
	c := Collection{SchemaID: 100, Horizon: 10_000, ClockMargin: time.Second, Manifests: map[string][]byte{}}
	add := func(generation int64, key string, files ...string) Generation {
		g := Generation{Generation: generation, Epoch: 1, ManifestKey: key, Token: uuid.New()}
		raw := encode(t, manifestFields(c.SchemaID, g, files...))
		g.ManifestSHA256 = digest(raw)
		c.Manifests[key] = raw
		return g
	}
	c.Retained = []Generation{add(2, "m2", "shared", "only2"), add(3, "m3")}
	c.Expired = []Generation{add(1, "m1", "only1", "shared")}
	c.Listed = []Object{
		{Key: "m1", LastModified: 1}, {Key: "m2", LastModified: 1}, {Key: "m3", LastModified: 1},
		{Key: "only1", LastModified: 1}, {Key: "shared", LastModified: 1}, {Key: "only2", LastModified: 1},
		// Horizon=10_000, ClockMargin=1s, LastModifiedPrecision=1s => cutoff=8_000.
		{Key: "orphan", LastModified: 7_999},
		{Key: "staged_at_cutoff", LastModified: 8_000},
		{Key: "staged", LastModified: 8_999},
	}
	return c
}

// manifestFields is generation g's manifest, listing keys, as a map a case
// can change before it is encoded.
func manifestFields(schemaID int16, g Generation, keys ...string) map[string]any {
	files := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		files = append(files, map[string]any{"tier": "delta", "key": k})
	}
	return map[string]any{
		"format": ManifestFormat, "schema_id": schemaID, "epoch": g.Epoch, "generation": g.Generation,
		"parent_generation": g.Generation - 1, "token": g.Token.String(), "kind": "flush", "files": files,
	}
}

func encode(t *testing.T, m map[string]any) []byte {
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	return raw
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// TestPlanCollectionProtectsRetainedManifests pins the retained set: each
// retained generation's manifest object and every key it lists. The current
// manifest m3, old and listed in no files list, is kept; so is an object an
// expired generation shares with a retained one. An orphan goes only once its
// LastModified plus the clock margin is before the horizon.
func TestPlanCollectionProtectsRetainedManifests(t *testing.T) {
	plan, err := PlanCollection(collection(t))
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
	c := collection(t)
	delete(c.Manifests, "m1")
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
	c := collection(t)
	delete(c.Manifests, "m2")
	_, err := PlanCollection(c)
	require.ErrorIs(t, err, ErrRetainedManifestMissing)
}

// TestPlanCollectionStopsOnAnInvalidManifest pins verification before trust,
// for the retained manifest m2 and the expired manifest m1 alike. Each case
// stores bytes under the manifest key that parse, or nearly do, but are not
// the manifest the history row committed. Trusted, the m2 cases would leave
// only2 unprotected and delete a retained generation's data, and the m1 cases
// would delete "staged" without the horizon's check. Every one stops the pass
// with an empty plan: nothing is deleted and no row is forgotten.
func TestPlanCollectionStopsOnAnInvalidManifest(t *testing.T) {
	// seal stores m under the target's key and commits its digest to the
	// row, as a writer that sealed the wrong manifest would have: only the
	// identity check can refuse it.
	seal := func(t *testing.T, c *Collection, g *Generation, m map[string]any) {
		raw := encode(t, m)
		c.Manifests[g.ManifestKey], g.ManifestSHA256 = raw, digest(raw)
	}
	cases := map[string]func(t *testing.T, c *Collection, g *Generation){
		"stored bytes differ from the row's digest": func(t *testing.T, c *Collection, g *Generation) {
			c.Manifests[g.ManifestKey] = encode(t, manifestFields(c.SchemaID, *g, "shared", "staged"))
		},
		"row's digest is not lowercase hex": func(_ *testing.T, _ *Collection, g *Generation) {
			g.ManifestSHA256 = "SHA256:" + g.ManifestSHA256
		},
		"another generation's manifest": func(t *testing.T, c *Collection, g *Generation) {
			other := *g
			other.Generation = 7
			seal(t, c, g, manifestFields(c.SchemaID, other, "shared", "staged"))
		},
		"another token": func(t *testing.T, c *Collection, g *Generation) {
			other := *g
			other.Token = uuid.New()
			seal(t, c, g, manifestFields(c.SchemaID, other, "shared", "staged"))
		},
		"another epoch": func(t *testing.T, c *Collection, g *Generation) {
			other := *g
			other.Epoch = 2
			seal(t, c, g, manifestFields(c.SchemaID, other, "shared", "staged"))
		},
		"another schema": func(t *testing.T, c *Collection, g *Generation) {
			seal(t, c, g, manifestFields(c.SchemaID+1, *g, "shared", "staged"))
		},
		"no token": func(t *testing.T, c *Collection, g *Generation) {
			m := manifestFields(c.SchemaID, *g, "shared", "staged")
			delete(m, "token")
			seal(t, c, g, m)
		},
		"unknown format": func(t *testing.T, c *Collection, g *Generation) {
			m := manifestFields(c.SchemaID, *g, "shared", "staged")
			m["format"] = "forma-manifest/1"
			seal(t, c, g, m)
		},
		"no files array": func(t *testing.T, c *Collection, g *Generation) {
			m := manifestFields(c.SchemaID, *g)
			delete(m, "files")
			seal(t, c, g, m)
		},
		"a file without a key": func(t *testing.T, c *Collection, g *Generation) {
			m := manifestFields(c.SchemaID, *g, "shared")
			m["files"] = append(m["files"].([]map[string]any), map[string]any{"tier": "delta"})
			seal(t, c, g, m)
		},
		"not JSON": func(_ *testing.T, c *Collection, g *Generation) {
			raw := []byte(`{"format": "forma-manifest/2", "files": [`)
			c.Manifests[g.ManifestKey], g.ManifestSHA256 = raw, digest(raw)
		},
		"trailing data": func(_ *testing.T, c *Collection, g *Generation) {
			raw := append(append([]byte{}, c.Manifests[g.ManifestKey]...), `{"files": [{"key": "staged"}]}`...)
			c.Manifests[g.ManifestKey], g.ManifestSHA256 = raw, digest(raw)
		},
		// Decoder.More answers false at a closing delimiter, so these two
		// pin that verification decodes to EOF rather than asking More.
		"trailing closing brace": func(_ *testing.T, c *Collection, g *Generation) {
			raw := append(append([]byte{}, c.Manifests[g.ManifestKey]...), "\n}"...)
			c.Manifests[g.ManifestKey], g.ManifestSHA256 = raw, digest(raw)
		},
		"trailing closing bracket": func(_ *testing.T, c *Collection, g *Generation) {
			raw := append(append([]byte{}, c.Manifests[g.ManifestKey]...), "]"...)
			c.Manifests[g.ManifestKey], g.ManifestSHA256 = raw, digest(raw)
		},
	}
	targets := map[string]func(c *Collection) *Generation{
		"retained": func(c *Collection) *Generation { return &c.Retained[0] },
		"expired":  func(c *Collection) *Generation { return &c.Expired[0] },
	}
	for target, pick := range targets {
		for name, corrupt := range cases {
			t.Run(target+"/"+name, func(t *testing.T) {
				c := collection(t)
				corrupt(t, &c, pick(&c))
				plan, err := PlanCollection(c)
				require.ErrorIs(t, err, ErrManifestInvalid)
				require.Equal(t, Plan{}, plan, "a pass that meets an invalid manifest deletes nothing")
			})
		}
	}
}

func TestOrphanCutoffPrecisionAndOverflow(t *testing.T) {
	require.Equal(t, int64(8_000), OrphanCutoff(10_000, time.Second))
	require.Equal(t, int64(7_999), OrphanCutoff(10_000, time.Second+time.Nanosecond), "sub-ms margin rounds up")
	require.Equal(t, int64(8_999), OrphanCutoff(10_000, time.Nanosecond), "1ns margin rounds up to 1ms plus 1s precision")
	require.Equal(t, int64(math.MinInt64), OrphanCutoff(10_000, -time.Second))
	require.Equal(t, int64(math.MinInt64), OrphanCutoff(math.MinInt64+500, time.Second))
}

func TestPlanCollectionRejectsNegativeMarginAndKeepsUnknownAge(t *testing.T) {
	c := collection(t)
	c.ClockMargin = -time.Millisecond
	_, err := PlanCollection(c)
	require.Error(t, err)

	c = collection(t)
	c.Listed = append(c.Listed,
		Object{Key: "zero_ts", LastModified: 0},
		Object{Key: "neg_ts", LastModified: -62135596800000},
	)
	plan, err := PlanCollection(c)
	require.NoError(t, err)
	require.Equal(t, []string{"only1", "orphan"}, plan.Objects, "objects with missing or non-positive LastModified are never deleted as orphans")
}
