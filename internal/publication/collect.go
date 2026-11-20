package publication

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrRetainedManifestMissing reports that the manifest of a generation a
// reader may still scan is not in S3. The collector cannot tell which objects
// that generation protects, so the pass deletes nothing; the missing object is
// loss for the readers, and an operator re-initializes the schema.
var ErrRetainedManifestMissing = errors.New("retained generation's manifest missing")

// Object is one object a collector listed under the schema's prefix.
type Object struct {
	Key string
	// LastModified is S3's time for the object, epoch milliseconds.
	LastModified int64
}

// Collection is what one collector pass learned, in this order: the horizon
// RaiseHorizon committed, the objects listed under the schema's prefix, the
// retained and expired generations read in one snapshot, and their manifests.
type Collection struct {
	SchemaID int16
	Horizon  int64
	Listed   []Object
	Retained []Generation
	Expired  []Generation
	// Manifests maps the manifest key of each retained or expired generation
	// to the manifest object's stored bytes, exactly as S3 answered them. A
	// manifest key Manifests lacks is a manifest object S3 does not have.
	// PlanCollection verifies every present one (ManifestFiles) before it
	// reads a key it lists.
	Manifests map[string][]byte
	// ClockMargin covers the drift between S3's LastModified and the
	// database clock the horizon is on.
	ClockMargin time.Duration
}

// LastModifiedPrecision is how much earlier than an object's creation the
// LastModified a listing answers for it can be. S3 keeps LastModified in
// whole seconds and lists it truncated, so an object written at 12.700 lists
// as 12.000.
const LastModifiedPrecision = time.Second

// OrphanCutoff answers the LastModified, in epoch milliseconds, that an
// object no generation names must be below for its publication to be fenced
// by horizon. The object was created before its LastModified plus
// LastModifiedPrecision, its writer's staged_at is no later than that plus
// the clock margin, rounded up to the millisecond so that a margin under one
// counts in full, and a compare-and-set refuses every staged_at at or below
// the horizon.
func OrphanCutoff(horizon int64, margin time.Duration) int64 {
	ms := margin.Milliseconds()
	if margin%time.Millisecond > 0 {
		ms++
	}
	return horizon - ms - LastModifiedPrecision.Milliseconds()
}

// Plan is one pass's deletes in the order they run: every key in Objects,
// then every key in Manifests, then Forget of every generation in Forget.
// Each step starts only once the one before it has finished, so a crash at
// any point leaves a state the next pass plans from again.
type Plan struct {
	Objects   []string
	Manifests []string
	Forget    []int64
}

// PlanCollection answers what a pass deletes. It verifies every manifest it
// was given before it plans anything, so a pass it refuses deletes nothing.
//
//   - Verified: a present manifest, retained or expired, is trusted only once
//     ManifestFiles accepts it against its history row: the digest of its
//     stored bytes and its identity. One that fails stops the pass
//     (ErrManifestInvalid). A retained manifest that lied could omit a key a
//     reader still scans; an expired one could list a key no committed
//     generation lists, a staged object of a publication in flight included,
//     and a listed key is deleted without the horizon's check. Neither the
//     invalid manifest nor its row is deleted on its account.
//   - Protected: the manifest object of every retained generation, and every
//     key its manifest lists. A retained manifest that is missing stops the
//     pass (ErrRetainedManifestMissing).
//   - An expired generation's manifest is deleted in the step after the
//     objects only it listed, and its history row is forgotten after that.
//     So a missing expired manifest means a pass already deleted its
//     unprotected objects: the row is forgotten, and no object is deleted on
//     its account.
//   - An object no history row names, whether as a manifest or through a
//     manifest that exists, is an orphan. It is deleted once its
//     LastModified is below OrphanCutoff: its staging began before the
//     horizon, whatever the listing's precision and the clocks' drift, so
//     its publication can never commit.
func PlanCollection(c Collection) (Plan, error) {
	protected := map[string]bool{}
	for _, g := range c.Retained {
		raw, ok := c.Manifests[g.ManifestKey]
		if !ok {
			return Plan{}, fmt.Errorf("generation %d of epoch %d lists manifest %s: %w", g.Generation, g.Epoch, g.ManifestKey, ErrRetainedManifestMissing)
		}
		files, err := ManifestFiles(raw, c.SchemaID, g)
		if err != nil {
			return Plan{}, fmt.Errorf("retained generation %d of epoch %d: %w", g.Generation, g.Epoch, err)
		}
		protected[g.ManifestKey] = true
		for _, key := range files {
			protected[key] = true
		}
	}
	manifests, listed := map[string]bool{}, map[string]bool{}
	var p Plan
	for _, g := range c.Expired {
		if raw, ok := c.Manifests[g.ManifestKey]; ok {
			files, err := ManifestFiles(raw, c.SchemaID, g)
			if err != nil {
				return Plan{}, fmt.Errorf("expired generation %d of epoch %d: %w", g.Generation, g.Epoch, err)
			}
			for _, key := range files {
				listed[key] = true
			}
		}
		manifests[g.ManifestKey] = true
		p.Forget = append(p.Forget, g.Generation)
	}
	before := OrphanCutoff(c.Horizon, c.ClockMargin)
	for _, o := range c.Listed {
		switch {
		case protected[o.Key]:
		case manifests[o.Key]:
			p.Manifests = append(p.Manifests, o.Key)
		case listed[o.Key], o.LastModified < before:
			p.Objects = append(p.Objects, o.Key)
		}
	}
	slices.Sort(p.Objects)
	slices.Sort(p.Manifests)
	slices.Sort(p.Forget)
	return p, nil
}
