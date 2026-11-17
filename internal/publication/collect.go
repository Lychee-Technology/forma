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
	Horizon  int64
	Listed   []Object
	Retained []Generation
	Expired  []Generation
	// Files maps the manifest key of each retained or expired generation to
	// the object keys its manifest lists. A manifest key Files lacks is a
	// manifest object S3 does not have.
	Files map[string][]string
	// ClockMargin covers the drift between S3's LastModified and the
	// database clock the horizon is on.
	ClockMargin time.Duration
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

// PlanCollection answers what a pass deletes.
//
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
//     LastModified plus the clock margin is before the horizon: its staging
//     began before the horizon, so its publication can never commit.
func PlanCollection(c Collection) (Plan, error) {
	protected := map[string]bool{}
	for _, g := range c.Retained {
		files, ok := c.Files[g.ManifestKey]
		if !ok {
			return Plan{}, fmt.Errorf("generation %d of epoch %d lists manifest %s: %w", g.Generation, g.Epoch, g.ManifestKey, ErrRetainedManifestMissing)
		}
		protected[g.ManifestKey] = true
		for _, key := range files {
			protected[key] = true
		}
	}
	manifests, listed := map[string]bool{}, map[string]bool{}
	var p Plan
	for _, g := range c.Expired {
		manifests[g.ManifestKey] = true
		for _, key := range c.Files[g.ManifestKey] {
			listed[key] = true
		}
		p.Forget = append(p.Forget, g.Generation)
	}
	before := c.Horizon - c.ClockMargin.Milliseconds()
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
