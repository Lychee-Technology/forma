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
	// Displaced are the inventories replaced generations' history rows
	// named before the replacing transition wrote them back from the
	// record (Displacement), each as the generation its row then named,
	// once no reader can still scan it: the replaced generation's row is
	// no longer retained. Each is collected, its objects then its
	// manifest, but only under the horizon's fence (Plan.Deferred), and it
	// has no history row to forget: its displacement is forgotten after its
	// manifest is deleted.
	Displaced []Generation
	// Manifests maps the manifest key of each retained, expired or
	// displaced generation to the manifest object's stored bytes, exactly as S3 answered them. A
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
	// Deferred are the keys only a displaced inventory names that the pass
	// keeps: every object it lists that is not yet below OrphanCutoff, and
	// the manifest of every displaced inventory the pass does not delete,
	// because the manifest itself or an object only it lists is not yet
	// below the cutoff. A displacement whose manifest is deferred is not
	// forgotten: its verified manifest is the evidence a later pass deletes
	// them by, once the horizon fences them (R13-F1).
	Deferred []string
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
//   - A displaced inventory no reader can still scan is the evidence that
//     names its objects: once the displacement is forgotten, nothing would
//     tell them from another deployment's (R11-F1). Its manifest must verify
//     against the displacement (one that fails stops the pass), but a
//     verified manifest proves only which keys the displaced row named, not
//     that a publication can no longer commit one of them: the row was
//     written outside the protocol, and a Parquet key spells its schema,
//     epoch and tier, not the attempt that staged it, so the manifest can
//     list a fully formed entry of an object a publication of the same epoch
//     staged and may still commit (R13-F1). So an object only displaced
//     inventories list is deleted only once it is below OrphanCutoff, as an
//     orphan is, and is deferred until then; the manifest is deleted after
//     them, only once it and every object only it lists are below the
//     cutoff, and is deferred with them otherwise, so its displacement is
//     kept (Deferred) and a later pass finds them named again. It forgets no
//     history row. ManifestFiles checks the digest and the identity, not
//     that each key is the displacement's schema and epoch: the caller gives
//     only displacements whose manifests passed the reader's full check
//     (manifestv2.VerifyDisplaced), and refuses a pass with one that did not
//     (R12-F1).
//   - An object no history row names, whether as a manifest or through a
//     manifest that exists, is an orphan. It is deleted once its
//     LastModified is below OrphanCutoff: its staging began before the
//     horizon, whatever the listing's precision and the clocks' drift, so
//     its publication can never commit.
func PlanCollection(c Collection) (Plan, error) {
	protected, err := c.protected()
	if err != nil {
		return Plan{}, err
	}
	var p Plan
	manifests, listed := map[string]bool{}, map[string]bool{}
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
	displaced, err := c.displaced()
	if err != nil {
		return Plan{}, err
	}
	before := OrphanCutoff(c.Horizon, c.ClockMargin)
	deferred := c.deferDisplaced(displaced, protected, listed, before)
	for _, o := range c.Listed {
		_, isDisplaced := displaced[o.Key]
		switch {
		case protected[o.Key], deferred[o.Key]:
		case manifests[o.Key], isDisplaced:
			p.Manifests = append(p.Manifests, o.Key)
		case listed[o.Key], o.LastModified < before:
			p.Objects = append(p.Objects, o.Key)
		}
	}
	for key := range deferred {
		p.Deferred = append(p.Deferred, key)
	}
	slices.Sort(p.Objects)
	slices.Sort(p.Manifests)
	slices.Sort(p.Forget)
	slices.Sort(p.Deferred)
	return p, nil
}

// protected answers the manifest key of every retained generation and every
// key its manifest lists, each manifest verified first.
func (c Collection) protected() (map[string]bool, error) {
	protected := map[string]bool{}
	for _, g := range c.Retained {
		raw, ok := c.Manifests[g.ManifestKey]
		if !ok {
			return nil, fmt.Errorf("generation %d of epoch %d lists manifest %s: %w", g.Generation, g.Epoch, g.ManifestKey, ErrRetainedManifestMissing)
		}
		files, err := ManifestFiles(raw, c.SchemaID, g)
		if err != nil {
			return nil, fmt.Errorf("retained generation %d of epoch %d: %w", g.Generation, g.Epoch, err)
		}
		protected[g.ManifestKey] = true
		for _, key := range files {
			protected[key] = true
		}
	}
	return protected, nil
}

// displaced answers, by manifest key, the keys each present displaced
// manifest lists, each verified against its displacement first. A
// displaced manifest S3 does not have is absent: an earlier pass deleted it
// after the objects only it listed.
func (c Collection) displaced() (map[string][]string, error) {
	out := map[string][]string{}
	for _, g := range c.Displaced {
		raw, ok := c.Manifests[g.ManifestKey]
		if !ok {
			continue
		}
		files, err := ManifestFiles(raw, c.SchemaID, g)
		if err != nil {
			return nil, fmt.Errorf("displaced inventory of generation %d of epoch %d: %w", g.Generation, g.Epoch, err)
		}
		out[g.ManifestKey] = files
	}
	return out, nil
}

// deferDisplaced answers the keys only displaced inventories name that a
// pass keeps: an object they list that is not yet below the cutoff, unless
// a retained generation protects it or an expired generation's verified
// manifest lists it, and the manifest of every displaced inventory that
// lists such an object, that is itself not below the cutoff, or that the
// listing did not answer, so that the pass does not delete it. The caller
// gives no displaced inventory whose manifest a history row or the record
// names (a retained one's is protected).
func (c Collection) deferDisplaced(displaced map[string][]string, protected, listed map[string]bool, before int64) map[string]bool {
	young, seen := map[string]bool{}, map[string]bool{}
	for _, o := range c.Listed {
		seen[o.Key], young[o.Key] = true, o.LastModified >= before
	}
	deferred := map[string]bool{}
	for manifest, files := range displaced {
		if young[manifest] || !seen[manifest] {
			deferred[manifest] = true
		}
		for _, key := range files {
			if young[key] && !protected[key] && !listed[key] {
				deferred[key], deferred[manifest] = true, true
			}
		}
	}
	return deferred
}
