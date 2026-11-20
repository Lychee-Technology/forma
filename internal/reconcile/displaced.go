package reconcile

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/publication"
)

// liveDisplacements answers the displaced inventories (publication.
// Displacement) a reader may still scan. A displacement is the inventory a
// replaced generation's history row named when the replacing transition
// found it disagreeing with the record and wrote the row back from the
// record: one of the two was changed outside the protocol, and a reader may
// have chosen the generation under either. Its manifest is in no history
// row, so its objects would be orphans, and every object it shares with an
// expired generation would be that generation's alone. It is live while the
// replaced generation's row is retained, which it is until a collector,
// after seeing the replacement committed, retired it and the retention grace
// passed: a full grace, measured from no earlier than that commit, after
// which no reader that chose the generation is still inside its pass
// deadline. A displacement whose row the snapshot did not read is live too,
// unless the row lies at or below the history's floor, the highest expired
// generation, below which every generation is past every reader
// (checkHistory); otherwise its loss is a missing_history finding anyway.
func (p *pass) liveDisplacements() []publication.Displacement {
	var floor int64
	for _, g := range p.expired {
		floor = max(floor, g.Generation)
	}
	return slices.DeleteFunc(slices.Clone(p.displaced), func(d publication.Displacement) bool {
		expired := slices.ContainsFunc(p.expired, func(g publication.Generation) bool { return g.Generation == d.Generation })
		retained := slices.ContainsFunc(p.retained, func(g publication.Generation) bool { return g.Generation == d.Generation })
		return expired || (!retained && d.Generation <= floor)
	})
}

// retiredDisplacements answers, as the generations their rows named, the
// displaced inventories no reader can still scan (those liveDisplacements
// leaves out), one per manifest key, but for a manifest key a history row
// or the record names too: a manifest key spells its generation's token and
// the object is written once, so its bytes are the row's or the record's
// to verify and protect, and a displacement that kept the same key with
// another digest names no other inventory. A collection deletes what only they
// list and then their manifests, whatever the objects' age, as it does an
// expired generation's (publication.Collection.Displaced), and forgets them
// only after that: the displacement is the evidence that names the
// inventory, and the replaced generation's own manifest may be gone, as
// after a drop, so that nothing else proves the keys are this deployment's
// (R11-F1).
func (p *pass) retiredDisplacements() []publication.Generation {
	live := p.liveDisplacements()
	seen := map[string]bool{}
	for _, g := range append(append([]publication.Generation{}, p.retained...), p.expired...) {
		seen[g.ManifestKey] = true
	}
	if p.record != nil && p.record.ManifestKey != "" {
		seen[p.record.ManifestKey] = true
	}
	var out []publication.Generation
	for _, d := range p.displaced {
		if seen[d.ManifestKey] || slices.ContainsFunc(live, func(l publication.Displacement) bool { return l.Generation == d.Generation }) {
			continue
		}
		seen[d.ManifestKey] = true
		out = append(out, d.Inventory())
	}
	return out
}

// checkDisplaced verifies the manifest of every retired displacement
// against it (publication.ManifestFiles: digest and identity, the
// collector's check) and keeps the keys it lists. One that fails is not
// trusted, and the plan, which verifies it again, refuses the pass; one
// missing was deleted by an earlier pass after the objects only it listed.
func (r *Reconciler) checkDisplaced(p *pass) {
	p.displacedFiles = map[string][]string{}
	for _, g := range p.retiredDisplacements() {
		raw, ok := p.raw[g.ManifestKey]
		if !ok || !r.Layout.Owns(p.id, g.ManifestKey) {
			continue
		}
		if files, err := publication.ManifestFiles(raw, p.id, g); err == nil {
			p.displacedFiles[g.ManifestKey] = files
		}
	}
}

// displacementRefusal says why a pass with live displacements deletes
// nothing, and when it resumes.
func displacementRefusal(live []publication.Displacement, grace time.Duration) string {
	var named []string
	for _, d := range live {
		named = append(named, fmt.Sprintf("generation %d of epoch %d named manifest %s (displaced at %d)", d.Generation, d.Epoch, d.ManifestKey, d.DisplacedAt))
	}
	return fmt.Sprintf("a replaced generation's history row disagreed with the record when the generation was replaced, and was written "+
		"back from the record: %s. Readers that chose the generation through the record may scan either inventory, so garbage "+
		"collection deletes nothing for the schema until the replaced generation's row expires, the retention grace %s after a "+
		"pass retires it, which a pass does only after seeing the replacement committed (docs/manifest-reconcile.md, \"A history "+
		"row that disagrees with the record\")", strings.Join(named, "; "), grace)
}

// forgetDisplacements deletes, after a collection ran its plan, the
// displaced inventories of generations the pass found no longer live. It
// runs only once every delete of the plan succeeded, the displaced
// manifests and the objects only they listed included, so a pass that
// stops before then finds them named again and deletes the rest. Each
// delete checks again that the generation's row is not retained
// (publication.ForgetDisplaced), so a displacement is never forgotten while
// it is live.
func (r *Reconciler) forgetDisplacements(ctx context.Context, p *pass) error {
	live := p.liveDisplacements()
	done := map[int64]bool{}
	for _, d := range p.displaced {
		if done[d.Generation] || slices.ContainsFunc(live, func(l publication.Displacement) bool { return l.Generation == d.Generation }) {
			continue
		}
		done[d.Generation] = true
		if _, err := publication.ForgetDisplaced(ctx, publication.DB(r.DB), r.Tables, p.id, d.Generation, r.Opts.RetentionGrace); err != nil {
			return fmt.Errorf("schema %d: %w", p.id, err)
		}
	}
	return nil
}
