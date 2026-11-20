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
// displaced inventories of generations the pass found no longer live. Each
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
