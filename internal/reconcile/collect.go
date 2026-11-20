package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/publication"
)

// errPlanTouchesProtected reports a plan that would delete a key the pass
// itself found protected. PlanCollection never plans one; the check keeps a
// fault in either from reaching S3.
var errPlanTouchesProtected = errors.New("the collection plan deletes a protected key")

// summarize fills rep from p and plans the collection (steps 5 to 7) when
// nothing stops it. A blocking finding stops it, and so does a pass without
// a record, one inside the retention grace after the schema's lifecycle
// epoch moved (publication.ReadLifecycleFence), or a diagnosis a collection
// ran beside.
func (r *Reconciler) summarize(p *pass, rep *SchemaReport) {
	if p.record != nil {
		rep.State, rep.Epoch = p.record.State, p.record.Epoch
		if p.record.State == publication.Published {
			rep.Generation = p.record.Generation
		}
	}
	rep.Horizon, rep.Retained, rep.Expired, rep.Listed = p.horizon, len(p.retained), len(p.expired), len(p.listed)
	rep.Findings, rep.ConcurrentlyExpired = p.findings, p.concurrent
	for _, f := range p.findings {
		if f.Kind.blocking() {
			rep.Refused = fmt.Sprintf("%s: %s", f.Kind, f.describe())
			return
		}
	}
	if p.record == nil {
		rep.Refused = fmt.Sprintf("no usable record: %v", p.recordErr)
		return
	}
	if p.concurrent > 0 {
		rep.Refused = fmt.Sprintf("a collection ran beside the diagnosis and deleted what generations of its snapshot listed "+
			"(%d finding(s) left out), so the diagnosis's snapshot no longer plans what a collection would delete; "+
			"run the diagnosis again for the plan", p.concurrent)
		return
	}
	if p.lifecycle.Inside {
		rep.Refused = fmt.Sprintf("the schema's lifecycle epoch moved at %d (a lost record's recovery, a drop or a registration), "+
			"less than the retention grace %s ago: a recovery cannot tell whether the lost record's generation lost its history row "+
			"too, and then nothing names the objects a reader pinned to it still scans; collection resumes once the grace has passed, at %d",
			p.lifecycle.MovedAt, r.Opts.RetentionGrace, p.lifecycle.MovedAt+r.Opts.RetentionGrace.Milliseconds())
		return
	}
	var owned []publication.Object
	for _, o := range p.listed {
		if r.Layout.Owns(p.id, o.Key) {
			owned = append(owned, o)
		}
	}
	plan, err := publication.PlanCollection(publication.Collection{
		SchemaID: p.id, Horizon: p.horizon, Listed: owned, Retained: p.retained, Expired: p.expired,
		Manifests: p.raw, ClockMargin: r.Opts.ClockMargin,
	})
	if err == nil {
		err = r.guardPlan(p, plan)
	}
	if err != nil {
		rep.Refused = err.Error()
		return
	}
	plan.Forget = keepFloor(plan.Forget)
	rep.Plan = plan
	rep.PendingOrphans = r.pendingOrphans(p, owned)
}

// keepFloor keeps the highest expired generation's row out of the rows a
// pass forgets (forget is sorted ascending). Its manifest and the objects
// only it lists are still deleted; the row stays as the history's floor,
// below which every generation is past every reader (checkHistory), and is
// forgotten once a higher generation expires and takes its place.
func keepFloor(forget []int64) []int64 {
	if len(forget) == 0 {
		return forget
	}
	return forget[:len(forget)-1]
}

// guardPlan refuses a plan that deletes the record's manifest, a retained
// generation's manifest or an object a retained generation lists, or a key
// the layout does not build for the schema.
func (r *Reconciler) guardPlan(p *pass, plan publication.Plan) error {
	protected := map[string]bool{p.record.ManifestKey: p.record.ManifestKey != ""}
	for _, g := range p.retained {
		protected[g.ManifestKey] = true
		if m := p.verified[g.ManifestKey]; m != nil {
			for _, f := range m.Files {
				protected[f.Key] = true
			}
		}
	}
	for _, key := range append(append([]string{}, plan.Objects...), plan.Manifests...) {
		if protected[key] || !r.Layout.Owns(p.id, key) {
			return fmt.Errorf("schema %d: %s: %w", p.id, key, errPlanTouchesProtected)
		}
	}
	return nil
}

// pendingOrphans counts the V2 objects no generation names that are not
// yet below the horizon less the clock margin: a publication staging them
// may still commit.
func (r *Reconciler) pendingOrphans(p *pass, owned []publication.Object) int {
	named := map[string]bool{}
	for _, g := range append(append([]publication.Generation{}, p.retained...), p.expired...) {
		named[g.ManifestKey] = true
		if m := p.verified[g.ManifestKey]; m != nil {
			for _, f := range m.Files {
				named[f.Key] = true
			}
		}
	}
	n := 0
	before := p.horizon - r.Opts.ClockMargin.Milliseconds()
	for _, o := range owned {
		if !named[o.Key] && o.LastModified >= before {
			n++
		}
	}
	return n
}

// collect runs the plan in the collection order: every object (step 5),
// then every expired manifest (step 6), then Forget of every expired row
// (step 7). A step starts only once every delete of the one before it
// succeeded, so after a crash or a failed delete the next pass plans the
// rest from what is left: an expired manifest still there lists its objects
// again, and one already gone leaves its row to be forgotten.
func (r *Reconciler) collect(ctx context.Context, p *pass, plan publication.Plan, done *Collected) error {
	if err := r.deleteAll(ctx, plan.Objects, &done.Objects); err != nil {
		return fmt.Errorf("schema %d: delete the unprotected objects: %w", p.id, err)
	}
	if err := r.deleteAll(ctx, plan.Manifests, &done.Manifests); err != nil {
		return fmt.Errorf("schema %d: delete the expired manifests: %w", p.id, err)
	}
	for _, g := range plan.Forget {
		ok, err := publication.Forget(ctx, publication.DB(r.DB), r.Tables, p.id, g, r.Opts.RetentionGrace)
		if err != nil {
			return fmt.Errorf("schema %d: %w", p.id, err)
		}
		if ok {
			done.Forgotten = append(done.Forgotten, g)
		}
	}
	return nil
}

// deleteAll deletes every key, and fails once all were tried if any delete
// failed. A context that ended (the schema lock lost) stops it at once.
func (r *Reconciler) deleteAll(ctx context.Context, keys []string, done *[]string) error {
	var errs []error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("stopped after %d of %d deletes: %w", len(*done), len(keys), err)
		}
		if err := r.Objects.Delete(ctx, key); err != nil {
			errs = append(errs, err)
			continue
		}
		*done = append(*done, key)
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d of %d deletes failed: %w", len(errs), len(keys), errors.Join(errs...))
	}
	return nil
}
