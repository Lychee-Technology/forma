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
// a record, a diagnosis a collection ran beside, a live displaced inventory
// (liveDisplacements), one inside the retention grace after the schema's
// lifecycle epoch moved (publication.ReadLifecycleFence), or a plan that
// deletes from S3 when the schema has nothing of its own in the bucket
// (unbound).
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
	if live := p.liveDisplacements(); len(live) > 0 {
		rep.Refused = displacementRefusal(live, r.Opts.RetentionGrace)
		return
	}
	if p.lifecycle.Inside {
		rep.Refused = fmt.Sprintf("the schema's lifecycle epoch moved at %d (a lost record's recovery, a drop or a registration), "+
			"less than the retention grace %s ago: a recovery cannot tell whether the lost record's generation lost its history row "+
			"too, and then nothing names the objects a reader pinned to it still scans; collection resumes once the grace has passed, at %d",
			p.lifecycle.MovedAt, r.Opts.RetentionGrace, p.lifecycle.MovedAt+publication.GraceMillis(r.Opts.RetentionGrace))
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
	if refused := r.unbound(p, plan); refused != "" {
		rep.Refused = refused
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

// unbound refuses a plan that deletes from S3 when no manifest of the
// schema's generation history verified in the bucket under the layout's
// prefix (#463). It is a second guard behind the storage binding
// (checkStorage), never a proof of which bucket this is: a verified
// manifest shows only that the bucket holds this database's committed
// inventory, and a copy of the bucket holds that too, so only the binding,
// compared with the endpoint the run's client uses, the bucket and the
// prefix, tells the writers' storage from a copy. What the guard adds is
// for a schema with nothing of its own in the bucket (an UNPUBLISHED
// schema, or one whose every manifest was collected): every V2 deployment
// builds the same keys for a schema id, so every key under the prefix could
// be another deployment's live object, and such a plan is kept until a
// generation of the schema publishes. It answers why, empty when the plan
// may run.
func (r *Reconciler) unbound(p *pass, plan publication.Plan) string {
	if len(p.verified) > 0 || len(plan.Objects)+len(plan.Manifests) == 0 {
		return ""
	}
	return fmt.Sprintf("no manifest of the schema's generation history is in the bucket under prefix %q (the schema has "+
		"published no generation, or every manifest it published was collected), so nothing proves the listed keys are "+
		"this deployment's rather than another's that builds the same keys for schema %d; the %d object(s) and %d "+
		"manifest(s) the plan would delete are kept until a generation of the schema publishes. Before removing any "+
		"by hand, check that the run's S3 endpoint, --s3-bucket and --data-prefix are the writers'",
		r.Layout.Prefix, p.id, len(plan.Objects), len(plan.Manifests))
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
// yet below the orphan cutoff (publication.OrphanCutoff): a publication
// staging them may still commit.
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
	before := publication.OrphanCutoff(p.horizon, r.Opts.ClockMargin)
	for _, o := range owned {
		if !named[o.Key] && o.LastModified >= before {
			n++
		}
	}
	return n
}

// collect runs the plan in the collection order: every object (step 5),
// then every expired manifest (step 6), then Forget of every expired row
// (step 7), and last the displaced inventories no longer live. A step starts only once every delete of the one before it
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
	return r.forgetDisplacements(ctx, p)
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
