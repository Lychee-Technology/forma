package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/publication"
)

var (
	errPlanTouchesProtected = errors.New("the collection plan deletes a protected key")
	errPlanUnverified       = errors.New("the collection plan deletes a key not authorized by verified history or orphan cutoff")
)

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
		rep.Refused = fmt.Sprintf("generations seen retained in the diagnosis snapshot expired while it ran (%d finding(s) left out): "+
			"a collection may have deleted what they listed, or none ran and it was missing before; run diagnosis again", p.concurrent)
		return
	}
	if p.lifecycle.RecoveryActive {
		rep.Refused = fmt.Sprintf("lost-record recovery fence for epoch %d is active (recovery_retired_at=%d, grace=%s): "+
			"collection deletes nothing until the recovery is retired and the full retention grace elapses",
			p.lifecycle.RecoveryEpoch, p.lifecycle.RecoveryRetiredAt, r.Opts.RetentionGrace)
		return
	}
	var owned []publication.Object
	for _, o := range p.listed {
		if r.Layout.Owns(p.id, o.Key) {
			owned = append(owned, o)
		}
	}
	plan, err := publication.PlanCollection(publication.Collection{
		SchemaID:    p.id,
		Horizon:     p.horizon,
		Listed:      owned,
		Retained:    p.retained,
		Expired:     p.expired,
		Manifests:   p.raw,
		ClockMargin: r.Opts.ClockMargin,
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
	rep.Plan = plan
	rep.PendingOrphans = r.pendingOrphans(p, owned)
}

// unbound is a second guard behind the PostgreSQL storage binding (#463): for
// a schema with no verified manifest in the bucket (for example an UNPUBLISHED
// schema or one whose manifests were already collected), refuse to delete S3
// objects rather than risk wiping another deployment's identically named keys.
func (r *Reconciler) unbound(p *pass, plan publication.Plan) string {
	if len(p.verified) > 0 || len(plan.Objects)+len(plan.Manifests) == 0 {
		return ""
	}
	return fmt.Sprintf("no manifest of schema %d's generation history verified in the bucket under prefix %q "+
		"(schema unpublished or all manifests already collected); keeping %d planned object(s) and %d manifest(s) until a generation publishes",
		p.id, r.Layout.Prefix, len(plan.Objects), len(plan.Manifests))
}

func (r *Reconciler) guardPlan(p *pass, plan publication.Plan) error {
	protected := map[string]bool{}
	if p.record.ManifestKey != "" {
		protected[p.record.ManifestKey] = true
	}
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
	verifiedFiles, expiredManifests := map[string]bool{}, map[string]bool{}
	for _, m := range p.verified {
		for _, f := range m.Files {
			verifiedFiles[f.Key] = true
		}
	}
	for _, g := range p.expired {
		expiredManifests[g.ManifestKey] = true
	}
	young := map[string]bool{}
	before := publication.OrphanCutoff(p.horizon, r.Opts.ClockMargin)
	for _, o := range p.listed {
		young[o.Key] = o.LastModified >= before
	}
	for _, key := range plan.Objects {
		if young[key] && !verifiedFiles[key] {
			return fmt.Errorf("schema %d: %s is not below orphan cutoff and no verified history manifest lists it: %w",
				p.id, key, errPlanUnverified)
		}
	}
	for _, key := range plan.Manifests {
		if young[key] && !expiredManifests[key] {
			return fmt.Errorf("schema %d: manifest %s is not below orphan cutoff and is no expired generation's: %w",
				p.id, key, errPlanUnverified)
		}
	}
	return nil
}

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

func (r *Reconciler) collect(ctx context.Context, p *pass, plan publication.Plan, done *Collected) error {
	if err := r.deleteAll(ctx, plan.Objects, &done.Objects); err != nil {
		return fmt.Errorf("schema %d: delete unprotected objects: %w", p.id, err)
	}
	if err := r.deleteAll(ctx, plan.Manifests, &done.Manifests); err != nil {
		return fmt.Errorf("schema %d: delete expired manifests: %w", p.id, err)
	}
	for _, g := range plan.Forget {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("schema %d: stopped before forgetting generation %d: %w", p.id, g, err)
		}
		ok, err := r.forgetOne(ctx, p.id, g)
		if err != nil {
			return fmt.Errorf("schema %d: %w", p.id, err)
		}
		if ok {
			done.Forgotten = append(done.Forgotten, g)
		}
	}
	return nil
}

func (r *Reconciler) forgetOne(ctx context.Context, schemaID int16, gen int64) (bool, error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin forget transaction for generation %d: %w", gen, err)
	}
	defer func() { _ = tx.Rollback() }()
	ok, err := publication.Forget(ctx, publication.SQLTx(tx), r.Tables, schemaID, gen, r.Opts.RetentionGrace)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit forget transaction for generation %d: %w", gen, err)
	}
	return ok, nil
}

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
