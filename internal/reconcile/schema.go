package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// pass is what one schema's pass read, in the collection order.
type pass struct {
	id int16
	// horizon is the fence the plan is made against: the one RaiseHorizon
	// committed under --gc, the snapshot record's otherwise.
	horizon int64
	listed  []publication.Object
	// record is nil when the snapshot read no usable record; recordErr
	// then says why.
	record    *publication.Snapshot
	recordErr error
	retained  []publication.Generation
	expired   []publication.Generation
	// raw holds the stored bytes of every manifest the store answered, by
	// key; a retained or expired manifest key it lacks is missing.
	raw map[string][]byte
	// verified holds every present manifest that passed the reader's full
	// check against its history row.
	verified map[string]*manifestv2.Manifest
	findings []Finding
}

// schema runs one schema's pass on ctx, which holds the schema's lock.
func (r *Reconciler) schema(ctx context.Context, id int16) SchemaReport {
	rep := SchemaReport{SchemaID: id}
	if r.Opts.GC {
		rep.Collected = &Collected{}
	}
	p, err := r.read(ctx, id)
	if err != nil {
		rep.Err = err
		return rep
	}
	if r.Opts.GC && p.record != nil {
		retired, err := r.retire(ctx, p)
		rep.Collected.Retired = retired
		if err != nil {
			rep.Err = err
			return rep
		}
	}
	if err := r.fetchManifests(ctx, p); err != nil {
		rep.Err = fmt.Errorf("schema %d: %w", id, err)
		return rep
	}
	if err := r.inspect(ctx, p); err != nil {
		rep.Err = err
		return rep
	}
	r.summarize(p, &rep)
	if !r.Opts.GC || rep.Refused != "" {
		return rep
	}
	if err := r.collect(ctx, p, rep.Plan, rep.Collected); err != nil {
		rep.Err = err
	}
	return rep
}

// read runs steps 1 to 3 of the collection order but the retirement: the
// horizon under --gc, the listing, and the snapshot.
func (r *Reconciler) read(ctx context.Context, id int16) (*pass, error) {
	p := &pass{id: id}
	if r.Opts.GC {
		h, err := publication.RaiseHorizon(ctx, publication.DB(r.DB), r.Tables, id, r.Opts.OrphanAge)
		switch {
		case errors.Is(err, publication.ErrNoRecord):
			// The snapshot reads the absence again and reports it; without
			// a horizon nothing unreferenced is deleted.
		case err != nil:
			return nil, fmt.Errorf("schema %d: %w", id, err)
		default:
			p.horizon = h
		}
	}
	listed, err := r.Objects.List(ctx, r.Layout.SchemaPrefix(id))
	if err != nil {
		return nil, fmt.Errorf("schema %d: list its objects under %s: %w", id, r.Layout.SchemaPrefix(id), err)
	}
	p.listed = listed
	if err := r.snapshot(ctx, p); err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	if p.record != nil && !r.Opts.GC {
		p.horizon = p.record.Horizon
	}
	return p, nil
}

// snapshot reads the record and the retained and expired generations in one
// read-only REPEATABLE READ snapshot, and ends it.
func (r *Reconciler) snapshot(ctx context.Context, p *pass) error {
	tx, err := r.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin the collector's snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := publication.SQLTx(tx)
	s, err := publication.Read(ctx, q, r.Tables, p.id)
	switch {
	case errors.Is(err, publication.ErrNoRecord):
		p.recordErr = err
		p.findings = append(p.findings, recordFinding(NoRecord, err))
	case errors.Is(err, publication.ErrRecordInvalid):
		p.recordErr = err
		p.findings = append(p.findings, recordFinding(RecordInvalid, err))
	case err != nil:
		return fmt.Errorf("read the record in the collector's snapshot: %w", err)
	default:
		p.record = &s
	}
	if p.retained, err = publication.Retained(ctx, q, r.Tables, p.id, r.Opts.RetentionGrace); err != nil {
		return fmt.Errorf("read the retained generations in the collector's snapshot: %w", err)
	}
	if p.expired, err = publication.Expired(ctx, q, r.Tables, p.id, r.Opts.RetentionGrace); err != nil {
		return fmt.Errorf("read the expired generations in the collector's snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("end the collector's snapshot: %w", err)
	}
	return nil
}

// retire stamps retired_at on every retained generation the snapshot saw
// superseded and not retired yet (step 3). The snapshot has ended, so each
// stamp is no earlier than the commit that replaced the generation. Those
// generations stay retained in this pass and expire in a later one.
func (r *Reconciler) retire(ctx context.Context, p *pass) ([]int64, error) {
	var retired []int64
	for _, g := range p.retained {
		if g.SupersededAt == 0 || g.RetiredAt != 0 {
			continue
		}
		ok, err := publication.Retire(ctx, publication.DB(r.DB), r.Tables, p.id, g.Generation)
		if err != nil {
			return retired, fmt.Errorf("schema %d: %w", p.id, err)
		}
		if ok {
			retired = append(retired, g.Generation)
		}
	}
	return retired, nil
}

// fetchManifests reads the manifest of every retained and expired
// generation (step 4's reads). Only a confirmed not-found counts as missing: a refused or
// failed read fails the pass, which then deletes nothing.
func (r *Reconciler) fetchManifests(ctx context.Context, p *pass) error {
	p.raw = map[string][]byte{}
	for _, g := range append(append([]publication.Generation{}, p.retained...), p.expired...) {
		if _, seen := p.raw[g.ManifestKey]; seen {
			continue
		}
		raw, err := r.Objects.Get(ctx, g.ManifestKey)
		if errors.Is(err, manifestv2.ErrObjectNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read the manifest %s of epoch %d generation %d: %w", g.ManifestKey, g.Epoch, g.Generation, err)
		}
		p.raw[g.ManifestKey] = raw
	}
	return nil
}
