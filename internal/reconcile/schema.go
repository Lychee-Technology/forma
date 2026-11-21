package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// pass holds what one schema's pass observed in collection order.
type pass struct {
	id         int16
	horizon    int64
	listed     []publication.Object
	record     *publication.Snapshot
	recordErr  error
	retained   []publication.Generation
	expired    []publication.Generation
	lifecycle  publication.Lifecycle
	raw        map[string][]byte
	verified   map[string]*manifestv2.Manifest
	findings   []Finding
	byRecord   bool
	rowFinding int
	rowDetail  string
	concurrent int
}

// stateSnapshot is what one REPEATABLE READ READ ONLY snapshot reads.
type stateSnapshot struct {
	record    *publication.Snapshot
	recordErr error
	retained  []publication.Generation
	expired   []publication.Generation
	lifecycle publication.Lifecycle
}

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
		retired, recoveryRetired, err := r.retire(ctx, p)
		rep.Collected.Retired = retired
		rep.Collected.RecoveryRetired = recoveryRetired
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
	if !r.Opts.GC {
		if err := r.dropConcurrentlyExpired(ctx, p); err != nil {
			rep.Err = fmt.Errorf("schema %d: %w", id, err)
			return rep
		}
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

func (r *Reconciler) read(ctx context.Context, id int16) (*pass, error) {
	p := &pass{id: id}
	if r.Opts.GC {
		h, err := publication.RaiseHorizon(ctx, publication.DB(r.DB), r.Tables, id, r.Opts.OrphanAge)
		switch {
		case errors.Is(err, publication.ErrNoRecord):
		case err != nil:
			return nil, fmt.Errorf("schema %d: %w", id, err)
		default:
			p.horizon = h
		}
	}
	listed, err := r.Objects.List(ctx, r.Layout.SchemaPrefix(id))
	if err != nil {
		return nil, fmt.Errorf("schema %d: list objects under %s: %w", id, r.Layout.SchemaPrefix(id), err)
	}
	for _, o := range listed {
		if o.LastModified <= 0 {
			return nil, fmt.Errorf("schema %d: object %s was listed with LastModified %d: %w", id, o.Key, o.LastModified, ErrUnknownAge)
		}
	}
	p.listed = listed
	snap, err := r.readSnapshot(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	p.record, p.recordErr = snap.record, snap.recordErr
	p.retained, p.expired, p.lifecycle = snap.retained, snap.expired, snap.lifecycle
	if errors.Is(snap.recordErr, publication.ErrNoRecord) {
		p.findings = append(p.findings, recordFinding(NoRecord, snap.recordErr))
	} else if errors.Is(snap.recordErr, publication.ErrRecordInvalid) {
		p.findings = append(p.findings, recordFinding(RecordInvalid, snap.recordErr))
	}
	if p.record != nil && !r.Opts.GC {
		p.horizon = p.record.Horizon
	}
	return p, nil
}

// readSnapshot reads the record, retained generations, expired generations and
// lifecycle state in one REPEATABLE READ READ ONLY transaction and ends it.
func (r *Reconciler) readSnapshot(ctx context.Context, id int16) (stateSnapshot, error) {
	tx, err := r.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return stateSnapshot{}, fmt.Errorf("begin read-only snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := publication.SQLTx(tx)
	var snap stateSnapshot
	s, err := publication.Read(ctx, q, r.Tables, id)
	switch {
	case errors.Is(err, publication.ErrNoRecord), errors.Is(err, publication.ErrRecordInvalid):
		snap.recordErr = err
	case err != nil:
		return stateSnapshot{}, fmt.Errorf("read record in snapshot: %w", err)
	default:
		snap.record = &s
	}
	if snap.retained, err = publication.Retained(ctx, q, r.Tables, id, r.Opts.RetentionGrace); err != nil {
		return stateSnapshot{}, fmt.Errorf("read retained generations in snapshot: %w", err)
	}
	if snap.expired, err = publication.Expired(ctx, q, r.Tables, id, r.Opts.RetentionGrace); err != nil {
		return stateSnapshot{}, fmt.Errorf("read expired generations in snapshot: %w", err)
	}
	if snap.lifecycle, err = publication.ReadLifecycle(ctx, q, r.Tables, id, r.Opts.RetentionGrace); err != nil {
		return stateSnapshot{}, fmt.Errorf("read lifecycle in snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return stateSnapshot{}, fmt.Errorf("end read-only snapshot: %w", err)
	}
	return snap, nil
}

// retire stamps retired_at on superseded non-current generations and stamps
// recovery_retired_at on an unretired lost-record recovery after the snapshot
// that observed them committed has ended.
func (r *Reconciler) retire(ctx context.Context, p *pass) ([]int64, bool, error) {
	var retired []int64
	for _, g := range p.retained {
		if g.SupersededAt == 0 || g.RetiredAt != 0 || p.isCurrent(g) {
			continue
		}
		ok, err := publication.Retire(ctx, publication.DB(r.DB), r.Tables, p.id, g.Generation)
		if err != nil {
			return retired, false, fmt.Errorf("schema %d: %w", p.id, err)
		}
		if ok {
			retired = append(retired, g.Generation)
		}
	}
	recoveryRetired := false
	if p.lifecycle.Present && p.lifecycle.RecoveryEpoch > 0 && p.lifecycle.RecoveryRetiredAt == 0 {
		ok, err := publication.RetireRecovery(ctx, publication.DB(r.DB), r.Tables, p.id, p.lifecycle.RecoveryEpoch)
		if err != nil {
			return retired, false, fmt.Errorf("schema %d: %w", p.id, err)
		}
		recoveryRetired = ok
	}
	return retired, recoveryRetired, nil
}

func (p *pass) isCurrent(g publication.Generation) bool {
	return p.record != nil && p.record.State == publication.Published && g.Generation == p.record.Generation
}

func (r *Reconciler) fetchManifests(ctx context.Context, p *pass) error {
	p.raw = map[string][]byte{}
	gens := append(append([]publication.Generation{}, p.retained...), p.expired...)
	if p.record != nil && p.record.State == publication.Published {
		gens = append(gens, p.recordGeneration())
	}
	for _, g := range gens {
		if _, seen := p.raw[g.ManifestKey]; seen {
			continue
		}
		raw, err := r.Objects.Get(ctx, g.ManifestKey)
		if errors.Is(err, manifestv2.ErrObjectNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read manifest %s of epoch %d generation %d: %w", g.ManifestKey, g.Epoch, g.Generation, err)
		}
		p.raw[g.ManifestKey] = raw
	}
	return nil
}

// dropConcurrentlyExpired opens a single fresh REPEATABLE READ READ ONLY
// snapshot S2 to distinguish permanent loss from a generation that expired or
// was forgotten while a lock-free diagnosis ran. A finding is left out only
// when S2 proves the generation is no longer current, is no longer retained,
// and either crossed into Expired or was legitimately forgotten (<= S2's
// durable forgotten_generation floor). Dropped findings mark the schema
// Inconclusive (exit 2), never clean.
func (r *Reconciler) dropConcurrentlyExpired(ctx context.Context, p *pass) error {
	if !slices.ContainsFunc(p.findings, concurrentlyCaused) {
		return nil
	}
	s2, err := r.readSnapshot(ctx, p.id)
	if err != nil {
		return fmt.Errorf("recheck publication state in snapshot: %w", err)
	}
	stillCurrent := samePublishedRecord(p.record, s2.record, s2.recordErr)
	beforeFloor := max(p.lifecycle.ForgottenGeneration, maxGeneration(p.expired))
	afterFloor := max(s2.lifecycle.ForgottenGeneration, maxGeneration(s2.expired))
	kept := p.findings[:0]
	rowFinding, recordManifestLeft := -1, false
	for i, f := range p.findings {
		expiredNow := concurrentlyCaused(f) &&
			!(f.Current && stillCurrent) &&
			beforeFloor < f.Generation && afterFloor >= f.Generation &&
			!hasGeneration(s2.retained, f.Generation, f.Epoch) &&
			(f.Generation <= s2.lifecycle.ForgottenGeneration || hasGeneration(s2.expired, f.Generation, f.Epoch))
		if expiredNow {
			p.concurrent++
			recordManifestLeft = recordManifestLeft || (p.byRecord && f.Current && f.Key == p.record.ManifestKey)
			continue
		}
		if p.byRecord && i == p.rowFinding {
			rowFinding = len(kept)
		}
		kept = append(kept, f)
	}
	p.findings = kept
	if p.byRecord {
		p.rowFinding = rowFinding
		if recordManifestLeft && rowFinding >= 0 {
			p.findings[rowFinding].Detail = p.rowDetail + "; " + readsCollected
		}
	}
	return nil
}

func samePublishedRecord(before, after *publication.Snapshot, afterErr error) bool {
	if before == nil || before.State != publication.Published {
		return false
	}
	if errors.Is(afterErr, publication.ErrNoRecord) || errors.Is(afterErr, publication.ErrRecordInvalid) || after == nil {
		return true // missing or invalid record is no proof the generation moved
	}
	rec := before.Record
	return after.State == publication.Published && after.Epoch == rec.Epoch &&
		after.Generation == rec.Generation && after.Token == rec.Token
}

func maxGeneration(gens []publication.Generation) int64 {
	var top int64
	for _, g := range gens {
		top = max(top, g.Generation)
	}
	return top
}

func hasGeneration(gens []publication.Generation, generation, epoch int64) bool {
	return slices.ContainsFunc(gens, func(g publication.Generation) bool {
		return g.Generation == generation && g.Epoch == epoch
	})
}

func concurrentlyCaused(f Finding) bool {
	return f.absent && f.Generation != 0
}
