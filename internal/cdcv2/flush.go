package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// DeltaStager writes a flush generation's parquet objects.
type DeltaStager interface {
	// StageDelta exports the rows of req.Entries to delta objects under
	// fresh keys of req.Layout and req.Epoch (Layout.NewParquetKey), each
	// written once, and answers their manifest entries. It runs before the
	// publication transaction, and what it writes stays unreachable until
	// that transaction commits.
	StageDelta(ctx context.Context, req DeltaRequest) ([]manifestv2.File, error)
}

// DeltaRequest is one flush generation's export.
type DeltaRequest struct {
	SchemaID int16
	Epoch    int64
	Layout   manifestv2.Layout
	Entries  []Entry
}

// FlushReport is what a Flush published.
type FlushReport struct {
	// Generations are the records the flush committed, in order.
	Generations []publication.Record
	// Selected counts the entries selected, Marked those the committed
	// generations marked flushed.
	Selected, Marked int
	// Unconfirmed counts the entries a generation may have marked beyond
	// Marked: it was found committed after COMMITs that went without an
	// answer (Resolve), so the transaction that committed is one of those,
	// and their marks counted differently (a row moved, or a writer held
	// an entry locked, between them). change_log says which are flushed: the
	// flush marked at least Marked entries and at most Marked + Unconfirmed.
	// When they counted alike, Marked is exact and Unconfirmed zero.
	Unconfirmed int
	// Restarts counts abandoned publications after which the flush selected
	// its entries again.
	Restarts int
	// Resends counts publications sent again with the same token, and
	// Rebases those restaged onto a new head.
	Resends, Rebases int
}

// Flush publishes schemaID's unflushed entries as flush generations: up to
// Config.BatchSize entries, in generations of at most DSQLGenerationLimit on
// Aurora DSQL and of the whole batch on PostgreSQL. Each generation stages
// its delta objects and its manifest before BEGIN, then commits the
// compare-and-set and the exact-version marks in one transaction.
//
// pin is the epoch the process loaded the schema at (0 pins nothing): a
// record of another epoch fails the flush closed (ErrEpochChanged). A schema
// that is AWAITING_INIT or DROPPED is refused. A publication abandoned under
// the rebase rules makes the flush select the still unflushed entries again,
// at most Config.Restarts times; the generations committed before stay.
func (w *Writer) Flush(ctx context.Context, schemaID int16, pin int64, st DeltaStager) (FlushReport, error) {
	var rep FlushReport
	if err := w.needChangeLog("flush"); err != nil {
		return rep, err
	}
	for {
		err := w.flushRound(ctx, schemaID, pin, st, &rep)
		if err == nil || !errors.Is(err, ErrAbandoned) || rep.Restarts >= w.cfg.Restarts {
			return rep, err
		}
		rep.Restarts++
	}
}

// flushRound reads the record, selects a batch and publishes it.
func (w *Writer) flushRound(ctx context.Context, schemaID int16, pin int64, st DeltaStager, rep *FlushReport) error {
	cur, err := w.readRecord(ctx, schemaID, pin)
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	switch cur.State {
	case publication.AwaitingInit:
		return fmt.Errorf("flush schema %d: only a cdc-init publishes its first V2 generation: %w", schemaID, publication.ErrAwaitingInit)
	case publication.Dropped:
		return fmt.Errorf("flush schema %d at epoch %d: %w", schemaID, cur.Epoch, publication.ErrDropped)
	}
	entries, err := w.selectEntries(ctx, schemaID, w.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	rep.Selected += len(entries)
	// The first flush of an unpublished epoch starts a lineage; every later
	// one keeps the record's.
	lineage := uuid.New()
	for chunk := range slices.Chunk(entries, w.generationLimit()) {
		out, err := w.flushGeneration(ctx, cur, lineage, chunk, st, rep)
		if err != nil {
			return err
		}
		rep.Generations = append(rep.Generations, out)
		cur = out
	}
	return nil
}

// flushGeneration stages and publishes one generation of entries on cur, and
// records in rep what it marked.
func (w *Writer) flushGeneration(ctx context.Context, cur publication.Record, lineage uuid.UUID, entries []Entry, st DeltaStager, rep *FlushReport) (publication.Record, error) {
	stagedAt, err := w.now(ctx, w.cfg.DB)
	if err != nil {
		return publication.Record{}, fmt.Errorf("flush schema %d: %w", cur.SchemaID, err)
	}
	if err := w.fail(ctx, PointStageObjects); err != nil {
		return publication.Record{}, fmt.Errorf("flush schema %d: %w", cur.SchemaID, err)
	}
	deltas, err := st.StageDelta(ctx, DeltaRequest{SchemaID: cur.SchemaID, Epoch: cur.Epoch, Layout: w.cfg.Layout, Entries: entries})
	if err != nil {
		return publication.Record{}, fmt.Errorf("flush schema %d: stage the delta objects of %d entries: %w", cur.SchemaID, len(entries), err)
	}
	p := &pub{
		kind: publication.KindFlush,
		cur:  cur,
		build: func(ctx context.Context, cur publication.Record) (publication.Next, error) {
			return w.stageFlushManifest(ctx, cur, lineage, deltas, stagedAt)
		},
		mark: func(ctx context.Context, tx *sql.Tx, out publication.Record) (int, error) {
			return w.mark(ctx, tx, out.SchemaID, entries, out.UpdatedAt)
		},
	}
	if p.next, err = p.build(ctx, cur); err != nil {
		return publication.Record{}, fmt.Errorf("flush schema %d: %w", cur.SchemaID, err)
	}
	out, err := w.publish(ctx, p)
	rep.Resends += p.resends
	rep.Rebases += p.rebases
	if err != nil {
		return publication.Record{}, fmt.Errorf("flush: %w", err)
	}
	lo, hi := p.markedRange()
	rep.Marked += lo
	rep.Unconfirmed += hi - lo
	return out, nil
}

// stageFlushManifest stages the flush generation after cur: the parent's
// files, verified through the cache, and deltas, under a fresh token.
func (w *Writer) stageFlushManifest(ctx context.Context, cur publication.Record, lineage uuid.UUID, deltas []manifestv2.File, stagedAt int64) (publication.Next, error) {
	var parent *manifestv2.Manifest
	if cur.State == publication.Published {
		lineage = cur.Lineage
		ref, err := manifestv2.RecordRef(cur)
		if err != nil {
			return publication.Next{}, fmt.Errorf("name the parent manifest of schema %d: %w", cur.SchemaID, err)
		}
		if parent, err = w.cfg.Cache.Get(ctx, ref); err != nil {
			return publication.Next{}, fmt.Errorf("get the parent manifest of schema %d generation %d: %w", cur.SchemaID, cur.Generation, err)
		}
	}
	h, err := manifestv2.Stage(cur, publication.KindFlush, lineage, uuid.New())
	if err != nil {
		return publication.Next{}, err
	}
	m, err := manifestv2.NewFlush(parent, h, deltas)
	if err != nil {
		return publication.Next{}, err
	}
	return w.stageManifest(ctx, m, stagedAt)
}

// stageManifest writes m once and answers what Publish commits for it.
func (w *Writer) stageManifest(ctx context.Context, m manifestv2.Manifest, stagedAt int64) (publication.Next, error) {
	if err := w.fail(ctx, PointStageManifest); err != nil {
		return publication.Next{}, err
	}
	key, digest, err := manifestv2.PutManifest(ctx, w.cfg.Store, w.cfg.Layout, m)
	if err != nil {
		return publication.Next{}, err
	}
	return publication.Next{
		Kind: m.Kind, Token: m.Token, ManifestKey: key, ManifestSHA256: digest,
		Lineage: m.Lineage, StagedAt: stagedAt, StagingBudget: w.cfg.StagingBudget,
	}, nil
}
