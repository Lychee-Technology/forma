package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// SchemaFlush is what one schema's flush published.
type SchemaFlush struct {
	SchemaID int16
	// Epoch is the record's epoch the run loaded and pinned.
	Epoch int64
	// Generations are the generation numbers the flush committed, in order.
	// Each is an immutable manifest the record named in the transaction
	// that marked its entries.
	Generations []int64
	// Selected counts the unflushed entries the flush selected, Marked those
	// the committed generations marked flushed. Unconfirmed counts entries a
	// generation found committed after a COMMIT without an answer may have
	// marked beyond Marked (cdcv2.FlushReport).
	Selected, Marked, Unconfirmed int
	// Restarts, Resends and Rebases count the writer's retries.
	Restarts, Resends, Rebases int
}

// flushUnderLock is processSchema's work once the schema lock is held, on
// the lock's held context. It loads the schema's publication record, whose
// epoch pins every publication of this run, then publishes the schema's
// batch if the thresholds call for it.
func (c *schemaFlushContext) flushUnderLock(ctx context.Context, schemaID int16) error {
	rec, err := readPublicationRecord(ctx, c.db, c.cfg.EntityMainTable, schemaID)
	if err != nil {
		return err
	}
	if err := refuseMovedEpoch(rec, c.epochs); err != nil {
		return err
	}
	if err := refuseUnflushable(rec); err != nil {
		return err
	}
	cnt, oldest, err := GetChangeLogStats(ctx, c.db, c.tableName, schemaID)
	if err != nil {
		return fmt.Errorf("get changelog stats: %w", err)
	}
	if cnt == 0 {
		c.logger.Sugar().Infow("no unflushed rows", "schema_id", schemaID)
		return nil
	}
	if !shouldFlush(c.cfg, cnt, oldest) {
		c.logger.Sugar().Infow("skip flush: thresholds not met", "schema_id", schemaID, "cnt", cnt, "oldest", oldest)
		return nil
	}
	if c.dryRun {
		// Decided before the first side effect: no export, no staged object
		// or manifest, no record change and no mark (#180). Log the work a
		// real run would do.
		c.logger.Sugar().Infow("dry-run: skipping flush",
			"schema_id", schemaID, "epoch", rec.Epoch, "state", rec.State, "generation", rec.Generation,
			"unflushed", cnt, "would_select", min(cnt, int64(c.cfg.BatchSize)))
		return nil
	}
	if err := c.publishFlush(ctx, rec); err != nil {
		return fmt.Errorf("execute flush: %w", err)
	}
	return nil
}

// publishFlush runs the V2 writer's flush of rec's schema at rec's epoch.
func (c *schemaFlushContext) publishFlush(ctx context.Context, rec publication.Record) error {
	stager := &deltaStager{
		duck: c.duck, db: c.db, store: c.store, cfg: c.cfg,
		registry: c.schemaRegistry, attrCache: c.attrCaches[rec.SchemaID], logger: c.logger,
	}
	rep, err := c.writer.Flush(ctx, rec.SchemaID, rec.Epoch, stager)
	rep, err = c.settleFlush(ctx, rep, err)
	c.recordFlush(rec, rep)
	if errors.Is(err, errNothingExported) {
		c.logger.Sugar().Infow("flush published nothing more: the batch's export wrote no row, and its entries stay dirty for the next flush",
			"schema_id", rec.SchemaID, "err", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("publish flush generations: %w", explainWriterError("cdc-flush", rec.SchemaID, err))
	}
	return nil
}

// settleFlush settles, once and on a fresh context, a flush the writer
// stopped with ErrUnresolved: its COMMIT went without an answer and the
// writer could not tell whether it landed. A flush found committed is
// complete, since its marks committed with it: the report counts them from
// the range the error carries (the marks of each COMMIT that went without an
// answer), exact when those counted alike and Unconfirmed otherwise.
func (c *schemaFlushContext) settleFlush(ctx context.Context, rep cdcv2.FlushReport, err error) (cdcv2.FlushReport, error) {
	var u *cdcv2.UnresolvedError
	if !errors.As(err, &u) {
		return rep, err
	}
	c.logger.Sugar().Warnw("flush publication outcome unknown; settling it once", "schema_id", u.Cur.SchemaID, "err", err)
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	gen, serr := c.writer.Settle(sctx, u)
	if serr == nil {
		c.logger.Sugar().Warnw("flush publication found committed after its COMMIT went without an answer",
			"schema_id", gen.SchemaID, "generation", gen.Generation)
		rep.Generations = append(rep.Generations, gen)
		rep.Marked += u.Marked
		rep.Unconfirmed += u.Unconfirmed
		return rep, nil
	}
	if errors.Is(serr, cdcv2.ErrAbandoned) {
		return rep, fmt.Errorf("%s; settling it proved it can never commit, so nothing it staged is visible "+
			"and its entries stay dirty for the next flush: %w", unresolvedNote(u), serr)
	}
	return rep, fmt.Errorf("%s; settling it in-process failed too. Its marks commit with it, so its entries are flushed "+
		"or dirty as change_log says and the next flush exports what is still dirty; its staged objects must stay "+
		"until garbage collection: %w", unresolvedNote(u), serr)
}

// recordFlush adds what the writer published to the run's report and log.
func (c *schemaFlushContext) recordFlush(rec publication.Record, rep cdcv2.FlushReport) {
	if rep.Selected == 0 && len(rep.Generations) == 0 {
		return
	}
	gens := make([]int64, len(rep.Generations))
	for i, g := range rep.Generations {
		gens[i] = g.Generation
	}
	c.flushed = append(c.flushed, SchemaFlush{
		SchemaID: rec.SchemaID, Epoch: rec.Epoch, Generations: gens,
		Selected: rep.Selected, Marked: rep.Marked, Unconfirmed: rep.Unconfirmed,
		Restarts: rep.Restarts, Resends: rep.Resends, Rebases: rep.Rebases,
	})
	c.logger.Sugar().Infow("flush published",
		"schema_id", rec.SchemaID, "epoch", rec.Epoch, "generations", gens,
		"rows_selected", rep.Selected, "rows_flushed", rep.Marked, "rows_unconfirmed", rep.Unconfirmed,
		"restarts", rep.Restarts, "resends", rep.Resends, "rebases", rep.Rebases)
	if unmarked := rep.Selected - rep.Marked - rep.Unconfirmed; unmarked > 0 {
		// Unmarked entries changed since the selection or were held by an
		// open writer; the mark cannot tell which. Either way they stay dirty.
		c.logger.Sugar().Infow("flush left entries unmarked; they stay dirty for the next flush",
			"schema_id", rec.SchemaID, "rows_unmarked", unmarked)
	}
}
