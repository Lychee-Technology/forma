package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
	"go.uber.org/zap"
)

// initSchema re-onboards one schema under its lock, on the lock's held
// context. It loads the schema's publication record, whose epoch pins the
// init, then has the V2 writer export every live row in snapshots of the
// schema's batch size, publish them as an init generation and hand off the
// exported entries (cdcv2.Writer.Init). A schema with no live row publishes
// an empty generation: the parquet tiers hold nothing, and no placeholder
// object is written.
func initSchema(ctx context.Context, runCtx *initRunContext, schemaID int16) (SchemaInit, error) {
	rec, err := readPublicationRecord(ctx, runCtx.db, runCtx.cfg.EntityMainTable, schemaID)
	if err != nil {
		return SchemaInit{}, err
	}
	if err := refuseMovedEpoch(rec, runCtx.epochs); err != nil {
		return SchemaInit{}, err
	}
	if rec.State == publication.Dropped {
		return SchemaInit{}, fmt.Errorf("schema %d at epoch %d is dropped; it is onboarded again once it is registered again: %w",
			schemaID, rec.Epoch, publication.ErrDropped)
	}
	attrCache := runCtx.attrCaches[schemaID]
	batchSize := resolveInitBatchSize(runCtx, schemaID, attrCache)
	if runCtx.dryRun {
		return planInit(ctx, runCtx, rec, batchSize)
	}
	runCtx.logger.Info("initializing schema",
		zap.Int16("schema_id", schemaID), zap.Int64("epoch", rec.Epoch), zap.String("state", string(rec.State)),
		zap.Int64("generation", rec.Generation), zap.Int("batch_size", batchSize))

	writer, err := newPublicationWriter(runCtx.cfg, runCtx.db, runCtx.store, 0, batchSize)
	if err != nil {
		return SchemaInit{}, fmt.Errorf("cdc init: %w", err)
	}
	exporter := &baseExporter{
		duck: runCtx.duck, store: runCtx.store, cfg: runCtx.cfg,
		registry: runCtx.schemaRegistry, attrCache: attrCache, logger: runCtx.logger,
	}
	rep, err := writer.Init(ctx, schemaID, rec.Epoch, exporter)
	rep, err = resumeInit(ctx, runCtx, writer, rep, err)
	out := schemaInitFrom(rec, rep)
	if err != nil {
		return out, fmt.Errorf("publish the init generation: %w", explainWriterError("cdc-init", schemaID, err))
	}
	if rep.HandoffStopped {
		runCtx.logger.Warn("init handoff stopped: a later cdc-init or a drop replaced the init's lineage; the entries it had not marked stay dirty for the next flush",
			zap.Int16("schema_id", schemaID), zap.Int64("generation", rep.Generation.Generation))
	}
	runCtx.logger.Info("schema init completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("epoch", rec.Epoch),
		zap.Int64("generation", out.Generation),
		zap.Int64("total_rows_exported", out.Rows),
		zap.Int("total_files_created", out.Files),
		zap.Int("entries_exported", out.Entries),
		zap.Int("entries_handed_off", out.Marked),
		zap.Int("entries_unconfirmed", out.Unconfirmed))
	return out, nil
}

// planInit is a dry run's initSchema: it counts the live rows and the base
// objects a real run would write and changes nothing: no export, no object,
// no record change and no mark (#180).
func planInit(ctx context.Context, runCtx *initRunContext, rec publication.Record, batchSize int) (SchemaInit, error) {
	rows, err := getEntityMainCount(ctx, runCtx.db, runCtx.cfg.EntityMainTable, rec.SchemaID)
	if err != nil {
		return SchemaInit{}, fmt.Errorf("count the live rows: %w", err)
	}
	files := 0
	if batchSize > 0 {
		files = int((rows + int64(batchSize) - 1) / int64(batchSize))
	}
	runCtx.logger.Info("dry-run: skipping init export",
		zap.Int16("schema_id", rec.SchemaID), zap.Int64("epoch", rec.Epoch), zap.String("state", string(rec.State)),
		zap.Int64("rows", rows), zap.Int("would_create_files", files), zap.Int("batch_size", batchSize))
	return SchemaInit{SchemaID: rec.SchemaID, Epoch: rec.Epoch, Rows: rows, Files: files}, nil
}

// resumeInit settles, once and on a fresh context, an init the writer
// stopped with ErrUnresolved: the COMMIT of its init generation went without
// an answer, and its handoff has not run. ResumeInit runs the handoff from the
// entries the error kept when it finds the init committed.
func resumeInit(ctx context.Context, runCtx *initRunContext, writer *cdcv2.Writer, rep cdcv2.InitReport, err error) (cdcv2.InitReport, error) {
	var u *cdcv2.UnresolvedError
	if !errors.As(err, &u) {
		return rep, err
	}
	runCtx.logger.Warn("init publication outcome unknown; settling it once",
		zap.Int16("schema_id", u.Cur.SchemaID), zap.Error(err))
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	resumed, rerr := writer.ResumeInit(sctx, u)
	if rerr == nil {
		rep.Generation = resumed.Generation
		rep.Marked, rep.Unconfirmed, rep.HandoffStopped = resumed.Marked, resumed.Unconfirmed, resumed.HandoffStopped
		return rep, nil
	}
	var again *cdcv2.UnresolvedError
	switch {
	case errors.As(rerr, &again):
		return rep, fmt.Errorf("%s; settling it in-process failed too. Nothing records it across this process: its %d exported "+
			"entries were not handed off and stay dirty, which is safe. If the record names that token, the init is published "+
			"and the next flush exports those entries again; otherwise run cdc-init again. Its staged objects must stay until "+
			"garbage collection: %w", unresolvedNote(u), len(u.Handoff), rerr)
	case errors.Is(rerr, cdcv2.ErrAbandoned):
		return rep, fmt.Errorf("%s; settling it proved it can never commit, so nothing it exported is visible and change_log "+
			"is unchanged: run cdc-init again: %w", unresolvedNote(u), rerr)
	}
	return rep, fmt.Errorf("%s; it was found committed, but its handoff failed: the entries it did not mark stay dirty "+
		"for the next flush: %w", unresolvedNote(u), rerr)
}

// schemaInitFrom answers what rep says the init of rec's schema did.
func schemaInitFrom(rec publication.Record, rep cdcv2.InitReport) SchemaInit {
	return SchemaInit{
		SchemaID: rec.SchemaID, Epoch: rec.Epoch, Generation: rep.Generation.Generation,
		Rows: int64(rep.Rows), Files: rep.Files, Entries: rep.Entries, Marked: rep.Marked,
		Unconfirmed: rep.Unconfirmed, HandoffStopped: rep.HandoffStopped,
	}
}
