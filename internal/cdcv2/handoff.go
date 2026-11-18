package cdcv2

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lychee-technology/forma/internal/publication"
)

// handoff marks the init generation's exported entries at their export
// versions, in transactions of at most HandoffChunk entries, each starting
// with HoldLineage on gen's epoch and lineage, and records in rep what it
// marked and whether ErrLineageReplaced stopped it. Marks are exact against
// flushed_at = 0 and the version, so a chunk sent again, after a 40001 or a
// COMMIT without an answer, marks nothing twice.
func (w *Writer) handoff(ctx context.Context, gen publication.Record, entries []Entry, rep *InitReport) error {
	for chunk := range slices.Chunk(entries, HandoffChunk) {
		n, lost, err := w.handoffChunk(ctx, gen, chunk)
		if lost {
			// A COMMIT of this chunk went without an answer and may have
			// landed: what the answered transaction did not mark, that
			// COMMIT may have, and the report cannot say which.
			rep.Unconfirmed += len(chunk) - n
		}
		if errors.Is(err, publication.ErrLineageReplaced) {
			rep.HandoffStopped = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("hand off %d of %d exported entries after %d marked: %w", len(chunk), len(entries), rep.Marked, err)
		}
		rep.Marked += n
	}
	return nil
}

// handoffChunk sends one handoff transaction until it commits, in at most
// Config.Attempts transactions. A 40001, the T_pub deadline or a COMMIT
// without an answer sends it again. It answers how many entries the
// transaction that committed marked, and lost, whether a COMMIT of the chunk
// went without an answer before, which may have marked the rest.
func (w *Writer) handoffChunk(ctx context.Context, gen publication.Record, chunk []Entry) (int, bool, error) {
	var last error
	lost := false
	for attempt := 1; attempt <= w.cfg.Attempts; attempt++ {
		n, err := w.handoffOnce(ctx, gen, chunk)
		if err == nil || errors.Is(err, publication.ErrLineageReplaced) {
			return n, lost, err
		}
		last = err
		v := w.classify(ctx, err)
		if v != resend && v != resolve {
			return 0, lost, err
		}
		lost = lost || v == resolve
		if err := pause(ctx, time.Duration(attempt)*resendBackoff); err != nil {
			return 0, lost, err
		}
	}
	return 0, lost, fmt.Errorf("after %d transactions: %w: %w", w.cfg.Attempts, ErrAttemptsExhausted, last)
}

// handoffOnce sends one handoff transaction: HoldLineage first, then the
// database time the marks carry, then the marks.
func (w *Writer) handoffOnce(ctx context.Context, gen publication.Record, chunk []Entry) (int, error) {
	if err := w.fail(ctx, PointHandoff); err != nil {
		return 0, err
	}
	tctx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	defer cancel()
	tx, err := w.cfg.DB.BeginTx(tctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin a handoff transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := publication.HoldLineage(tctx, publication.SQLTx(tx), w.tables, gen.SchemaID, gen.Epoch, gen.Lineage); err != nil {
		return 0, err
	}
	flushedAt, err := w.now(tctx, tx)
	if err != nil {
		return 0, err
	}
	n, err := w.mark(tctx, tx, gen.SchemaID, chunk, flushedAt)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, &commitError{err}
	}
	if err := w.fail(ctx, PointCommitAnswer); err != nil {
		return 0, &commitError{fmt.Errorf("%w: %w", ErrCommitAnswerLost, err)}
	}
	return n, nil
}
