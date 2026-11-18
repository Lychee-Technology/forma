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
// with HoldLineage on gen's epoch and lineage. It answers how many it marked
// and whether ErrLineageReplaced stopped it. Marks are exact against
// flushed_at = 0 and the version, so a chunk sent again, after a 40001 or a
// COMMIT without an answer, marks nothing twice.
func (w *Writer) handoff(ctx context.Context, gen publication.Record, entries []Entry) (int, bool, error) {
	marked := 0
	for chunk := range slices.Chunk(entries, HandoffChunk) {
		n, err := w.handoffChunk(ctx, gen, chunk)
		if errors.Is(err, publication.ErrLineageReplaced) {
			return marked, true, nil
		}
		if err != nil {
			return marked, false, fmt.Errorf("hand off %d of %d exported entries after %d marked: %w", len(chunk), len(entries), marked, err)
		}
		marked += n
	}
	return marked, false, nil
}

// handoffChunk sends one handoff transaction until it commits, in at most
// Config.Attempts transactions. A 40001, the T_pub deadline or a COMMIT
// without an answer sends it again.
func (w *Writer) handoffChunk(ctx context.Context, gen publication.Record, chunk []Entry) (int, error) {
	var last error
	for attempt := 1; attempt <= w.cfg.Attempts; attempt++ {
		n, err := w.handoffOnce(ctx, gen, chunk)
		if err == nil || errors.Is(err, publication.ErrLineageReplaced) {
			return n, err
		}
		last = err
		if v := w.classify(ctx, err); v != resend && v != resolve {
			return 0, err
		}
		if err := pause(ctx, time.Duration(attempt)*resendBackoff); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("after %d transactions: %w: %w", w.cfg.Attempts, ErrAttemptsExhausted, last)
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
