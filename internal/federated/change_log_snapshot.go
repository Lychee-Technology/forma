package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
)

// The change_log reads of a federated pass (design.md §6.1). A pass reads
// its publication record and its dirty set in one REPEATABLE READ READ ONLY
// snapshot, and reads the hot keys and the page's hot rows in the same
// snapshot, so the reads below take the transaction rather than opening their
// own.

// ChangeLogSnapshotRequest says what one snapshot reads: the strict dirty set
// of SchemaID, the row IDs with an unflushed change_log entry. The pass reads
// its generation in the same snapshot, and that generation holds every row
// the snapshot shows flushed, so nothing widens the set (#252's flush-grace
// widening made up for a path set resolved before the snapshot).
type ChangeLogSnapshotRequest struct {
	Tables   model.StorageTables
	SchemaID int16
}

// validate reports why r cannot be read, or nil.
func (r ChangeLogSnapshotRequest) validate() error {
	if r.Tables.ChangeLog == "" {
		return errors.New("change log table name cannot be empty")
	}
	return nil
}

// ChangeLogSnapshot is what one snapshot read answered.
type ChangeLogSnapshot struct {
	// DirtyIDs holds each dirty row ID once, in no particular order.
	DirtyIDs []uuid.UUID
}

// snapshotTxOptions is the snapshot every pgx hot-leg read shares. READ ONLY
// lets PostgreSQL refuse a stray write, and on Aurora DSQL, whose only
// isolation level is REPEATABLE READ, it is the transaction DSQL runs anyway.
var snapshotTxOptions = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

// snapshotBeginner opens a transaction: the pool, or one connection.
type snapshotBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// beginReadSnapshot opens the snapshot of a pgx pass. The caller ends it.
func beginReadSnapshot(ctx context.Context, b snapshotBeginner) (pgx.Tx, error) {
	tx, err := b.BeginTx(ctx, snapshotTxOptions)
	if err != nil {
		return nil, fmt.Errorf("begin read-only snapshot: %w", err)
	}
	return tx, nil
}

// ReadChangeLogSnapshot reads req in a snapshot of its own and ends it. A
// pass that reads more in the same snapshot opens it with beginReadSnapshot
// and calls readChangeLogSnapshot instead.
func (f *PostgresDirtyIDFetcher) ReadChangeLogSnapshot(ctx context.Context, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error) {
	if err := req.validate(); err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read change log snapshot of schema %d: %w", req.SchemaID, err)
	}
	if f == nil || f.pool == nil {
		return ChangeLogSnapshot{}, errors.New("postgres dirty id fetcher is not available")
	}
	tx, err := beginReadSnapshot(ctx, f.pool)
	if err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read change log snapshot of schema %d: %w", req.SchemaID, err)
	}
	// A read-only transaction has nothing to undo: Rollback after Commit is
	// a no-op, and after a failure it releases the snapshot.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	snap, err := readChangeLogSnapshot(ctx, tx, req)
	if err != nil {
		return ChangeLogSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("end change log snapshot of schema %d: %w", req.SchemaID, err)
	}
	return snap, nil
}

// readChangeLogSnapshot reads req on tx, which must be a snapshot from
// beginReadSnapshot: the dirty set is only consistent with the record and the
// hot keys the pass reads when they share it.
func readChangeLogSnapshot(ctx context.Context, tx pgx.Tx, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error) {
	if err := req.validate(); err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read change log snapshot of schema %d: %w", req.SchemaID, err)
	}
	ids, err := queryRowIDs(ctx, tx, strictDirtySetQuery(req.Tables.ChangeLog), req.SchemaID)
	if err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read the dirty set of schema %d: %w", req.SchemaID, err)
	}
	return ChangeLogSnapshot{DirtyIDs: ids}, nil
}
