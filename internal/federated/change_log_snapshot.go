package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The change_log reads of the pgx hot leg (design.md §6.1.1, plan step 1).
// A pgx pass reads its dirty set, and when it needs one its parquet-history
// answer, in one REPEATABLE READ READ ONLY snapshot. Later steps read the hot
// keys and the page's hot rows in the same snapshot, so the reads below take
// the transaction rather than opening their own. Nothing in the engine reads
// through them yet: the attach transport keeps FetchDirtyRowIDs until the
// pgx transport is wired behind DuckDBConfig.HotLegTransport (plan step 5).

// DirtySetForm names the dirty set a federated pass reads, by its tier form
// (design.md §5, HasHot).
type DirtySetForm int

const (
	// DirtySetHotExcluded is the strict set, flushed_at = 0. A pass whose
	// preferred tiers leave out hot has no hot leg to serve a row the
	// widening would discard, so it shadows only the unflushed rows, as
	// today.
	DirtySetHotExcluded DirtySetForm = iota + 1
	// DirtySetHot is the widened set, flushed_at = 0 OR flushed_at >= the
	// flush-grace cutoff (#252): the barrier of a pass with a hot leg, the
	// set the template's dirty_ids reads in DuckDB today.
	DirtySetHot
)

func (f DirtySetForm) String() string {
	switch f {
	case DirtySetHotExcluded:
		return "hot-excluded"
	case DirtySetHot:
		return "hot"
	}
	return fmt.Sprintf("DirtySetForm(%d)", int(f))
}

// ChangeLogSnapshotRequest says what one snapshot reads.
type ChangeLogSnapshotRequest struct {
	Tables   model.StorageTables
	SchemaID int16
	Form     DirtySetForm
	// GraceCutoffMs is the flush-grace cutoff of the hot form: an entry
	// marked at or after it counts as dirty. The hot-excluded form ignores
	// it. sqlgen.FlushGraceCutoffDisabled reads no marked entry, the strict
	// set; the pgx transport never passes it (design.md §6.1.1, *The
	// widening is never disabled on this path*), but the reader does not
	// police the engine's cutoff resolution.
	GraceCutoffMs int64
	// ParquetHistory asks for HasParquetHistory from the same snapshot. Only
	// the #720 empty-scan check needs it, and its entity_main anti-join is
	// not free, so a pass that does not ask does not pay for it.
	ParquetHistory bool
}

// validate reports why r cannot be read, or nil.
func (r ChangeLogSnapshotRequest) validate() error {
	if r.Tables.ChangeLog == "" {
		return errors.New("change log table name cannot be empty")
	}
	if r.ParquetHistory && r.Tables.EntityMain == "" {
		return errors.New("entity main table name cannot be empty when parquet history is read")
	}
	if r.Form != DirtySetHotExcluded && r.Form != DirtySetHot {
		return fmt.Errorf("unknown dirty set form %s", r.Form)
	}
	return nil
}

// ChangeLogSnapshot is what one snapshot read answered.
type ChangeLogSnapshot struct {
	// DirtyIDs holds each dirty row ID once, in no particular order.
	DirtyIDs []uuid.UUID
	// HasParquetHistory is PostgresDirtyIDFetcher.HasParquetHistory's answer
	// in the snapshot. It is false when the request did not ask for it.
	HasParquetHistory bool
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
// beginReadSnapshot: the dirty set and the parquet-history answer are only
// consistent with each other, and with the hot keys a later read takes, when
// they share it.
func readChangeLogSnapshot(ctx context.Context, tx pgx.Tx, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error) {
	if err := req.validate(); err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read change log snapshot of schema %d: %w", req.SchemaID, err)
	}
	query, args := dirtySetQuery(req)
	ids, err := queryRowIDs(ctx, tx, query, args...)
	if err != nil {
		return ChangeLogSnapshot{}, fmt.Errorf("read %s dirty set of schema %d: %w", req.Form, req.SchemaID, err)
	}
	snap := ChangeLogSnapshot{DirtyIDs: ids}
	if req.ParquetHistory {
		found, err := queryParquetHistory(ctx, tx, req.Tables, req.SchemaID)
		if err != nil {
			return ChangeLogSnapshot{}, err
		}
		snap.HasParquetHistory = found
	}
	return snap, nil
}

// dirtySetQuery renders req's dirty set and its arguments. The hot-excluded
// form is FetchDirtyRowIDs' query. The hot form reads the same set and the
// entries marked at or after the cutoff as two arms of a UNION, so that each
// arm is one range of the (schema_id, flushed_at) index init-db builds, and
// the UNION answers a row with entries in both arms once.
func dirtySetQuery(req ChangeLogSnapshotRequest) (string, []any) {
	if req.Form == DirtySetHotExcluded {
		return strictDirtySetQuery(req.Tables.ChangeLog), []any{req.SchemaID}
	}
	changeLog := sqlutil.SanitizeIdentifier(req.Tables.ChangeLog)
	return fmt.Sprintf(`SELECT row_id FROM %[1]s WHERE schema_id = $1 AND flushed_at = 0
	UNION
	SELECT row_id FROM %[1]s WHERE schema_id = $1 AND flushed_at >= $2`, changeLog), []any{req.SchemaID, req.GraceCutoffMs}
}
