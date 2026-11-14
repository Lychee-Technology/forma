package federated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
	"go.uber.org/zap"
)

// The seams of the pgx hot leg (design.md §6.1, pgx-hot-leg-plan.md step
// 5): the snapshot a pass reads PostgreSQL in, and the pinned DuckDB
// connection its parquet leg runs on.

// HotLegReader is the PostgreSQL route's half of a pgx pass beyond the
// change_log: the hot keys, and after the merge the page's hot rows. Both
// read in the pass's snapshot. internal.DBPersistentRecordRepository
// satisfies it.
type HotLegReader interface {
	QueryHotKeys(ctx context.Context, tx pgx.Tx, req model.HotKeysQuery) ([]model.HotKey, error)
	ReadHotRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]*model.PersistentRecord, error)
}

// SnapshotBeginner opens the pass's snapshot: the engine's pgx pool, or one
// connection. *pgxpool.Pool satisfies it.
type SnapshotBeginner = snapshotBeginner

// WithPgxHotLeg gives the engine what a pgx pass reads PostgreSQL through:
// the pool it opens each pass's REPEATABLE READ READ ONLY snapshot on, and
// the reader of the hot keys and the page's hot rows. Without it every pass
// fails as misconfigured.
func WithPgxHotLeg(pool SnapshotBeginner, reader HotLegReader) EngineOption {
	return func(e *DBFederatedQueryEngine) {
		if pool == nil || reader == nil {
			return
		}
		e.hotLeg = pgxHotLegSnapshots{pool: pool, reader: reader}
	}
}

// hotLegSnapshots opens one pass's snapshot.
type hotLegSnapshots interface {
	begin(ctx context.Context) (hotLegSnapshot, error)
}

// hotLegSnapshot is one pass's open snapshot: every PostgreSQL read of the
// pass goes through it, so the dirty set, the hot keys and the page's hot
// rows agree (design.md §6.1, Consistency).
type hotLegSnapshot interface {
	readChangeLog(ctx context.Context, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error)
	queryHotKeys(ctx context.Context, req model.HotKeysQuery) ([]model.HotKey, error)
	readHotRows(ctx context.Context, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]*model.PersistentRecord, error)
	// commit ends the snapshot. rollback releases it after a failure, and
	// is a no-op after commit.
	commit(ctx context.Context) error
	rollback(ctx context.Context)
}

// pgxHotLegSnapshots is the production snapshot source.
type pgxHotLegSnapshots struct {
	pool   snapshotBeginner
	reader HotLegReader
}

func (s pgxHotLegSnapshots) begin(ctx context.Context) (hotLegSnapshot, error) {
	tx, err := beginReadSnapshot(ctx, s.pool)
	if err != nil {
		return nil, err
	}
	return &pgxHotLegSnapshot{tx: tx, reader: s.reader}, nil
}

type pgxHotLegSnapshot struct {
	tx     pgx.Tx
	reader HotLegReader
}

func (s *pgxHotLegSnapshot) readChangeLog(ctx context.Context, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error) {
	return readChangeLogSnapshot(ctx, s.tx, req)
}

func (s *pgxHotLegSnapshot) queryHotKeys(ctx context.Context, req model.HotKeysQuery) ([]model.HotKey, error) {
	return s.reader.QueryHotKeys(ctx, s.tx, req)
}

func (s *pgxHotLegSnapshot) readHotRows(ctx context.Context, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]*model.PersistentRecord, error) {
	return s.reader.ReadHotRows(ctx, s.tx, tables, schemaID, rowIDs)
}

func (s *pgxHotLegSnapshot) commit(ctx context.Context) error {
	if err := s.tx.Commit(ctx); err != nil {
		return fmt.Errorf("end the pass's snapshot: %w", err)
	}
	return nil
}

// rollback runs on a context that outlives the request's, so a pass whose
// deadline expired still returns its connection to the pool.
func (s *pgxHotLegSnapshot) rollback(ctx context.Context) {
	_ = s.tx.Rollback(context.WithoutCancel(ctx))
}

// stagedQuerier is the optional half of DuckDBQueryExecutor that runs a
// parquet-only statement on a connection it staged the dirty set on. The
// staged temp table is private to that connection, so the statement must run
// there; closing the rows drops the table and returns the connection.
type stagedQuerier interface {
	queryStaged(ctx context.Context, dirtyIDs []uuid.UUID, sqlStr string, args ...any) (duckDBRowsIterator, error)
}

// queryStaged takes a pooled connection, stages dirtyIDs into
// sqlgen.StagedDirtyIDsTable on it (StageDirtySet), and runs sqlStr there.
func (e *DuckDBClientQueryExecutor) queryStaged(ctx context.Context, dirtyIDs []uuid.UUID, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	if e == nil || e.client == nil || e.client.DB == nil {
		return nil, fmt.Errorf("duckdb client not available")
	}
	conn, err := e.client.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("take a duckdb connection for the parquet leg: %w", err)
	}
	if err := StageDirtySet(ctx, conn, dirtyIDs); err != nil {
		_ = releaseStagedConn(ctx, conn)
		return nil, fmt.Errorf("stage the dirty set for the parquet leg: %w", err)
	}
	rows, err := conn.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		_ = releaseStagedConn(ctx, conn)
		return nil, fmt.Errorf("query the parquet leg on its staged connection: %w", err)
	}
	return &stagedRows{Rows: rows, ctx: ctx, conn: conn}, nil
}

// stagedDropTimeout bounds the drop of a staged set on a context the request
// no longer bounds.
const stagedDropTimeout = 5 * time.Second

// releaseStagedConn drops the staged set and returns conn to the pool. The
// drop runs past the request's deadline, under its own bound, so an expired
// request does not leave its set on a pooled connection; the next pass's
// CREATE OR REPLACE would empty it anyway.
func releaseStagedConn(ctx context.Context, conn *sql.Conn) error {
	dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagedDropTimeout)
	defer cancel()
	return errors.Join(DropStagedDirtySet(dropCtx, conn), conn.Close())
}

// stagedRows are the rows of a statement on a staged connection. Close
// closes them, drops the staged set and returns the connection, once.
type stagedRows struct {
	*sql.Rows
	ctx      context.Context
	conn     *sql.Conn
	once     sync.Once
	closeErr error
}

func (r *stagedRows) Close() error {
	r.once.Do(func() {
		r.closeErr = errors.Join(r.Rows.Close(), releaseStagedConn(r.ctx, r.conn))
	})
	return r.closeErr
}

// passCutoffMs is the flush-grace cutoff of a pass (#252): the instant the
// pass resolved its parquet path set, minus FlushVisibilityGraceMs. A
// negative margin reads as 0, the exact anchor: the snapshot argument needs a
// cutoff no later than path resolution, so the widening is never disabled
// (design.md §6.1). The hot-excluded form reads the strict set and ignores
// the cutoff.
func (e *DBFederatedQueryEngine) passCutoffMs(pathsResolvedAtMs int64) int64 {
	return pathsResolvedAtMs - max(0, e.flushVisibilityGraceMs)
}

// logNegativeGrace says once, at construction, that the engine reads a
// negative FlushVisibilityGraceMs as 0.
func (e *DBFederatedQueryEngine) logNegativeGrace() {
	if e.flushVisibilityGraceMs >= 0 {
		return
	}
	e.log().Warn("duckdb.flushVisibilityGraceMs is negative, which the federated engine reads as 0: the pass's snapshot needs the flush-grace widening, so it cannot be disabled",
		zap.Int64("flush_visibility_grace_ms", e.flushVisibilityGraceMs))
}
