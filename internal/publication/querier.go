package publication

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Querier is what the package runs its statements on. Build one with DB for
// a pool's autocommit statements, or with SQLTx or PgxTx for a transaction the
// caller opened. The interface is sealed, so a transition whose statements must
// commit together with the caller's own can demand a Tx and refuse a pool when
// it compiles.
type Querier interface {
	runner() runner
}

// Tx is a transaction the caller opened and alone commits or rolls back.
// Publish, Reset, Activate, HoldLineage, BindStorageTx and both Initialize
// functions take a Tx: their statements and the caller's must commit as one.
// Through a pool every statement commits on its own, so Publish would commit
// the pointer before the history row and the caller's marks, HoldLineage would
// release its lock before the marks it holds the lineage for, and
// BindStorageTx would bind the deployment for a publication that never
// commits.
type Tx interface {
	Querier
	inTx()
}

// DB runs each statement on db in its own autocommit transaction. It suits
// the reads and the single-statement steps of garbage collection, never a
// transition.
func DB(db *sql.DB) Querier { return dbQuerier{sqlRunner{db}} }

// SQLTx runs the statements on tx, a database/sql transaction.
func SQLTx(tx *sql.Tx) Tx { return sqlTx{sqlRunner{tx}} }

// PgxTx runs the statements on tx, a pgx transaction, for the callers that
// hold one already (init-db's registration among them).
func PgxTx(tx pgx.Tx) Tx { return pgxTx{tx} }

// runner is the three calls the package makes, answered alike by both drivers.
// A row that is not there scans as sql.ErrNoRows on either.
type runner interface {
	exec(ctx context.Context, query string, args ...any) (int64, error)
	queryRow(ctx context.Context, query string, args ...any) row
	query(ctx context.Context, query string, args ...any) (rows, error)
}

type row interface {
	Scan(dest ...any) error
}

type rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// sqlConn is what *sql.DB and *sql.Tx share.
type sqlConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type sqlRunner struct{ c sqlConn }

func (s sqlRunner) exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := s.c.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s sqlRunner) queryRow(ctx context.Context, query string, args ...any) row {
	return s.c.QueryRowContext(ctx, query, args...)
}

func (s sqlRunner) query(ctx context.Context, query string, args ...any) (rows, error) {
	r, err := s.c.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return sqlRows{r}, nil
}

type sqlRows struct{ *sql.Rows }

func (r sqlRows) Close() { _ = r.Rows.Close() }

type dbQuerier struct{ sqlRunner }

func (q dbQuerier) runner() runner { return q.sqlRunner }

type sqlTx struct{ sqlRunner }

func (q sqlTx) runner() runner { return q.sqlRunner }
func (sqlTx) inTx()            {}

type pgxTx struct{ tx pgx.Tx }

func (p pgxTx) runner() runner { return p }
func (pgxTx) inTx()            {}

func (p pgxTx) exec(ctx context.Context, query string, args ...any) (int64, error) {
	tag, err := p.tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (p pgxTx) queryRow(ctx context.Context, query string, args ...any) row {
	return pgxRow{p.tx.QueryRow(ctx, query, args...)}
}

func (p pgxTx) query(ctx context.Context, query string, args ...any) (rows, error) {
	return p.tx.Query(ctx, query, args...)
}

type pgxRow struct{ r pgx.Row }

func (r pgxRow) Scan(dest ...any) error {
	err := r.r.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return sql.ErrNoRows
	}
	return err
}
