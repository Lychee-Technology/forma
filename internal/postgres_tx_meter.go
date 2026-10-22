package internal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// txMeter accounts for the statements one Tx session sends (#648). Two rules
// read it:
//
//   - Poisoning: an operation's error is an expected outcome only if none of
//     its statements failed, and a pre-write ErrInvalidInput only if it sent
//     none. A failed statement means PostgreSQL has aborted the transaction,
//     or the connection is gone, so nothing after it can commit.
//   - The physical-row limit: every statement's command tag reports the rows
//     it modified, and the statement that takes the count past maxRows fails.
//     The crossing statement has therefore run, but its failure poisons the
//     Tx: no statement runs after it and nothing the Tx wrote commits (#648
//     rule 19, as amended in #673's review). Counting here, where every
//     statement passes, covers every write path, a later one included,
//     without any of them predicting its rows; a pre-check would need each
//     path to declare every statement's rows, and a delete an extra count of
//     its EAV rows. What this cannot bound is the crossing statement itself,
//     so a write path sends only statements bounded by one entity's rows, as
//     Create, Update and Delete do: a set-based UPDATE or DELETE must cap its
//     own rows before it is sent.
//
// It is only touched by the goroutine holding the session's busy lock.
type txMeter struct {
	statements int
	failures   int
	rows       int64
	maxRows    int64
	// fault, set only by tests, runs after each statement the server
	// accepted; a non-nil error fails that statement the way a storage error
	// would.
	fault func(sql string) error
}

// fail records a failed statement and hands its error back.
func (m *txMeter) fail(err error) error {
	m.failures++
	return err
}

// accept records a statement the server completed, which modified rows
// rows. A SELECT's tag counts the rows it returned, not rows it modified, so
// callers pass zero for one.
func (m *txMeter) accept(sql string, rows int64) error {
	m.rows += rows
	if m.rows > m.maxRows {
		return m.fail(forma.InvalidInputf(
			"the transaction would modify more than its limit of %d physical rows "+
				"(entity_main, EAV, change_log, unique-key and row-guard rows); split the work across transactions or raise the limit with WithTxMaxPhysicalRows",
			m.maxRows))
	}
	if m.fault != nil {
		if err := m.fault(sql); err != nil {
			return m.fail(err)
		}
	}
	return nil
}

// modifiedRows is the row count a command tag reports for modified rows.
func modifiedRows(tag pgconn.CommandTag) int64 {
	if tag.Select() {
		return 0
	}
	return tag.RowsAffected()
}

// meteredTx is the pgx.Tx a session's writes run on: the transaction itself,
// with every statement method routed through the meter. SendBatch is refused
// rather than passed through, because a batch's statements would bypass the
// meter and with it the row limit and the poisoning rule.
type meteredTx struct {
	pgx.Tx
	meter *txMeter
	// guards holds the guard rows the transaction has written, by guard
	// table (see guardsWritten).
	guards map[string]map[model.PersistentRecordKey]struct{}
}

// guardsWritten answers the set of table's guard rows the transaction has
// written, which writeGuardRows reads and adds to. A Tx's operations each
// lock their rows, so a claim or a predicate mutation, which locks its
// candidates and then writes each through Update, and an assertion followed
// by an update would otherwise write one guard row twice. The first write
// already decides the conflict, and holds until the transaction ends: a Tx
// has no savepoints, and a failed statement poisons it. A second write would
// only cost a statement and a second count against the physical-row limit.
func (t *meteredTx) guardsWritten(table string) map[model.PersistentRecordKey]struct{} {
	if t.guards == nil {
		t.guards = make(map[string]map[model.PersistentRecordKey]struct{})
	}
	written, ok := t.guards[table]
	if !ok {
		written = make(map[model.PersistentRecordKey]struct{})
		t.guards[table] = written
	}
	return written
}

func (t *meteredTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.meter.statements++
	tag, err := t.Tx.Exec(ctx, sql, args...)
	if err != nil {
		return tag, t.meter.fail(err)
	}
	return tag, t.meter.accept(sql, modifiedRows(tag))
}

func (t *meteredTx) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error) {
	t.meter.statements++
	n, err := t.Tx.CopyFrom(ctx, table, columns, src)
	if err != nil {
		return n, t.meter.fail(err)
	}
	return n, t.meter.accept("COPY "+table.Sanitize(), n)
}

func (t *meteredTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.meter.statements++
	rows, err := t.Tx.Query(ctx, sql, args...)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, t.meter.fail(err)
	}
	return &meteredRows{Rows: rows, meter: t.meter, sql: sql}, nil
}

// QueryRow runs through Query so that a DML statement with RETURNING (the
// main-table update and delete) reports its command tag to the meter; a
// pgx.Row exposes none.
func (t *meteredTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := t.Query(ctx, sql, args...)
	if err != nil {
		return &meteredRow{err: err}
	}
	return &meteredRow{rows: rows.(*meteredRows)}
}

func (t *meteredTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	t.meter.statements++
	return &refusedBatch{err: t.meter.fail(fmt.Errorf("a batch inside a Forma transaction bypasses its statement meter and is not supported"))}
}

// meteredRows settles its statement with the meter once, when the rows are
// exhausted or closed: only then does pgx know the command tag and whether
// the statement failed.
type meteredRows struct {
	pgx.Rows
	meter   *txMeter
	sql     string
	settled bool
	err     error
}

func (r *meteredRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.settle()
	return false
}

func (r *meteredRows) Close() {
	r.Rows.Close()
	r.settle()
}

func (r *meteredRows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Rows.Err()
}

func (r *meteredRows) settle() {
	if r.settled {
		return
	}
	r.settled = true
	if err := r.Rows.Err(); err != nil {
		r.err = r.meter.fail(err)
		return
	}
	r.err = r.meter.accept(r.sql, modifiedRows(r.Rows.CommandTag()))
}

// meteredRow is pgx's QueryRow contract over meteredRows: ErrNoRows when the
// statement returned nothing, otherwise the first row, with the rows closed
// before Scan returns.
type meteredRow struct {
	rows *meteredRows
	err  error
}

func (r *meteredRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	scanErr := r.rows.Scan(dest...)
	r.rows.Close()
	if err := r.rows.Err(); err != nil {
		return err
	}
	return scanErr
}

// refusedBatch answers every result of a refused SendBatch with its error.
type refusedBatch struct{ err error }

func (b *refusedBatch) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, b.err }
func (b *refusedBatch) Query() (pgx.Rows, error)         { return nil, b.err }
func (b *refusedBatch) QueryRow() pgx.Row                { return &meteredRow{err: b.err} }
func (b *refusedBatch) Close() error                     { return b.err }
