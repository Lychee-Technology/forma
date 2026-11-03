//go:build dsqlspike

// Package spikefix is SYNTHETIC code for the DSQL typed call-site spike's
// attribution experiments (#740). It is not forma code and compiles only
// with -tags dsqlspike.
package spikefix

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
)

// DirectExec sends a literal through pgx.
func DirectExec(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "CREATE TABLE direct (id BIGINT PRIMARY KEY)")
	return err
}

// TwoCalls sends two statements from one function.
func TwoCalls(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SELECT 'two-first'"); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "SELECT 'two-second'")
	return err
}

// OneLine sends two statements from one source line, in one gofmt-stable
// statement.
func OneLine(ctx context.Context, tx pgx.Tx) (pgx.Row, pgx.Row) {
	a, b := tx.QueryRow(ctx, "SELECT 'line-a'"), tx.QueryRow(ctx, "SELECT 'line-b'")
	return a, b
}

func execSQL(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

// WrapperCallerA and WrapperCallerB send through a verbatim wrapper.
func WrapperCallerA(ctx context.Context, tx pgx.Tx) error { return execSQL(ctx, tx, "SELECT 'wrap-a'") }

// WrapperCallerB is the wrapper's second caller.
func WrapperCallerB(ctx context.Context, tx pgx.Tx) error { return execSQL(ctx, tx, "SELECT 'wrap-b'") }

func inner(ctx context.Context, tx pgx.Tx, sql string) error { return execSQL(ctx, tx, sql) }

func outer(ctx context.Context, tx pgx.Tx, sql string) error { return inner(ctx, tx, sql) }

// NestedCaller sends through two nested wrappers.
func NestedCaller(ctx context.Context, tx pgx.Tx) error { return outer(ctx, tx, "SELECT 'nested'") }

// MultiLine spreads one call over several lines.
func MultiLine(ctx context.Context, tx pgx.Tx, id int) error {
	_, err := tx.Exec(
		ctx,
		"SELECT 'multi' WHERE $1 > 0",
		id,
	)
	return err
}

func buildDDL(table string) string { return "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY)" }

// UsesBuilder executes what a pure builder returns.
func UsesBuilder(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, buildDDL("built"))
	return err
}

// Batched queues two statements and sends them as a batch.
func Batched(ctx context.Context, tx pgx.Tx) error {
	b := &pgx.Batch{}
	b.Queue("SELECT 'batch-1'")
	b.Queue("SELECT 'batch-2'")
	return tx.SendBatch(ctx, b).Close()
}

// BeginWithOptions lets pgx write a BEGIN for its options.
func BeginWithOptions(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// InClosure executes inside a pgx.BeginFunc closure.
func InClosure(ctx context.Context, conn *pgx.Conn) error {
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SELECT 'closure'")
		return err
	})
}

// SQLDirect sends through database/sql.
func SQLDirect(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SELECT 'database-sql'")
	return err
}
