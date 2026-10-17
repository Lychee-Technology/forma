package internal

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rowguard"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// rowLocking is the backend half of the per-row write lock (#633): how a
// write keeps every other writer of the same (schema_id, row_id) out of its
// critical section. Every write path, the Tx's included, reaches the engine
// only through it, so the two engines differ in this file and nowhere else on
// the write path.
//
// Both implementations keep the invariants the lock exists for: a recreate of
// a reused row ID stamps a version above every tombstone of the row (#274),
// and an update's read, merge and write are one critical section (#457). They
// keep them differently. PostgreSQL makes the second writer wait and then
// read the first one's commit. Aurora DSQL lets both run and aborts one of
// them with 40001 at its write or its COMMIT, which inWriteTx and InTx report
// as forma.ErrRetryableConflict.
type rowLocking interface {
	// writeTxOptions answers how a write transaction begins. readCommitted
	// says the write reads rows after taking their locks and must see the
	// commit of the previous holder (#457), which on PostgreSQL needs READ
	// COMMITTED whatever the session's default.
	writeTxOptions(readCommitted bool) pgx.TxOptions
	// lockRow takes the write lock of one row until the transaction ends.
	lockRow(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error
	// lockRows takes the write lock of every key until the transaction ends,
	// in sortedRowKeys order (#554).
	lockRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, keys []model.PersistentRecordKey) error
	// tryLockRows takes, without waiting, the write lock of each of rowIDs,
	// and answers the rows it got in rowIDs order (#653).
	tryLockRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]uuid.UUID, error)
	// recheckClause is the locking clause of a statement that re-reads rows
	// whose write locks the transaction holds: skipHeld skips a row another
	// transaction holds instead of waiting for it.
	recheckClause(skipHeld bool) string
}

// rowLockingFor answers the row locking of engine.
func rowLockingFor(engine forma.DatabaseEngine) (rowLocking, error) {
	switch engine.Resolved() {
	case forma.EnginePostgreSQL:
		return advisoryLocking{}, nil
	case forma.EngineAuroraDSQL:
		return guardRowLocking{}, nil
	}
	return nil, engine.Validate()
}

// advisoryLocking is the PostgreSQL engine's row locking: the per-row
// transaction advisory lock (lockRowVersion), READ COMMITTED wherever a write
// reads under its lock, and a row lock on every re-check.
type advisoryLocking struct{}

func (advisoryLocking) writeTxOptions(readCommitted bool) pgx.TxOptions {
	if readCommitted {
		return pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	}
	return pgx.TxOptions{}
}

func (advisoryLocking) lockRow(ctx context.Context, tx pgx.Tx, _ model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	return lockRowVersion(ctx, tx, schemaID, rowID)
}

// lockRows takes lockRowVersion for every key in sortedRowKeys order. A
// duplicate key is harmless — pg_advisory_xact_lock is re-entrant within a
// transaction — so no dedup happens here.
func (advisoryLocking) lockRows(ctx context.Context, tx pgx.Tx, _ model.StorageTables, keys []model.PersistentRecordKey) error {
	return lockRowKeys(ctx, tx, keys)
}

func (advisoryLocking) tryLockRows(ctx context.Context, tx pgx.Tx, _ model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]uuid.UUID, error) {
	return tryLockRowsInTx(ctx, tx, schemaID, rowIDs)
}

func (advisoryLocking) recheckClause(skipHeld bool) string {
	if skipHeld {
		return "FOR UPDATE OF m SKIP LOCKED"
	}
	return "FOR UPDATE OF m"
}

// guardRowLocking is the Aurora DSQL engine's row locking. DSQL has no
// advisory locks, runs every transaction at REPEATABLE READ and accepts no
// other level, and never makes a statement wait. A lock is therefore a write
// of the row's guard row (internal/rowguard): of two transactions that write
// one guard row, the one that commits second fails with 40001, whatever else
// each of them read or wrote. A guard row exists for a row ID that has no
// entity row, so a create conflicts with the delete or the missing-row
// assertion it races, which the entity_main row alone could not give.
//
// A transaction's snapshot predates every write it races, so nothing here
// re-reads under a lock: the write that read a stale snapshot is the one DSQL
// aborts. Statements carry no locking clause, isolation level or advisory
// function, the PostgreSQL features DSQL lacks or restricts.
//
// On PostgreSQL the same SQL is a row lock: run under REPEATABLE READ, the
// second writer waits for the first and then fails with 40001, the outcome
// DSQL reaches without the wait. That is how CI exercises this engine.
type guardRowLocking struct{}

// guardRowsPerStatement bounds one guard upsert at two bind parameters a row.
const guardRowsPerStatement = 1000

func (guardRowLocking) writeTxOptions(bool) pgx.TxOptions { return pgx.TxOptions{} }

func (l guardRowLocking) lockRow(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	if err := writeGuardRows(ctx, tx, tables, []model.PersistentRecordKey{{SchemaID: schemaID, RowID: rowID}}); err != nil {
		return fmt.Errorf("write row guard for %s: %w", rowID, err)
	}
	return nil
}

func (guardRowLocking) lockRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, keys []model.PersistentRecordKey) error {
	if err := writeGuardRows(ctx, tx, tables, keys); err != nil {
		return fmt.Errorf("write row guards for %d rows: %w", len(keys), err)
	}
	return nil
}

// tryLockRows writes every candidate's guard row and answers them all: DSQL
// cannot tell that another transaction holds a row, so nothing is skipped.
// Of two claimers that take one row, one fails at COMMIT with 40001
// (docs/claims.md, "Aurora DSQL").
func (l guardRowLocking) tryLockRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]uuid.UUID, error) {
	keys := make([]model.PersistentRecordKey, len(rowIDs))
	for i, id := range rowIDs {
		keys[i] = model.PersistentRecordKey{SchemaID: schemaID, RowID: id}
	}
	if err := l.lockRows(ctx, tx, tables, keys); err != nil {
		return nil, err
	}
	return slices.Clone(rowIDs), nil
}

func (guardRowLocking) recheckClause(bool) string { return "" }

// writeGuardRows upserts the guard row of every distinct key, in
// sortedRowKeys order and at most guardRowsPerStatement a statement. One
// upsert may not name a row twice, so duplicates are dropped; a second write
// of a row the transaction already wrote changes nothing about the conflict.
func writeGuardRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, keys []model.PersistentRecordKey) error {
	if tables.EntityMain == "" {
		return fmt.Errorf("entity main table name is required to name its row guards")
	}
	sorted := slices.Compact(sortedRowKeys(keys))
	table := sanitizeIdentifier(rowguard.Table(tables.EntityMain))
	for chunk := range slices.Chunk(sorted, guardRowsPerStatement) {
		args := make([]any, 0, 2*len(chunk))
		for _, key := range chunk {
			args = append(args, key.SchemaID, key.RowID)
		}
		if _, err := tx.Exec(ctx, guardUpsertSQL(table, len(chunk)), args...); err != nil {
			return fmt.Errorf("upsert %d row guards: %w", len(chunk), err)
		}
	}
	return nil
}

// guardUpsertSQL is the upsert of n guard rows into table, a sanitized
// identifier. A new row starts at generation 1 and each later write adds one.
func guardUpsertSQL(table string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s AS g (ltbase_schema_id, ltbase_row_id, generation) VALUES ", table)
	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "($%d, $%d, 1)", 2*i+1, 2*i+2)
	}
	b.WriteString(" ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET generation = g.generation + 1")
	return b.String()
}
