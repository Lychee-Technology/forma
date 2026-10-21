package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// txBoundRepository is the repository a Tx session's services run on (#648,
// #650). Each write runs the same body the public method runs inside its own
// transaction (insertRecordInTx, mergeRecordInTx, deleteRecordInTx), on the
// session's transaction instead, so a write through a Tx and the same write
// through EntityManager are one code path. Each read runs the read path's own
// code on reader, the manager's repository with every statement sent on the
// transaction, so it sees the session's own uncommitted writes and never
// reaches for the pool. Statement errors are classified here, as inWriteTx
// classifies them, because no inWriteTx wraps these calls; the COMMIT is
// InTx's.
type txBoundRepository struct {
	repo   *DBPersistentRecordRepository
	tx     pgx.Tx
	reader *DBPersistentRecordRepository
}

func newTxBoundRepository(repo *DBPersistentRecordRepository, tx pgx.Tx) *txBoundRepository {
	reader := *repo
	reader.pool = txStatementPool{tx: tx}
	return &txBoundRepository{repo: repo, tx: tx, reader: &reader}
}

// txStatementPool is the statement surface of a Tx's reader: every statement
// goes to the transaction. BeginTx refuses, because a read path that opened a
// transaction of its own would leave the Tx's connection and miss its writes.
type txStatementPool struct{ tx pgx.Tx }

func (p txStatementPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.tx.Query(ctx, sql, args...)
}

func (p txStatementPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.tx.QueryRow(ctx, sql, args...)
}

func (p txStatementPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("a read inside a Forma transaction cannot open a transaction of its own")
}

var _ model.PersistentRecordRepository = (*txBoundRepository)(nil)

// uniqueKeyOwner answers the row that holds value of key keyID of schemaID,
// as the transaction sees it.
func (b *txBoundRepository) uniqueKeyOwner(ctx context.Context, tables model.StorageTables, schemaID, keyID int16, value string) (uuid.UUID, bool, error) {
	owner, found, err := uniqueKeyOwner(ctx, b.tx, tables, schemaID, keyID, value)
	if err != nil {
		return uuid.Nil, false, classifyStatementError(err)
	}
	return owner, found, nil
}

func (b *txBoundRepository) InsertPersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	return b.insertPersistentRecordAt(ctx, tables, record, b.repo.nowMillis())
}

// insertPersistentRecordAt is InsertPersistentRecord stamped with now, a read
// of the repository's clock the caller made earlier: Tx.Upsert looks its
// target up before it inserts, and a key over ltbase_created_at must be
// looked up and reserved under the one stamp the row is stored with (#651).
func (b *txBoundRepository) insertPersistentRecordAt(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord, now int64) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for insert: %w", err)
	}
	return classifyStatementError(b.repo.insertRecordInTx(ctx, b.tx, tables, record, now))
}

func (b *txBoundRepository) MergePersistentRecord(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
	guard model.RowGuard,
	merge model.PersistentRecordMerge,
) (*model.PersistentRecord, error) {
	if merge == nil {
		return nil, fmt.Errorf("merge function cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return nil, fmt.Errorf("validate tables for update: %w", err)
	}
	rendered, err := b.repo.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return nil, fmt.Errorf("prepare update of %s: %w", rowID, err)
	}
	stored, err := b.repo.mergeRecordInTx(ctx, b.tx, tables, schemaID, rowID, rendered, merge)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return stored, nil
}

func (b *txBoundRepository) DeletePersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard model.RowGuard) error {
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for delete: %w", err)
	}
	rendered, err := b.repo.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return fmt.Errorf("prepare delete of %s: %w", rowID, err)
	}
	return classifyStatementError(b.repo.deleteRecordInTx(ctx, b.tx, tables, schemaID, rowID, rendered))
}

func (b *txBoundRepository) GetPersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) (*model.PersistentRecord, error) {
	record, err := b.reader.GetPersistentRecord(ctx, tables, schemaID, rowID)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return record, nil
}

func (b *txBoundRepository) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	page, err := b.reader.QueryPersistentRecords(ctx, query)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return page, nil
}

func (b *txBoundRepository) QueryPersistentRecordsByAttrValues(ctx context.Context, tables model.StorageTables, schemaID int16, attr string, values []string, limit int) (*model.PersistentRecordPage, error) {
	page, err := b.reader.QueryPersistentRecordsByAttrValues(ctx, tables, schemaID, attr, values, limit)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return page, nil
}

// LockPersistentRecord takes the write lock of (schemaID, rowID) until the
// transaction ends and checks guard on the locked row, writing nothing: the
// lock behind Tx.AssertRow and Tx.Get with ForUpdate (#650). guard renders
// before any statement is sent, so a malformed precondition fails without
// SQL. See lockRowInTx.
func (b *txBoundRepository) LockPersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard model.RowGuard) error {
	if err := validateTables(tables); err != nil {
		return fmt.Errorf("validate tables for row lock: %w", err)
	}
	rendered, err := b.repo.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return fmt.Errorf("prepare lock of %s: %w", rowID, err)
	}
	return classifyStatementError(lockRowInTx(ctx, b.tx, b.repo.locks, tables, schemaID, rowID, rendered))
}
