package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// txBoundRepository is the repository a Tx session's CRUD service writes
// through (#648). Each write runs the same body the public method runs inside
// its own transaction (insertRecordInTx, mergeRecordInTx, deleteRecordInTx),
// on the session's transaction instead, so a write through a Tx and the same
// write through EntityManager are one code path. Statement errors are
// classified here, as inWriteTx classifies them, because no inWriteTx wraps
// these calls; the COMMIT is InTx's.
//
// Reads refuse rather than fall back to the pool: a read on the pool would
// miss the session's own uncommitted writes. Tx reads arrive with #650.
type txBoundRepository struct {
	repo *DBPersistentRecordRepository
	tx   pgx.Tx
}

var _ model.PersistentRecordRepository = (*txBoundRepository)(nil)

func (b *txBoundRepository) InsertPersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for insert: %w", err)
	}
	return classifyStatementError(b.repo.insertRecordInTx(ctx, b.tx, tables, record, b.repo.nowMillis()))
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

func (b *txBoundRepository) GetPersistentRecord(context.Context, model.StorageTables, int16, uuid.UUID) (*model.PersistentRecord, error) {
	return nil, errTxReadUnsupported
}

func (b *txBoundRepository) QueryPersistentRecords(context.Context, *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	return nil, errTxReadUnsupported
}

func (b *txBoundRepository) QueryPersistentRecordsByAttrValues(context.Context, model.StorageTables, int16, string, []string, int) (*model.PersistentRecordPage, error) {
	return nil, errTxReadUnsupported
}

var errTxReadUnsupported = fmt.Errorf("reading through a Forma transaction is not supported yet")
