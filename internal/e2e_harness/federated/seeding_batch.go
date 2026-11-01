package federated

import (
	"context"
	"database/sql"
	"fmt"
)

// sqlExecer is the statement surface hot-record seeding writes through: the
// harness's *sql.DB, where every statement commits on its own, or a *sql.Tx,
// where a batch of records shares one commit.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// hotSeedBatchRecords is how many records one seeding transaction writes.
// Each record is about ten statements (one entity_main row, one eav_data row
// per attribute, one change_log row), so a batch commits a few thousand rows
// at once instead of paying one commit per statement (#781).
const hotSeedBatchRecords = 500

// insertHotRecords writes records into the hot tier in transactions of
// hotSeedBatchRecords records each. Autocommitting every statement made
// seeding cost one commit per row, which on hosts with slow container
// storage used up a whole test's deadline before its first flush (#781).
// Batching changes only when rows become visible: no test reads the hot tier
// while it is being seeded.
func (h *FederatedTestHarness) insertHotRecords(ctx context.Context, records []TestRecord) error {
	for start := 0; start < len(records); start += hotSeedBatchRecords {
		end := min(start+hotSeedBatchRecords, len(records))
		if err := h.insertHotBatch(ctx, records[start:end]); err != nil {
			return fmt.Errorf("seed hot records %d-%d of %d: %w", start, end-1, len(records), err)
		}
	}
	return nil
}

// insertHotBatch writes one batch of records in a single transaction, rolling
// it back whole when any statement fails.
func (h *FederatedTestHarness) insertHotBatch(ctx context.Context, batch []TestRecord) error {
	tx, err := h.PGDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin seeding transaction: %w", err)
	}
	for _, r := range batch {
		if err := h.insertHotRecordWith(ctx, tx, r); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert hot record %s: %w", r.RowID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit seeding transaction: %w", err)
	}
	return nil
}
