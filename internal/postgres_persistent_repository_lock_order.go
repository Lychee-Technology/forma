package internal

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/jackc/pgx/v5"
)

// sortedRowKeys returns keys in ascending (schemaID, rowID) order, rowID
// compared byte-wise. Every batch writer takes its per-row advisory locks in
// this order (#554): two overlapping batches of any kind — update/update,
// update/delete, insert/delete on a recreate — then request the shared locks
// in the same sequence and cannot invert, so the batch-vs-batch deadlock
// class disappears instead of being detected by PostgreSQL after the fact.
//
// The sort key is the tuple, not rowVersionLockKey: the FNV hash would order
// deterministically too, but its order is unreadable in tests and logs, and
// a change to the hash would silently reorder the locks.
//
// The input is copied, not sorted in place: callers that answer per-row
// results positionally still write in input order.
func sortedRowKeys(keys []model.PersistentRecordKey) []model.PersistentRecordKey {
	sorted := slices.Clone(keys)
	slices.SortFunc(sorted, func(a, b model.PersistentRecordKey) int {
		if c := cmp.Compare(a.SchemaID, b.SchemaID); c != 0 {
			return c
		}
		return bytes.Compare(a.RowID[:], b.RowID[:])
	})
	return sorted
}

// lockRowKeys is the lock pass shared by the batch writers: it takes
// lockRowVersion for every key in sortedRowKeys order. A duplicate key is
// harmless — pg_advisory_xact_lock is re-entrant within a transaction — so
// no dedup happens here.
func lockRowKeys(ctx context.Context, tx pgx.Tx, keys []model.PersistentRecordKey) error {
	for _, key := range sortedRowKeys(keys) {
		if err := lockRowVersion(ctx, tx, key.SchemaID, key.RowID); err != nil {
			return fmt.Errorf("lock row version for %d/%s: %w", key.SchemaID, key.RowID, err)
		}
	}
	return nil
}
