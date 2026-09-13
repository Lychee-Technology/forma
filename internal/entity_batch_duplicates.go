package internal

import (
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// batchUpdateTarget identifies the row an update operation addresses. The
// schema name is part of the key so a cross-schema batch that happens to reuse
// a UUID is not mistaken for a duplicate.
type batchUpdateTarget struct {
	schemaName string
	rowID      uuid.UUID
}

// rejectDuplicateAtomicUpdateTargets refuses an atomic batch that addresses the
// same row twice (#458). The atomic path reads every base record before any
// write, so two operations on one row would both merge onto the same base and
// the later write would silently discard the earlier one while both were
// reported successful. Duplicates are a caller bug; rejecting them is
// observable, coalescing would not be. The best-effort path is unaffected: it
// re-reads before each operation and applies them in order.
func rejectDuplicateAtomicUpdateTargets(ops []forma.EntityOperation) error {
	seen := make(map[batchUpdateTarget]int, len(ops))
	for i, op := range ops {
		if op.SchemaName == "" || op.RowID == (uuid.UUID{}) {
			continue // batchUpdateAtomic's per-operation validation reports the missing field
		}
		key := batchUpdateTarget{schemaName: op.SchemaName, rowID: op.RowID}
		if first, dup := seen[key]; dup {
			return forma.InvalidInputf(
				"operation[%d]: duplicate row id %s for schema %s in atomic batch (already targeted by operation[%d])",
				i, op.RowID, op.SchemaName, first)
		}
		seen[key] = i
	}
	return nil
}
