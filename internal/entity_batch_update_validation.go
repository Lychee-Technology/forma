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

// validateAtomicUpdateOperations runs every input check an atomic BatchUpdate
// needs before it touches the registry or the repository. It walks the
// operations in list order and reports the first problem it meets, so a
// malformed operation always outranks a duplicate that comes later in the
// list, and a duplicate never masks a missing field on the same operation.
//
// The duplicate check exists because the atomic path reads every base record
// before any write (#458): two operations on one row would both merge onto the
// same base and the later write would silently discard the earlier one while
// both were reported successful. Duplicates are a caller bug; rejecting them
// is observable, coalescing would not be. The best-effort path is unaffected:
// it re-reads before each operation and applies them in order.
func validateAtomicUpdateOperations(ops []forma.EntityOperation) error {
	seen := make(map[batchUpdateTarget]int, len(ops))
	for i, op := range ops {
		if op.SchemaName == "" {
			return forma.InvalidInputf("operation[%d]: schema name is required", i)
		}
		if op.RowID == (uuid.UUID{}) {
			return forma.InvalidInputf("operation[%d]: row id is required for update operation", i)
		}
		if op.Updates == nil {
			return forma.InvalidInputf("operation[%d]: updates are required for update operation", i)
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
