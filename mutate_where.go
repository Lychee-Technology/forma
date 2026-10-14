package forma

import "github.com/google/uuid"

// The bounds of MutateWhereRequest.MaxAffected (#652).
const (
	// DefaultMaxAffected is the MaxAffected a request that leaves it zero gets.
	DefaultMaxAffected = 1000
	// MaxMaxAffected is the largest MaxAffected a request may set.
	MaxMaxAffected = 10000
)

// MutateWhereRequest is a predicate mutation: Tx.UpdateWhere or
// Tx.DeleteWhere over the rows of one schema that match Condition.
//
// The call takes the candidates, the matching rows in Sort order with the
// row ID ascending as the final tiebreak, as the Tx sees them: the latest
// committed hot-tier state plus the Tx's own writes. It locks every
// candidate, in row ID order whatever Sort says, then re-evaluates Condition
// on each locked row and mutates exactly the candidates that still match. A
// candidate that stopped matching or was deleted before its lock was granted
// is dropped and not replaced, so a call with Limit L can affect fewer than L
// rows. A row that starts matching after the candidates were taken is not
// affected. See docs/transactions.md.
type MutateWhereRequest struct {
	SchemaName string
	// Condition selects the rows. It is required, and an empty and or or
	// anywhere in it is ErrInvalidInput: there is no unconditioned bulk
	// mutation. It tests schema attributes only, as a precondition does.
	Condition Condition
	// Sort orders the candidates; Limit takes the first ones in that order.
	// The row ID ascending is always the last key.
	Sort []OrderBy
	// Limit caps the candidates at the first Limit rows in Sort order. Zero
	// takes every matching row, and a match of more than MaxAffected rows is
	// then ErrInvalidInput, which writes nothing and leaves the Tx usable.
	Limit int
	// MaxAffected bounds the rows one call can affect: 1 to MaxMaxAffected,
	// with zero meaning DefaultMaxAffected. Limit must not exceed it.
	MaxAffected int
	// Updates and UpdateOps are what UpdateWhere applies to each row, as
	// Update applies them (#649). UpdateWhere needs at least one of them;
	// DeleteWhere accepts neither.
	Updates   map[string]any
	UpdateOps []UpdateOp
	// Returning asks for the affected rows' records in MutateWhereResult.
	Returning bool
}

// MutateWhereResult answers a predicate mutation.
type MutateWhereResult struct {
	// RowIDs lists the affected rows in candidate order. It is empty, not an
	// error, when no row matched.
	RowIDs []uuid.UUID
	// Records holds, when the request set Returning, one record per affected
	// row in RowIDs order: for UpdateWhere the row as stored after the write,
	// for DeleteWhere the row as it was just before its deletion. It is nil
	// without Returning.
	Records []*DataRecord
}
