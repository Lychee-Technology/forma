package forma

import "github.com/google/uuid"

// Batch write contracts shared by EntityBatchOperator implementations and
// the HTTP API. Kept apart from types.go so the file-size gate holds.

// OperationType represents CRUD operations
type OperationType string

const (
	OperationCreate OperationType = "create"
	OperationRead   OperationType = "read"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
	OperationQuery  OperationType = "query"
)

// EntityIdentifier identifies an entity for operations
type EntityIdentifier struct {
	SchemaName string `json:"schemaName"`
	// RowID names the row. On a create (Create, BatchCreate, Tx.Create),
	// uuid.Nil asks Forma to assign a UUIDv7, and any other value becomes the
	// new row's ID; a live row of the schema with that ID makes the create
	// ErrUniqueConflict, writing nothing (#651). See docs/unique-keys.md.
	RowID uuid.UUID `json:"rowId"`
}

// EntityOperation represents CRUD operations
type EntityOperation struct {
	EntityIdentifier
	Type    OperationType  `json:"type"`
	Data    map[string]any `json:"data,omitempty"`
	Updates map[string]any `json:"updates,omitempty"`

	// Preconditions must hold on the row, evaluated after the row's lock is
	// granted and in the same transaction as the write, or the operation
	// writes nothing: a row that does not satisfy it is ErrPreconditionFailed,
	// a missing row ErrNotFound. It is a condition in Query's DSL over schema
	// attributes, and it holds exactly when a hot-tier Query for the row with
	// the same condition would return it. Update and Delete only (EntityManager
	// and Tx); nil means none. Batches refuse it. See
	// docs/conditional-writes.md.
	Preconditions Condition `json:"-"`
	// UpdateOps compute values from the locked row or the transaction clock
	// and apply after Updates; an attribute takes one or the other. Update
	// only, where they may stand in for Updates. Batches refuse them.
	UpdateOps []UpdateOp `json:"-"`
}

// BatchOperation represents batch entity operations.
//
// The two execution modes treat a row that appears more than once in
// Operations differently:
//
//   - Best-effort (Atomic == false) applies operations in list order, each one
//     re-reading the row first, so a later update on the same row merges onto
//     the earlier one. The final state is last-wins by execution order.
//   - Atomic (Atomic == true) rejects a batch that names the same
//     (schema, row_id) twice up front with an ErrInvalidInput carrier, before
//     any read or write. The atomic path merges every operation onto the base
//     it read before writing, so duplicates would otherwise silently discard
//     all but the last one while reporting every operation successful.
type BatchOperation struct {
	Operations []EntityOperation `json:"operations"`
	// Atomic requests all-or-nothing execution; may be rejected when
	// unsupported. See the type comment for how duplicates are treated.
	Atomic bool `json:"atomic"`
}

// BatchResult represents results from batch operations
type BatchResult struct {
	Successful []*DataRecord    `json:"successful"`
	Failed     []OperationError `json:"failed"`
	TotalCount int              `json:"totalCount"`
	Duration   int64            `json:"duration"` // microseconds
}

// OperationError represents an error for a specific operation
type OperationError struct {
	Operation EntityOperation `json:"operation"`
	Error     string          `json:"error"`
	// Code classifies the failure. A best-effort batch reports one of the
	// four storage outcomes (#645) when the operation hit one:
	// UNIQUE_CONFLICT (ErrUniqueConflict), PRECONDITION_FAILED
	// (ErrPreconditionFailed), RETRYABLE_CONFLICT (ErrRetryableConflict:
	// rolled back, safe to run again) or COMMIT_OUTCOME_UNKNOWN
	// (ErrCommitOutcomeUnknown: may have committed, never retry blindly).
	// Any other failure reports its operation's code: CREATE_FAILED,
	// UPDATE_FAILED or DELETE_FAILED.
	Code string `json:"code"`
	// ErrorID correlates this failure with the server log line that records
	// its full error: a UUID the caller can quote to an operator. It has the
	// same name and shape as the HTTP error body's error_id (#398).
	//
	// A failure produced by a best-effort batch always carries one, whether
	// Error holds a published message or "internal error", and no two failures
	// share one. The id is written as error_id on that failure's Warn log line,
	// next to the full error. Forma writes the line through the process-global
	// zap logger. Whether the line is kept is up to that logger's level,
	// sampling, and sinks, which belong to the embedding process.
	//
	// Empty only on a value Forma did not produce, such as one built by hand.
	ErrorID string         `json:"error_id,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}
