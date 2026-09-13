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
	SchemaName string    `json:"schemaName"`
	RowID      uuid.UUID `json:"rowId"`
}

// EntityOperation represents CRUD operations
type EntityOperation struct {
	EntityIdentifier
	Type    OperationType  `json:"type"`
	Data    map[string]any `json:"data,omitempty"`
	Updates map[string]any `json:"updates,omitempty"`
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
	Code      string          `json:"code"`
	Details   map[string]any  `json:"details,omitempty"`
}
