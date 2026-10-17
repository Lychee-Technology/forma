package forma

// UpsertAction says what Tx.Upsert does when its create conflicts with an
// existing row on the request's target (#651).
type UpsertAction string

const (
	// UpsertDoNothing leaves the existing row as stored and answers it.
	UpsertDoNothing UpsertAction = "do_nothing"
	// UpsertUpdate applies the request's Updates and UpdateOps to the
	// existing row when its Preconditions hold, as Update would.
	UpsertUpdate UpsertAction = "update"
)

// ConflictTarget names the identity on which Tx.Upsert resolves a conflict
// instead of failing. Exactly one field is set.
type ConflictTarget struct {
	// RowID targets the create's row ID, which must then be non-nil.
	RowID bool
	// Key targets the declared unique key of this name.
	Key string
}

// UpsertRequest is a create that resolves a conflict on Target by Action:
// when a live row holds the target, the create is not tried, and the action
// runs on that row instead. See docs/unique-keys.md.
type UpsertRequest struct {
	// Create is the row to create: SchemaName and Data are required, RowID
	// follows Create's rules, and Type may be empty or OperationCreate.
	Create EntityOperation
	Target ConflictTarget
	Action UpsertAction
	// Preconditions, Updates and UpdateOps apply to the existing row, with
	// Update's rules (see docs/conditional-writes.md), and only with
	// UpsertUpdate, which needs Updates or UpdateOps.
	Preconditions Condition
	Updates       map[string]any
	UpdateOps     []UpdateOp
}

// UpsertOutcome says which branch of an Upsert ran.
type UpsertOutcome string

const (
	// UpsertCreated: no row held the target, and the create wrote a row.
	UpsertCreated UpsertOutcome = "created"
	// UpsertExisting: a row held the target and the action was do_nothing.
	UpsertExisting UpsertOutcome = "existing"
	// UpsertUpdated: a row held the target, its preconditions held, and the
	// update was applied.
	UpsertUpdated UpsertOutcome = "updated"
)

// UpsertResult is a successful Upsert's outcome and the row as stored after
// it.
type UpsertResult struct {
	Outcome UpsertOutcome
	Record  *DataRecord
}
