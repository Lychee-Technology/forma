package forma

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/txseal"
)

// Transactor runs functions inside one hot-tier PostgreSQL transaction
// (#648). Build one with factory.NewTransactor. See docs/transactions.md.
type Transactor interface {
	// InTx begins one READ COMMITTED transaction and calls fn exactly once
	// with a Tx bound to it.
	//
	//   - fn returns nil: the transaction commits, unless the Tx was poisoned
	//     or ctx ended first. InTx answers nil or the classified COMMIT error
	//     (an unanswered COMMIT is ErrCommitOutcomeUnknown).
	//   - fn returns an error: the transaction rolls back and InTx answers that
	//     error, wrapped so errors.Is and errors.As still match it.
	//   - fn panics or calls runtime.Goexit: the transaction rolls back, and
	//     the panic continues with its original value.
	//   - ctx is cancelled or its deadline passes before COMMIT: the
	//     transaction rolls back and the error matches context.Canceled or
	//     context.DeadlineExceeded.
	//
	// An error poisons the Tx unless it is an expected outcome: ErrNotFound,
	// ErrPreconditionFailed or ErrUniqueConflict raised without a failed SQL
	// statement, or ErrInvalidInput raised before the operation sent any SQL.
	// Every later Tx method then answers ErrTxAborted wrapping the cause, and a
	// poisoned Tx never commits: InTx rolls back even when fn returns nil.
	//
	// InTx never retries. When IsRetryable reports true for its error, run the
	// whole of fn again. Calling InTx with a context derived from the one it
	// passed to fn answers ErrInvalidInput without starting a transaction:
	// there are no nested transactions or savepoints.
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error, opts ...TxOption) error
}

// Tx is one InTx call's handle on its transaction. It is valid only inside the
// fn passed to InTx and only from one goroutine at a time; after InTx returns,
// every method answers ErrTxDone.
//
// Create, Update and Delete take the same *EntityOperation as EntityManager's
// methods and apply the same validation, computed-field stripping, per-row
// lock, merge semantics, version stamping and change_log write, Preconditions
// and UpdateOps included: ErrPreconditionFailed and ErrNotFound leave the Tx
// usable, and every server_time and $now in it name the transaction's one
// instant. A later operation sees the writes of earlier ones, and no other
// connection sees any of them until commit. Tx reads and writes the hot tier
// only; it never touches the federated engine.
//
// Get and Query take the same *QueryRequest as EntityManager's and answer the
// same results, relation enrichment included, read on the transaction: they
// see its own writes. A request with Federated.Enabled is ErrInvalidInput.
// An unlocked read sees the latest committed state at the moment its
// statement runs (READ COMMITTED), so two reads in one Tx can disagree; a
// decision that must still hold at commit needs ForUpdate, AssertRow or a
// write's Preconditions. See docs/transactions.md.
//
// Tx is sealed, so methods can be added to it without breaking a caller's
// build. A type outside Forma cannot satisfy Tx with methods of its own; it can
// only embed a Tx, and then every method added later comes from the embedded
// value. A wrapper around the Tx that fn received forwards a new method
// straight to it, without running the wrapper's own code, and a test double
// that embeds a nil Tx panics when a new method is called. Forma never takes a
// Tx back from a caller, so a caller's own Tx type only reaches the caller's
// code.
type Tx interface {
	// Create, Update and Delete follow the EntityWriter rules, row ID and
	// unique keys included. An ErrUniqueConflict is found before the write
	// and leaves the Tx usable.
	Create(ctx context.Context, op *EntityOperation) (*DataRecord, error)
	Update(ctx context.Context, op *EntityOperation) (*DataRecord, error)
	Delete(ctx context.Context, op *EntityOperation) error

	// Get reads one row (req.RowID) on the transaction. A missing row is
	// ErrNotFound, which leaves the Tx usable. With ForUpdate it first takes
	// the row's write lock, as AssertRow does, so no other transaction can
	// change the row's own attributes until the Tx ends; attributes enriched
	// from related rows come from rows it does not lock. ForUpdate changes
	// only what Get locks: every request gets the same answer with or
	// without it.
	Get(ctx context.Context, req *QueryRequest, opts ...ReadOption) (*DataRecord, error)
	// Query runs a hot-tier query on the transaction. ForUpdate is
	// ErrInvalidInput: Query locks nothing.
	Query(ctx context.Context, req *QueryRequest, opts ...ReadOption) (*QueryResult, error)
	// AssertRow takes the write lock of (schemaName, rowID) until the Tx ends
	// and checks preconditions on the locked row, writing nothing. nil
	// preconditions assert only that the row exists. A missing row is
	// ErrNotFound and a row that fails them ErrPreconditionFailed; neither
	// poisons the Tx. If AssertRow answers nil and InTx then commits, no other
	// transaction committed a write to the row in between: a writer that
	// arrives after the assertion waits for the Tx to end.
	AssertRow(ctx context.Context, schemaName string, rowID uuid.UUID, preconditions Condition) error

	// UpdateWhere applies req.Updates and req.UpdateOps to the rows matching
	// req.Condition, each as Update would, and answers the rows it changed
	// (see MutateWhereRequest). A request refused by its validation, before
	// any SQL, or for matching more than MaxAffected rows leaves the Tx
	// usable; a failure after the first row was written poisons it, so the
	// Tx never commits part of a predicate mutation. A failure at the first
	// row follows the rules of Update.
	UpdateWhere(ctx context.Context, req *MutateWhereRequest) (*MutateWhereResult, error)
	// DeleteWhere deletes the rows matching req.Condition, each as Delete
	// would, and answers the rows it deleted. Its failure rules are
	// UpdateWhere's.
	DeleteWhere(ctx context.Context, req *MutateWhereRequest) (*MutateWhereResult, error)

	txseal.Sealed
}

// ReadOptions holds the options of one Tx read.
type ReadOptions struct {
	// ForUpdate makes Tx.Get take the row's write lock before it reads.
	ForUpdate bool
}

// ReadOption sets one option of one Tx read.
type ReadOption func(*ReadOptions)

// ForUpdate makes Tx.Get lock the row it reads against every other writer
// until the Tx ends: the per-row lock every Forma write takes, then the
// entity_main row lock. A missing row is ErrNotFound and locks no
// entity_main row, but the per-row lock is still held, so any other write of
// that RowID waits for the Tx. Tx.Query refuses it with ErrInvalidInput.
func ForUpdate() ReadOption {
	return func(o *ReadOptions) { o.ForUpdate = true }
}

// The default limits of one InTx call. DefaultTxMaxPhysicalRows matches
// Aurora DSQL's documented limit on rows modified per transaction, so a Tx
// that fits on PostgreSQL also fits there (#633).
const (
	DefaultTxMaxOperations   = 1000
	DefaultTxMaxPhysicalRows = 3000
)

// TxOptions holds the limits of one InTx call. InTx starts from the defaults
// (DefaultTxMaxOperations, DefaultTxMaxPhysicalRows and
// Config.Transaction.DefaultTimeout) and applies the options in order.
type TxOptions struct {
	// MaxOperations caps the Tx method calls. It must be positive.
	MaxOperations int
	// MaxPhysicalRows caps the rows the transaction modifies, counting
	// entity_main, EAV and change_log rows. It must be positive.
	MaxPhysicalRows int
	// Timeout bounds the whole InTx call as a context deadline. Zero means
	// no deadline beyond the caller's context; negative is invalid.
	Timeout time.Duration
}

// TxOption overrides one limit of one InTx call.
type TxOption func(*TxOptions)

// WithTxMaxOperations caps the Tx method calls of one InTx at n. The call that
// would exceed it runs nothing, answers ErrInvalidInput and poisons the Tx.
func WithTxMaxOperations(n int) TxOption {
	return func(o *TxOptions) { o.MaxOperations = n }
}

// WithTxMaxPhysicalRows caps the rows one InTx modifies at n, counted from
// what PostgreSQL reports each statement modified. The count is checked after
// each statement, so the statement that takes it past n has run: it fails,
// its operation answers ErrInvalidInput, and the Tx is poisoned, so no later
// statement runs and none of the Tx's writes is ever committed. The cap is
// exact for what commits; the work a Tx does can pass it by that one
// statement.
func WithTxMaxPhysicalRows(n int) TxOption {
	return func(o *TxOptions) { o.MaxPhysicalRows = n }
}

// WithTxTimeout bounds the whole InTx call by d instead of
// Config.Transaction.DefaultTimeout. Zero removes Forma's deadline.
func WithTxTimeout(d time.Duration) TxOption {
	return func(o *TxOptions) { o.Timeout = d }
}

// ErrTxDone marks a Tx method called after its InTx returned.
var ErrTxDone = errors.New("transaction already finished")

// ErrTxAborted marks a Tx method called after an earlier error poisoned the
// Tx, and an InTx whose fn returned nil over a poisoned Tx. The error also
// wraps the poisoning cause, so errors.Is reaches it (and IsRetryable reports
// true when the cause was a retryable conflict).
var ErrTxAborted = errors.New("transaction aborted")
