package forma

import (
	"context"
	"errors"
	"time"

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
// Tx is sealed, so methods can be added to it without breaking a caller's
// build. A type outside Forma cannot satisfy Tx with methods of its own; it can
// only embed a Tx, and then every method added later comes from the embedded
// value. A wrapper around the Tx that fn received forwards a new method
// straight to it, without running the wrapper's own code, and a test double
// that embeds a nil Tx panics when a new method is called. Forma never takes a
// Tx back from a caller, so a caller's own Tx type only reaches the caller's
// code.
type Tx interface {
	Create(ctx context.Context, op *EntityOperation) (*DataRecord, error)
	Update(ctx context.Context, op *EntityOperation) (*DataRecord, error)
	Delete(ctx context.Context, op *EntityOperation) error

	txseal.Sealed
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
