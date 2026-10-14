package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
)

// transactor is Forma's forma.Transactor (#648). It shares the manager's
// repository (and so its pool and metadata cache) and the manager's CRUD,
// query and relation services, which every session runs bound to its own
// transaction.
type transactor struct {
	repo     *DBPersistentRecordRepository
	crud     *entityCRUDService
	query    *entityQueryService
	relation *entityRelationService
	// defaultTimeout is Config.Transaction.DefaultTimeout, the InTx budget
	// WithTxTimeout overrides.
	defaultTimeout time.Duration
	// statementFault, set only by tests, is installed on every session's
	// meter (see txMeter.fault).
	statementFault func(sql string) error
}

var _ forma.Transactor = (*transactor)(nil)

// NewTransactor returns the Transactor for a manager built by
// NewEntityManager over the PostgreSQL repository: the factory's managers,
// and the e2e harness's. Any other forma.EntityManager — a caller's decorator,
// or a manager over another repository — answers ErrInvalidInput, because the
// transactor needs the manager's pool and services, not its interface.
func NewTransactor(em forma.EntityManager) (forma.Transactor, error) {
	manager, ok := em.(*entityManager)
	if !ok || manager == nil {
		return nil, forma.InvalidInputf("a transactor needs an EntityManager built by forma's factory, not %T", em)
	}
	repo, ok := manager.repository.(*DBPersistentRecordRepository)
	if !ok || repo == nil {
		return nil, forma.InvalidInputf("a transactor needs an EntityManager backed by Forma's PostgreSQL repository, not %T",
			manager.repository)
	}
	return &transactor{
		repo:           repo,
		crud:           manager.crud,
		query:          manager.query,
		relation:       manager.relation,
		defaultTimeout: manager.config.Transaction.DefaultTimeout,
	}, nil
}

// InTx implements forma.Transactor. The deferred rollback is what covers a
// panic and runtime.Goexit in fn: neither returns to the code after fn, and
// the panic is never recovered, so it continues with its original value.
func (t *transactor) InTx(ctx context.Context, fn func(context.Context, forma.Tx) error, opts ...forma.TxOption) error {
	if ctx == nil || fn == nil {
		return forma.InvalidInputf("InTx needs a non-nil context and function")
	}
	if ctx.Value(txSessionKey{}) != nil {
		return forma.InvalidInputf("InTx was called with a context from another InTx; nested transactions are not supported")
	}
	limits, err := t.resolveOptions(opts)
	if err != nil {
		return err
	}
	ctx, cancel := withBudget(ctx, limits.Timeout)
	defer cancel()

	pgTx, err := t.repo.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", classifyStatementError(err))
	}
	session := t.newSession(ctx, pgTx, limits)
	defer session.stop()

	returned := false
	defer func() {
		if !returned {
			_ = session.finish()
			_ = pgTx.Rollback(ctx)
		}
	}()
	fnErr := fn(context.WithValue(session.ctx, txSessionKey{}, session), session)
	returned = true
	return settle(ctx, pgTx, session, fnErr)
}

// settle commits or rolls back once fn has returned. The transaction
// commits only when fn returned nil, the session is not poisoned and ctx has
// not ended: ctx is checked before COMMIT because a COMMIT sent on an ended
// context is classified unknown, not rolled back.
func settle(ctx context.Context, pgTx pgx.Tx, session *txSession, fnErr error) error {
	cause := session.finish()
	var reasons []error
	if fnErr != nil {
		reasons = append(reasons, fnErr)
	}
	if cause != nil && !errors.Is(fnErr, cause) {
		reasons = append(reasons, fmt.Errorf("%w by an earlier error: %w", forma.ErrTxAborted, cause))
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !anyIs(reasons, ctxErr) {
		reasons = append(reasons, ctxErr)
	}
	if len(reasons) > 0 {
		_ = pgTx.Rollback(ctx)
		return &txRolledBack{reasons: reasons}
	}
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", classifyCommitError(err))
	}
	return nil
}

func anyIs(errs []error, target error) bool {
	for _, err := range errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// txRolledBack is InTx's error when the transaction did not commit. It wraps
// fn's error first, so errors.As finds fn's own types first, and then the
// poisoning cause and the context's error when fn's error does not already
// carry them: whatever fn returned, a Tx a deadlock poisoned still answers
// IsRetryable, and a cancelled one still matches context.Canceled.
type txRolledBack struct{ reasons []error }

func (e *txRolledBack) Error() string {
	parts := make([]string, len(e.reasons))
	for i, reason := range e.reasons {
		parts[i] = reason.Error()
	}
	return "transaction rolled back: " + strings.Join(parts, "; ")
}

func (e *txRolledBack) Unwrap() []error { return e.reasons }

func (t *transactor) resolveOptions(opts []forma.TxOption) (forma.TxOptions, error) {
	limits := forma.TxOptions{
		MaxOperations:   forma.DefaultTxMaxOperations,
		MaxPhysicalRows: forma.DefaultTxMaxPhysicalRows,
		Timeout:         t.defaultTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&limits)
		}
	}
	switch {
	case limits.MaxOperations <= 0:
		return limits, forma.InvalidInputf("the transaction's operation limit must be positive, got %d", limits.MaxOperations)
	case limits.MaxPhysicalRows <= 0:
		return limits, forma.InvalidInputf("the transaction's physical-row limit must be positive, got %d", limits.MaxPhysicalRows)
	case limits.Timeout < 0:
		return limits, forma.InvalidInputf("the transaction's timeout must not be negative, got %s", limits.Timeout)
	}
	return limits, nil
}

func (t *transactor) newSession(ctx context.Context, pgTx pgx.Tx, limits forma.TxOptions) *txSession {
	meter := &txMeter{maxRows: int64(limits.MaxPhysicalRows), fault: t.statementFault}
	tx := &meteredTx{Tx: pgTx, meter: meter}
	sessionCtx, stop := context.WithCancel(ctx)
	rows := newTxBoundRepository(t.repo, tx)
	enrich := t.relation.boundTo(rows).enrichDataRecords
	return &txSession{
		ctx:    sessionCtx,
		stop:   stop,
		crud:   t.crud.boundTo(rows, enrich),
		query:  t.query.boundTo(rows, enrich),
		rows:   rows,
		tx:     tx,
		meter:  meter,
		maxOps: limits.MaxOperations,
	}
}
