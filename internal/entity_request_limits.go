package internal

import (
	"context"
	"math"
	"time"

	"github.com/lychee-technology/forma"
)

// requestBudgets is the slice of forma.Config the services enforce per call
// (#465). Before #465 every one of these was declared, validated, and never
// read; the services now take them at construction so a manager built from a
// config honours it, and a zero value keeps the pre-#465 behaviour (no bound)
// instead of turning into an instant timeout or an empty batch.
type requestBudgets struct {
	// read bounds Get, Query and CrossSchemaSearch (QueryConfig.DefaultTimeout).
	read time.Duration
	// write bounds one write transaction (TransactionConfig.DefaultTimeout):
	// Create, Update, Delete, and each atomic batch. A best-effort batch is
	// N transactions and is bounded per operation through the CRUD service,
	// not as a whole.
	write time.Duration
	// maxBatchSize caps the operations one batch may carry
	// (PerformanceConfig.MaxBatchSize); zero or negative is unlimited.
	maxBatchSize int
}

func budgetsFromConfig(cfg *forma.Config) requestBudgets {
	if cfg == nil {
		return requestBudgets{}
	}
	return requestBudgets{
		read:         cfg.Query.DefaultTimeout,
		write:        cfg.Transaction.DefaultTimeout,
		maxBatchSize: cfg.Performance.MaxBatchSize,
	}
}

// withBudget bounds ctx by d. A non-positive d means "no bound" and hands
// back ctx with a no-op cancel, so call sites can always defer the cancel.
//
// The deadline is a real context deadline rather than a Postgres
// statement_timeout: pgx cancels the running statement when the context
// expires, DuckDB (duckdb-go v2) interrupts the running query the same way,
// and the same expiry also covers the Go-side work around the statements
// (transform, relation enrichment), which a server-side setting never would.
func withBudget(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// pageOffset turns a 1-based page into a row offset, refusing a page whose
// window ends past maxRows (QueryConfig.MaxRows, #598) or whose offset does
// not fit in an int (#465). Callers clamp page and itemsPerPage to at least 1,
// and itemsPerPage to MaxPageSize, before calling.
//
// The depth limit bounds the rows Postgres and DuckDB scan and discard to
// reach an OFFSET. It refuses rather than clamps: clamping would silently
// repeat the last admitted page for every deeper one. maxRows <= 0 leaves
// pagination unbounded. `page > maxRows/itemsPerPage` is exactly
// `page*itemsPerPage > maxRows` for integers and cannot overflow, so even a
// page whose offset would overflow is answered with the limit.
//
// Before the overflow guard the product wrapped to a negative offset, which
// the SQL generator rendered as `OFFSET -N`: an error from Postgres at best,
// and a negative that a future `offset < 0` clamp would have silently turned
// into page 1.
func pageOffset(page, itemsPerPage, maxRows int) (int, error) {
	if page < 1 || itemsPerPage < 1 {
		return 0, forma.InvalidInputf("page %d and items per page %d must both be positive", page, itemsPerPage)
	}
	if maxRows > 0 && page > maxRows/itemsPerPage {
		return 0, forma.InvalidInputf("page %d with %d items per page exceeds the pagination limit of %d rows; filter the query to reach rows beyond it", page, itemsPerPage, maxRows)
	}
	if page-1 > math.MaxInt/itemsPerPage {
		return 0, forma.InvalidInputf("page %d with %d items per page addresses an offset beyond the supported range", page, itemsPerPage)
	}
	return (page - 1) * itemsPerPage, nil
}

// validateBatchOperation is the shared admission check for the three batch
// entry points. The size cap is enforced here, at the manager, rather than in
// the HTTP layer, so a library embedder gets the same bound as an HTTP caller;
// maxBatchSize <= 0 leaves the batch unbounded.
//
// Batches carry no conditional writes (#649): an operation with Preconditions
// is refused before any write, because a best-effort batch would silently
// drop them and an atomic one has no per-row outcome to report. So is every
// update operator, with one exception: acceptsUnset (BatchUpdate) admits
// Unset (#940), which needs no per-row compute or guard contract. An
// Increment or a ServerTime computes from the locked row or the transaction
// clock, which a batch has no contract for. The operator's kind is all this
// check reads; the unset's target is the schema's to judge, per operation.
// Conditional writes of several rows go through Transactor.InTx.
func validateBatchOperation(req *forma.BatchOperation, maxBatchSize int, acceptsUnset bool) error {
	if req == nil {
		return forma.InvalidInputf("batch operation cannot be nil")
	}
	if maxBatchSize > 0 && len(req.Operations) > maxBatchSize {
		return forma.InvalidInputf("batch of %d operations exceeds the maximum batch size of %d", len(req.Operations), maxBatchSize)
	}
	for i := range req.Operations {
		op := &req.Operations[i]
		if op.Preconditions != nil {
			return forma.InvalidInputf("operation[%d]: batches do not accept preconditions (the precondition tests '%s'); run conditional writes through Transactor.InTx",
				i, firstConditionAttr(op.Preconditions))
		}
		if err := refuseBatchUpdateOps(i, op.UpdateOps, acceptsUnset); err != nil {
			return err
		}
	}
	return nil
}

// refuseBatchUpdateOps refuses the update operators of operation i that its
// batch does not accept: every one, or every one but Unset when acceptsUnset.
func refuseBatchUpdateOps(i int, ops []forma.UpdateOp, acceptsUnset bool) error {
	for _, op := range ops {
		if acceptsUnset && op.Kind == forma.UpdateOpUnset {
			continue
		}
		if acceptsUnset {
			return forma.InvalidInputf("operation[%d]: batches do not accept update operators (%s on '%s'); BatchUpdate accepts %s only, run other operators through Update or Transactor.InTx",
				i, op.Kind, op.Attribute, forma.UpdateOpUnset)
		}
		return forma.InvalidInputf("operation[%d]: batches do not accept update operators (%s on '%s'); run them through Update or Transactor.InTx",
			i, op.Kind, op.Attribute)
	}
	return nil
}

func emptyBatchResult() *forma.BatchResult {
	return &forma.BatchResult{
		Successful: make([]*forma.DataRecord, 0),
		Failed:     make([]forma.OperationError, 0),
		TotalCount: 0,
	}
}
