package cdc

import (
	"context"
	"fmt"
)

// initSchemaUnderLock holds the per-schema lock for the whole export +
// manifest publish, so manifest-reconcile (which reconciles under the same
// lock) can never race an in-flight init (#290). Contended is an error, not
// a skip: init is operator-initiated and a silently skipped schema would
// stay uninitialized unnoticed. The work runs on the lock's held context, so
// a lost lease (#633) ends it.
func initSchemaUnderLock(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error) {
	locker := runCtx.locker
	if locker == nil {
		locker = advisorySchemaLocker{db: runCtx.db}
	}
	held, release, locked, err := locker.TryLock(ctx, schemaID)
	if err != nil {
		return 0, 0, fmt.Errorf("acquire schema lock: %w", err)
	}
	if !locked {
		return 0, 0, ErrSchemaLockContended
	}
	defer release()
	initSchemaFn := runCtx.initSchemaFn
	if initSchemaFn == nil {
		initSchemaFn = initSchema
	}
	// The caller (processInitSchemas) already wraps this with "schema %d: %w",
	// so we name only the operation here — not the schema ID — to avoid the
	// double-naming rejected in earlier review.
	rowsExported, filesCreated, err := initSchemaFn(held, runCtx, schemaID)
	if err != nil {
		return rowsExported, filesCreated, WithLockLoss(held, fmt.Errorf("init export+publish under schema lock: %w", err))
	}
	return rowsExported, filesCreated, nil
}
