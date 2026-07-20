package cdc

import (
	"context"
	"fmt"
)

// initSchemaUnderLock holds the per-schema advisory lock for the whole
// export + manifest publish, so manifest-reconcile (which reconciles under
// the same lock) can never race an in-flight init (#290). Contended is an
// error, not a skip: init is operator-initiated and a silently skipped
// schema would stay uninitialized unnoticed.
func initSchemaUnderLock(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error) {
	tryLock := runCtx.tryLock
	if tryLock == nil {
		tryLock = TrySchemaLock
	}
	locked, unlock, err := tryLock(ctx, runCtx.db, schemaID)
	if err != nil {
		return 0, 0, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !locked {
		return 0, 0, ErrSchemaLockContended
	}
	defer unlock()
	initSchemaFn := runCtx.initSchemaFn
	if initSchemaFn == nil {
		initSchemaFn = initSchema
	}
	// The caller (processInitSchemas) already wraps this with "schema %d: %w",
	// so we name only the operation here — not the schema ID — to avoid the
	// double-naming rejected in earlier review.
	rowsExported, filesCreated, err := initSchemaFn(ctx, runCtx, schemaID)
	if err != nil {
		return rowsExported, filesCreated, fmt.Errorf("init export+publish under schema lock: %w", err)
	}
	return rowsExported, filesCreated, nil
}
