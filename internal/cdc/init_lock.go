package cdc

import (
	"context"
	"fmt"
)

// initSchemaUnderLock holds the per-schema lock for the whole export,
// publication and handoff, so a flusher, another cdc-init and
// manifest-reconcile (which take the same lock) stay out for liveness (#290).
// Safety does not rest on it: the publication's compare-and-set alone decides
// which of two writers commits. Contended is an error, not a skip: init is
// operator-initiated and a silently skipped schema would stay uninitialized
// unnoticed. The work runs on the lock's held context, so a lost lease (#633)
// ends it.
func initSchemaUnderLock(ctx context.Context, runCtx *initRunContext, schemaID int16) (SchemaInit, error) {
	locker := runCtx.locker
	if locker == nil {
		locker = advisorySchemaLocker{db: runCtx.db}
	}
	held, release, locked, err := locker.TryLock(ctx, schemaID)
	if err != nil {
		return SchemaInit{}, fmt.Errorf("acquire schema lock: %w", err)
	}
	if !locked {
		return SchemaInit{}, ErrSchemaLockContended
	}
	defer release()
	initSchemaFn := runCtx.initSchemaFn
	if initSchemaFn == nil {
		initSchemaFn = initSchema
	}
	// The caller (processInitSchemas) already wraps this with "schema %d: %w",
	// so we name only the operation here — not the schema ID — to avoid the
	// double-naming rejected in earlier review.
	out, err := initSchemaFn(held, runCtx, schemaID)
	if err != nil {
		err = fmt.Errorf("init export+publish under schema lock: %w", err)
	}
	return out, WithLockLoss(held, err)
}
