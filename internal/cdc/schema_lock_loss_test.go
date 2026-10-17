package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// lostLocker grants the lock with a held context that has already ended
// because the lock was lost, as a DSQL lease does when renewals fail.
func lostLocker(released *int) SchemaLocker {
	return SchemaLockerFunc(func(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
		held, cancel := context.WithCancelCause(ctx)
		cancel(fmt.Errorf("%w: schema %d's lease was not renewed within 45s", ErrSchemaLockLost, schemaID))
		return held, func() { *released++ }, true, nil
	})
}

// TestProcessSchema_NamesALostLock pins that the flush runs on the lock's
// held context, and that work cut short by a lost lease says so rather than
// reporting a bare context.Canceled (#633).
func TestProcessSchema_NamesALostLock(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE change_log (schema_id SMALLINT, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)

	released := 0
	flushCtx := &schemaFlushContext{
		db:        db,
		cfg:       CDCConfig{MinRecords: 1, MaxAgeMs: 1000},
		tableName: "change_log",
		logger:    zap.NewNop(),
		locker:    lostLocker(&released),
	}

	err = flushCtx.processSchema(context.Background(), 7)
	require.ErrorIs(t, err, ErrSchemaLockLost)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, released)
}

func TestProcessInitSchemas_NamesALostLock(t *testing.T) {
	released := 0
	var workCtx context.Context
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		locker:         lostLocker(&released),
		initSchemaFn: func(ctx context.Context, _ *initRunContext, _ int16) (int64, int, error) {
			workCtx = ctx
			return 0, 0, fmt.Errorf("export snapshot: %w", ctx.Err())
		},
	}

	_, err := processInitSchemas(context.Background(), runCtx, []int64{7})
	require.ErrorIs(t, err, ErrSchemaLockLost)
	require.ErrorIs(t, context.Cause(workCtx), ErrSchemaLockLost, "init must run on the held context")
	require.Contains(t, err.Error(), "schema 7")
	require.Equal(t, 1, released)
}
