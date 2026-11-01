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

// TestProcessSchemas_ReturnsALostLock pins that a lost lock survives the
// per-schema join processSchemas builds, which is the error RunOnce and
// Runner.RunOnce return, so the public cdc.ErrSchemaLockLost matches it
// (#768). A contended lock, by contrast, is a skip and fails nothing.
func TestProcessSchemas_ReturnsALostLock(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE change_log (schema_id SMALLINT, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)

	newCtx := func(locker SchemaLocker) *schemaFlushContext {
		return &schemaFlushContext{
			db:             db,
			cfg:            CDCConfig{MinRecords: 1, MaxAgeMs: 1000},
			tableName:      "change_log",
			logger:         zap.NewNop(),
			schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
			locker:         locker,
		}
	}

	released := 0
	_, err = newCtx(lostLocker(&released)).processSchemas(context.Background(), []int64{7})
	require.ErrorIs(t, err, ErrSchemaLockLost)
	require.Contains(t, err.Error(), "schema 7")
	require.Equal(t, 1, released)

	tried := 0
	contended := SchemaLockerFunc(func(context.Context, int16) (context.Context, func(), bool, error) {
		tried++
		return nil, nil, false, nil
	})
	_, err = newCtx(contended).processSchemas(context.Background(), []int64{7})
	require.NoError(t, err, "a contended schema lock is a skip in the flush path, not ErrSchemaLockContended")
	require.Equal(t, 1, tried)
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

// TestProcessInitSchemas_NamesALockLostUnderCleanWork pins that a lost lease
// fails the schema even when the init work returns no error, as work that
// logs and skips a cancelled step does.
func TestProcessInitSchemas_NamesALockLostUnderCleanWork(t *testing.T) {
	released := 0
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		locker:         lostLocker(&released),
		initSchemaFn: func(context.Context, *initRunContext, int16) (int64, int, error) {
			return 3, 1, nil
		},
	}

	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7})
	require.ErrorIs(t, err, ErrSchemaLockLost)
	require.Contains(t, err.Error(), "schema 7")
	require.Zero(t, summary.TotalFilesCreated, "a schema whose lock was lost is not counted as initialized")
	require.Equal(t, 1, released)
}
