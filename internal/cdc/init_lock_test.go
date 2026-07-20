package cdc

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
)

// TestProcessInitSchemas_ContendedLockRecordsErrorAndContinues verifies that a
// contended per-schema advisory lock is treated as an error (not a silent
// skip): the contended schema lands in the joined error naming it, while other
// schemas still run and their locks are released.
func TestProcessInitSchemas_ContendedLockRecordsErrorAndContinues(t *testing.T) {
	var initCalled []int16
	var unlockCalled []int16
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		tryLock: func(_ context.Context, _ *sql.DB, sid int16) (bool, func(), error) {
			if sid == 7 {
				return false, nil, nil
			}
			return true, func() { unlockCalled = append(unlockCalled, sid) }, nil
		},
		initSchemaFn: func(_ context.Context, _ *initRunContext, sid int16) (int64, int, error) {
			initCalled = append(initCalled, sid)
			return 5, 1, nil
		},
	}
	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7, 8})
	if !errors.Is(err, ErrSchemaLockContended) {
		t.Fatalf("want ErrSchemaLockContended in joined error, got %v", err)
	}
	if !strings.Contains(err.Error(), "schema 7") {
		t.Fatalf("error must name the contended schema: %v", err)
	}
	if len(initCalled) != 1 || initCalled[0] != 8 {
		t.Fatalf("schema 8 must still be processed, got %v", initCalled)
	}
	if len(unlockCalled) != 1 || unlockCalled[0] != 8 {
		t.Fatalf("unlock must run for locked schema 8, got %v", unlockCalled)
	}
	if summary.TotalRowsExported != 5 || summary.TotalFilesCreated != 1 {
		t.Fatalf("summary must reflect the successful schema, got %+v", summary)
	}
}
