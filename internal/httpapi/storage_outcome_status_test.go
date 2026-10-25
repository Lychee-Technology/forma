package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// The #645 storage outcomes on the HTTP surface. Each chain is shaped the way
// internal/storage_outcome.go builds it: the outcome sentinel beside the driver
// error, wrapped by the write path's plain prefixes. httpapi cannot import that
// package (it would pull the federated engine's DuckDB CGO into this pure-Go
// test build), so the shapes are written out here.

// driverText stands in for what the PostgreSQL driver puts in its error, which
// no redacted body may carry.
const driverText = `ERROR: deadlock detected (SQLSTATE 40P01) on "entity_main"`

type storageOutcomeCase struct {
	name      string
	err       error
	status    int
	wantError string // the body's error field
	wantClass string // empty on a disclosed body
}

func storageOutcomeCases() []storageOutcomeCase {
	driver := errors.New(driverText)
	return []storageOutcomeCase{
		{
			name: "published unique conflict is 409 with its message",
			err: fmt.Errorf("failed to insert persistent record: %w", forma.WithOperatorDetail(
				forma.UniqueConflictf("the write conflicts with a row that already exists"),
				fmt.Errorf("Key (ltbase_row_id)=(x) already exists.: %w", driver))),
			status:    http.StatusConflict,
			wantError: "update failed: the write conflicts with a row that already exists",
		},
		{
			name:      "published precondition failure is 412 with its message",
			err:       fmt.Errorf("failed to update: %w", forma.PreconditionFailedf("row r changed since version 7")),
			status:    http.StatusPreconditionFailed,
			wantError: "update failed: row r changed since version 7",
		},
		{
			name:      "bare unique conflict is a redacted 409 that keeps its class",
			err:       fmt.Errorf("insert: %w", forma.ErrUniqueConflict),
			status:    http.StatusConflict,
			wantError: "the write conflicts with an existing row",
			wantClass: errorClassUniqueConflict,
		},
		{
			name:      "bare precondition failure is a redacted 412 that keeps its class",
			err:       fmt.Errorf("update: %w", forma.ErrPreconditionFailed),
			status:    http.StatusPreconditionFailed,
			wantError: "a precondition of the write did not hold",
			wantClass: errorClassPreconditionFailed,
		},
		{
			name:      "retryable conflict is a redacted 503",
			err:       fmt.Errorf("failed to update persistent record: %w: %w", forma.ErrRetryableConflict, driver),
			status:    http.StatusServiceUnavailable,
			wantError: "the transaction was rolled back because of a concurrent conflict; it is safe to retry",
			wantClass: errorClassRetryableConflict,
		},
		{
			name:      "unknown commit is a redacted 500",
			err:       fmt.Errorf("commit transaction: %w: %w", forma.ErrCommitOutcomeUnknown, driver),
			status:    http.StatusInternalServerError,
			wantError: "the commit outcome is unknown; the write may or may not have been applied",
			wantClass: errorClassCommitOutcomeUnknown,
		},
		{
			// Given/When/Then 5: the budget expired while COMMIT was in
			// flight. 504 would tell the caller to retry a write that may
			// have landed.
			name: "unknown commit beside an expired deadline is 500, not 504",
			err: fmt.Errorf("commit transaction: %w: %w", forma.ErrCommitOutcomeUnknown,
				fmt.Errorf("timeout: %w", context.DeadlineExceeded)),
			status:    http.StatusInternalServerError,
			wantError: "the commit outcome is unknown; the write may or may not have been applied",
			wantClass: errorClassCommitOutcomeUnknown,
		},
		{
			name: "unknown commit beside a retryable conflict is never 503",
			err: errors.Join(fmt.Errorf("%w: %w", forma.ErrRetryableConflict, driver),
				fmt.Errorf("%w: %w", forma.ErrCommitOutcomeUnknown, driver)),
			status:    http.StatusInternalServerError,
			wantError: "the commit outcome is unknown; the write may or may not have been applied",
			wantClass: errorClassCommitOutcomeUnknown,
		},
		{
			name:      "regression: an expired budget is still a 504 timeout",
			err:       fmt.Errorf("update: %w", context.DeadlineExceeded),
			status:    http.StatusGatewayTimeout,
			wantError: "request timed out",
			wantClass: errorClassTimeout,
		},
		{
			name:      "regression: a published generic conflict is still a 409",
			err:       fmt.Errorf("update: %w", forma.Conflictf("the write conflicts with a row that already exists")),
			status:    http.StatusConflict,
			wantError: "update failed: the write conflicts with a row that already exists",
		},
	}
}

func TestStorageOutcomesMapToStatusBodyAndClass(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	for _, tt := range storageOutcomeCases() {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyManagerError(tt.err); got != tt.status {
				t.Fatalf("classifyManagerError = %d, want %d", got, tt.status)
			}

			server := NewServer(&mockEntityManager{updateErr: tt.err}, Options{})
			req := httptest.NewRequest(http.MethodPut, "/api/v1/lead/"+uuid.NewString(), strings.NewReader(`{"name":"x"}`))
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.status, rec.Body.String())
			}
			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.Error != tt.wantError {
				t.Fatalf("error = %q, want %q", resp.Error, tt.wantError)
			}
			if resp.ErrorClass != tt.wantClass {
				t.Fatalf("error_class = %q, want %q", resp.ErrorClass, tt.wantClass)
			}
			for _, leaked := range []string{"SQLSTATE", "deadlock detected", "ltbase_row_id", "Key ("} {
				if strings.Contains(rec.Body.String(), leaked) {
					t.Fatalf("body leaked driver detail %q: %s", leaked, rec.Body.String())
				}
			}
		})
	}
}

// TestStorageOutcomeClassesOnlyAddTokens pins that #645 extends the redacted
// vocabulary without moving any read-path class (#301).
func TestStorageOutcomeClassesOnlyAddTokens(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{&forma.ParquetSetInconsistentError{SchemaID: 1}, errorClassParquetSetInconsistent},
		{&forma.NoParquetPathsError{SchemaID: 1}, errorClassNoParquetPaths},
		{&forma.ManifestSchemaMismatchError{RequestedSchemaID: 1, ManifestSchemaID: 2}, errorClassManifestSchemaMismatch},
		{&forma.ParquetTypeIncompatibleError{SchemaID: 1}, errorClassParquetTypeIncompatible},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), errorClassTimeout},
		{fmt.Errorf("conflict: %w", forma.ErrConflict), errorClassInternal},
		{errors.New("db down"), errorClassInternal},
	}
	for _, tt := range tests {
		if got := errorClass(tt.err); got != tt.want {
			t.Fatalf("errorClass(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}
