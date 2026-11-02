package federated

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// These tests pin the scan loop that ExecutePostgresQuery and the DuckDB path
// share (#430). They run untagged, so `make test` reaches a loop that before
// #430 only the docker e2e suite exercised.

var (
	plainScanColumns     = []string{"row_id", "schema_id", "created_at", "changed_at", "deleted_at", "name", "version"}
	benchmarkScanColumns = append(append([]string{}, plainScanColumns...), "symbol", "exchange", "region", "tradeType", "tradeTime")
	errMidIteration      = errors.New("connection reset mid-iteration")
)

func TestExecutePostgresQueryScansBenchmarkProjection(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	rowID := uuid.New()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	// pgx's database/sql driver hands a BIGINT back as int64, the type the
	// Postgres-only select's COALESCE over BIGINT columns produces for tradeTime.
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(benchmarkScanColumns).
		AddRow(rowID.String(), int16(1), int64(10), int64(20), int64(30), "n", int64(2), "SYM", "EX", "EU", int64(4), int64(1700000000123)))

	result, err := h.ExecutePostgresQuery(context.Background(), &QueryOptions{SortBy: "tradeTime"})
	if err != nil {
		t.Fatalf("ExecutePostgresQuery: %v", err)
	}
	if result.TotalRecords != 1 || len(result.Records) != 1 {
		t.Fatalf("got total=%d records=%d, want 1 and 1", result.TotalRecords, len(result.Records))
	}
	rec := result.Records[0]
	if rec.RowID != rowID || rec.CreatedAt != 10 || rec.UpdatedAt != 20 || rec.DeletedAt == nil || *rec.DeletedAt != 30 {
		t.Fatalf("record identity/stamps = %+v", rec)
	}
	for key, want := range map[string]string{"name": "n", "symbol": "SYM", "exchange": "EX", "region": "EU"} {
		if rec.TextItems[key] != want {
			t.Errorf("TextItems[%q] = %q, want %q", key, rec.TextItems[key], want)
		}
	}
	if rec.Float64Items["version"] != 2 {
		t.Errorf("Float64Items[version] = %v, want 2", rec.Float64Items["version"])
	}
	if rec.Int64Items["tradeType"] != 4 || rec.Int64Items["tradeTime"] != 1700000000123 {
		t.Errorf("Int64Items = %v, want tradeType 4 and tradeTime 1700000000123", rec.Int64Items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutePostgresQueryPlainProjectionLeavesInt64ItemsNil(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(plainScanColumns).
		AddRow(uuid.NewString(), int16(1), nil, int64(20), int64(0), "n", int64(0)))

	result, err := h.ExecutePostgresQuery(context.Background(), nil)
	if err != nil {
		t.Fatalf("ExecutePostgresQuery: %v", err)
	}
	rec := result.Records[0]
	if rec.Int64Items != nil || rec.DeletedAt != nil || rec.CreatedAt != 0 {
		t.Fatalf("plain row = %+v, want nil Int64Items and DeletedAt, zero CreatedAt", rec)
	}
}

// A mid-iteration failure must not read as a complete, shorter result set:
// in a harness asserting record counts across tiers that looks like a
// data-correctness bug in the system under test.
func TestExecutePostgresQuerySurfacesMidIterationError(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(plainScanColumns).
		AddRow(uuid.NewString(), int16(1), int64(1), int64(1), int64(0), "a", int64(0)).
		AddRow(uuid.NewString(), int16(1), int64(1), int64(1), int64(0), "b", int64(0)).
		RowError(1, errMidIteration))

	result, err := h.ExecutePostgresQuery(context.Background(), nil)
	if !errors.Is(err, errMidIteration) {
		t.Fatalf("err = %v (result %+v), want wrapped %v", err, result, errMidIteration)
	}
}

func TestGetDirtyIDsSurfacesMidIterationError(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	mock.ExpectQuery("SELECT row_id FROM change_log").WillReturnRows(sqlmock.NewRows([]string{"row_id"}).
		AddRow(uuid.NewString()).
		AddRow(uuid.NewString()).
		RowError(1, errMidIteration))

	ids, err := h.getDirtyIDs(context.Background())
	if !errors.Is(err, errMidIteration) {
		t.Fatalf("err = %v (ids %v), want wrapped %v", err, ids, errMidIteration)
	}
}

func TestExecutePostgresQueryWrapsQueryErrors(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	queryErr := errors.New("relation does not exist")
	mock.ExpectQuery("SELECT COUNT").WillReturnError(queryErr)

	_, err := h.ExecutePostgresQuery(context.Background(), nil)
	if !errors.Is(err, queryErr) || err.Error() == queryErr.Error() {
		t.Fatalf("err = %v, want %v wrapped with context", err, queryErr)
	}
}
