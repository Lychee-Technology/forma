package federated

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/e2e_harness"
)

// newMockSeedingHarness returns a harness whose PGDB is a strict, in-order
// sqlmock, so a test states every statement and transaction boundary
// seeding is allowed to send.
func newMockSeedingHarness(t *testing.T) (*FederatedTestHarness, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	h := &FederatedTestHarness{
		TestHarness:   &e2e_harness.TestHarness{PGDB: db},
		SchemaID:      1,
		seededRecords: make(map[string][]TestRecord),
	}
	return h, mock
}

// attributeFreeRecords returns records with no attributes, so each costs
// exactly one entity_main and one change_log statement.
func attributeFreeRecords(n int) []TestRecord {
	records := make([]TestRecord, n)
	for i := range records {
		records[i] = TestRecord{RowID: uuid.New(), SchemaID: 1, Attributes: map[string]any{}, ChangedAt: 1}
	}
	return records
}

func expectRecordStatements(mock sqlmock.Sqlmock, n int) {
	for range n {
		mock.ExpectExec("INSERT INTO entity_main").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO change_log").WillReturnResult(sqlmock.NewResult(0, 1))
	}
}

func TestSeedHotRecordsWithDataCommitsOncePerBatch(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	records := attributeFreeRecords(hotSeedBatchRecords + 1)

	mock.ExpectBegin()
	expectRecordStatements(mock, hotSeedBatchRecords)
	mock.ExpectCommit()
	mock.ExpectBegin()
	expectRecordStatements(mock, 1)
	mock.ExpectCommit()

	if err := h.SeedHotRecordsWithData(context.Background(), records); err != nil {
		t.Fatalf("SeedHotRecordsWithData: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("seeding did not commit one transaction per %d records: %v", hotSeedBatchRecords, err)
	}
	if got := len(h.seededRecords["hot"]); got != len(records) {
		t.Fatalf("seededRecords[hot] holds %d records, want %d", got, len(records))
	}
}

func TestSeedHotRecordsWithDataRollsBackAFailedBatch(t *testing.T) {
	h, mock := newMockSeedingHarness(t)
	records := attributeFreeRecords(3)
	statementErr := errors.New("disk full")

	mock.ExpectBegin()
	expectRecordStatements(mock, 1)
	mock.ExpectExec("INSERT INTO entity_main").WillReturnError(statementErr)
	mock.ExpectRollback()

	err := h.SeedHotRecordsWithData(context.Background(), records)
	if !errors.Is(err, statementErr) {
		t.Fatalf("SeedHotRecordsWithData error = %v, want it to wrap %v", err, statementErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("failed batch was not rolled back: %v", err)
	}
	if got := len(h.seededRecords["hot"]); got != 0 {
		t.Fatalf("seededRecords[hot] holds %d records after a failed seed, want 0", got)
	}
}
