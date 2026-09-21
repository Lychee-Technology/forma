package main

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// A live best-effort failure carries its correlation id, and the import log
// shows it beside the published message so an operator can find the full
// error on the server log line (#398).
func TestFailureReasonShowsTheCorrelationID(t *testing.T) {
	got := failureReason(forma.OperationError{
		Code:    "CREATE_FAILED",
		Error:   "internal error",
		ErrorID: "0b1a3c6e-5d2f-4a8b-9c7d-1e2f3a4b5c6d",
	})
	want := "CREATE_FAILED: internal error (error_id 0b1a3c6e-5d2f-4a8b-9c7d-1e2f3a4b5c6d)"
	if got != want {
		t.Fatalf("failureReason = %q, want %q", got, want)
	}
}

// error_id is omitempty on the wire, so an entry without one (hand-built, or
// a payload from before #398) must not render an empty handle.
func TestFailureReasonOmitsAnAbsentCorrelationID(t *testing.T) {
	got := failureReason(forma.OperationError{Code: "CREATE_FAILED", Error: "internal error"})
	if want := "CREATE_FAILED: internal error"; got != want {
		t.Fatalf("failureReason = %q, want %q", got, want)
	}
}

// rowMapper maps every CSV record to its "name" column and rejects the record
// whose name is "bad", so a test can put an unmappable row inside a batch
// window.
type rowMapper struct{}

func (rowMapper) SchemaName() string       { return "visit" }
func (rowMapper) Mappings() []FieldMapping { return nil }
func (rowMapper) MapRecord(record map[string]string) (map[string]any, error) {
	if record["name"] == "bad" {
		return nil, &MappingError{CSVColumn: "name", RawValue: "bad", Reason: "rejected"}
	}
	return map[string]any{"name": record["name"]}, nil
}

// failNamedBatchCreator is an EntityBatchCreator that fails, best-effort,
// every operation whose name is in fail, reporting each by its index the way
// executeBestEffortBatch does.
type failNamedBatchCreator struct {
	fail map[string]bool
}

func (c failNamedBatchCreator) BatchCreate(_ context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	result := &forma.BatchResult{TotalCount: len(req.Operations)}
	for index, op := range req.Operations {
		name, _ := op.Data["name"].(string)
		if c.fail[name] {
			result.Failed = append(result.Failed, forma.OperationError{
				Index: index, Operation: op, Error: "internal error", Code: "CREATE_FAILED", ErrorID: "id-" + name,
			})
			continue
		}
		result.Successful = append(result.Successful, &forma.DataRecord{SchemaName: op.SchemaName})
	}
	return result, nil
}

// A failed operation is reported against the CSV row it came from, found by
// the operation's index in the batch — every operation carries the same
// schema, so nothing else tells them apart — and an unmappable row inside
// the batch window does not shift the rows after it.
func TestImportReportsAFailureAgainstItsOwnRow(t *testing.T) {
	csv := "name\nfirst\nsecond\nbad\nthird\nfourth\n"
	importer := NewCSVImporter(failNamedBatchCreator{fail: map[string]bool{"third": true, "fourth": true}}, rowMapper{}, 3)
	importer.SetLogger(zap.NewNop().Sugar())

	result, err := importer.ImportFromReader(context.Background(), strings.NewReader(csv))
	if err != nil {
		t.Fatalf("ImportFromReader: %v", err)
	}

	// Rows: 1 header, 2 first, 3 second, 4 bad (mapping), 5 third, 6 fourth.
	// Batches of 3 mapped rows: [first second third], [fourth].
	if result.SuccessCount != 2 || result.FailedCount != 3 {
		t.Fatalf("success/failed = %d/%d, want 2/3", result.SuccessCount, result.FailedCount)
	}
	got := map[int]string{}
	for _, importErr := range result.Errors {
		got[importErr.RowNumber] = importErr.Reason
	}
	want := map[int]string{
		4: "rejected",
		5: "CREATE_FAILED: internal error (error_id id-third)",
		6: "CREATE_FAILED: internal error (error_id id-fourth)",
	}
	if len(got) != len(want) {
		t.Fatalf("errors by row = %v, want %v", got, want)
	}
	for row, reason := range want {
		if got[row] != reason {
			t.Fatalf("row %d: reason %q, want %q (all: %v)", row, got[row], reason, got)
		}
	}
}
