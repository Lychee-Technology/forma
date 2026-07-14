package federated

// Shared engine-seam test fakes, extracted from engine_test.go as a pure
// move to keep it under the 500-line limit.

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

type fakePostgresFederatedSource struct {
	queryCalls int
	lastQuery  *model.PersistentRecordQuery
	page       *model.PersistentRecordPage
}

func (f *fakePostgresFederatedSource) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	f.queryCalls++
	f.lastQuery = query
	if f.page != nil {
		return f.page, nil
	}
	return &model.PersistentRecordPage{}, nil
}

func (f *fakePostgresFederatedSource) RunOptimizedQuery(ctx context.Context, tables model.StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []model.AttributeOrder, useMainTableAsAnchor bool) ([]*model.PersistentRecord, int64, error) {
	return nil, 0, nil
}

func (f *fakePostgresFederatedSource) BuildHybridConditions(tables model.StorageTables, fq *model.FederatedAttributeQuery) (string, []any, error) {
	return "1=1", nil, nil
}

type fakeDirtyIDFetcher struct {
	calls int
	ids   []uuid.UUID
	err   error
}

func (f *fakeDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.ids, nil
}

type fakeDuckDBExecutor struct {
	calls         int
	describeCalls int
	lastSQL       string
	lastArgs      []any
	err           error
	rows          duckDBRowsIterator
}

func (f *fakeDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") {
		// The pre-read schema validator (#189) probes each concrete parquet
		// path before the main scan; answer with the invariant-satisfying
		// system columns so engine-seam tests keep exercising the main query
		// without the probe consuming the canned rows or lastSQL. Probes are
		// counted separately so breaker tests can assert none reach storage.
		f.describeCalls++
		return &fakeDescribeRows{cols: [][2]string{
			{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"},
		}}, nil
	}
	if strings.HasPrefix(sql, "SELECT file FROM glob(") {
		// Glob expansion probe (#189): listing is not faked — inconclusive,
		// mirroring an unreachable store; the validator defers to the main
		// read.
		f.describeCalls++
		return nil, fmt.Errorf("glob listing not faked")
	}
	f.calls++
	f.lastSQL = sql
	f.lastArgs = args
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

type singleDuckDBRow struct {
	rowID   uuid.UUID
	scanned bool
}

func (r *singleDuckDBRow) Next() bool {
	if r.scanned {
		return false
	}
	r.scanned = true
	return true
}

func (r *singleDuckDBRow) Scan(dest ...any) error {
	return populateDuckDBRow(dest, 7, r.rowID, 10, 20, nil, "[]", 1)
}

func (r *singleDuckDBRow) Err() error {
	return nil
}

func (r *singleDuckDBRow) Close() error {
	return nil
}
