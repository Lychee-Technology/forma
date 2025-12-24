package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

// initTestDescriptors sets a minimal entityMainColumnDescriptors used by optimized scans.
// Tests restore the original descriptors after completion to avoid interfering with other tests.
func initTestDescriptors() (restore func()) {
	orig := entityMainColumnDescriptors
	entityMainColumnDescriptors = []columnDescriptor{
		{name: "ltbase_schema_id", kind: columnKindSmallint},
		{name: "ltbase_row_id", kind: columnKindUUID},
		{name: "ltbase_created_at", kind: columnKindBigint},
		{name: "ltbase_updated_at", kind: columnKindBigint},
		{name: "ltbase_deleted_at", kind: columnKindBigint},
	}
	return func() { entityMainColumnDescriptors = orig }
}

// Test that StreamOptimizedQuery invokes the rowHandler once per returned row.
func TestStreamOptimizedQuery_RowHandlerInvokedPerRow(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Build 3 fake rows using column names derived from descriptors, plus attributes_json and pagination columns.
	columns := make([]string, 0, len(entityMainColumnDescriptors)+4)
	for _, d := range entityMainColumnDescriptors {
		columns = append(columns, d.name)
	}
	columns = append(columns, "attributes_json", "total_records", "total_pages", "current_page")

	rowID1 := uuid.New()
	rowID2 := uuid.New()
	rowID3 := uuid.New()

	rows := pgxmock.NewRows(columns).
		AddRow(int64(1), rowID1.String(), int64(1), int64(10), nil, []byte("[]"), int64(3), int64(1), int32(1)).
		AddRow(int64(1), rowID2.String(), int64(2), int64(20), nil, []byte("[]"), int64(3), int64(1), int32(1)).
		AddRow(int64(1), rowID3.String(), int64(3), int64(30), nil, []byte("[]"), int64(3), int64(1), int32(1))

	mock.ExpectQuery(`SELECT .*`).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(rows)

	repo := NewPostgresPersistentRecordRepository(mock, nil)

	counter := 0
	var captured []*PersistentRecord

	total, err := repo.StreamOptimizedQuery(context.Background(), StorageTables{EntityMain: "entity_main", EAVData: "eav"}, 1, "1=1", nil, 10, 0, nil, true, func(rp *PersistentRecord) error {
		counter++
		captured = append(captured, rp)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, 3, counter)
	require.Len(t, captured, 3)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test that scanOptimizedRow produces nil maps and nil OtherAttributes when there is no data.
func TestScanOptimizedRow_EmptyAttributes_NilMaps(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.New()
	columns := make([]string, 0, len(entityMainColumnDescriptors)+4)
	for _, d := range entityMainColumnDescriptors {
		columns = append(columns, d.name)
	}
	columns = append(columns, "attributes_json", "total_records", "total_pages", "current_page")

	rows := pgxmock.NewRows(columns).
		AddRow(int64(1), rowID.String(), int64(0), int64(0), nil, []byte("[]"), int64(1), int64(1), int32(1))

	mock.ExpectQuery(`SELECT .*`).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(rows)

	repo := NewPostgresPersistentRecordRepository(mock, nil)

	var recs []*PersistentRecord
	total, err := repo.StreamOptimizedQuery(context.Background(), StorageTables{EntityMain: "entity_main", EAVData: "eav"}, 1, "1=1", nil, 10, 0, nil, true, func(rp *PersistentRecord) error {
		recs = append(recs, rp)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, recs, 1)
	rec := recs[0]

	// Maps should be nil when empty per format consistency rules
	require.Nil(t, rec.TextItems)
	require.Nil(t, rec.Int16Items)
	require.Nil(t, rec.Int32Items)
	require.Nil(t, rec.Int64Items)
	require.Nil(t, rec.Float64Items)
	require.Nil(t, rec.UUIDItems)
	require.Nil(t, rec.OtherAttributes)

	require.NoError(t, mock.ExpectationsWereMet())
}
