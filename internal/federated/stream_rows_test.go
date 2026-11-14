package federated

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type stubDuckDBRows struct {
	nextResults []bool
	scanFn      func(dest ...any) error
	err         error
	nextIndex   int
}

func (s *stubDuckDBRows) Next() bool {
	if s.nextIndex >= len(s.nextResults) {
		return false
	}
	result := s.nextResults[s.nextIndex]
	s.nextIndex++
	return result
}

func (s *stubDuckDBRows) Scan(dest ...any) error {
	if s.scanFn != nil {
		return s.scanFn(dest...)
	}
	return nil
}

func (s *stubDuckDBRows) Err() error {
	return s.err
}

func (s *stubDuckDBRows) Close() error {
	return nil
}

// A row the parquet leg cannot scan fails the leg as a read failure.
func TestStreamParquetLegRows_WhenScanFails_ItStopsAndReturnsScanError(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rows := &stubDuckDBRows{
		nextResults: []bool{true, true, false},
		scanFn: func(dest ...any) error {
			return fmt.Errorf("forced scan failure")
		},
	}

	out, total, totalSet, err := streamParquetLegRows(rows, sortkey.Order{})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "scan parquet leg row")
	require.Contains(t, err.Error(), "forced scan failure")
	require.Empty(t, out)
	require.Zero(t, total)
	require.False(t, totalSet)
	require.Equal(t, 1, rows.nextIndex, "the leg stops at the row it cannot scan")
}

// An iterator that ends with an error fails the leg, whatever rows it
// yielded before.
func TestStreamParquetLegRows_WhenIteratorEndsWithError_ItReturnsIterationError(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rowID := uuid.New()
	rows := &stubDuckDBRows{
		nextResults: []bool{true, false},
		err:         fmt.Errorf("forced rows iteration failure"),
		scanFn: func(dest ...any) error {
			return populateDuckDBRow(dest, 7, rowID, 101, 202, nil, "[]", 3)
		},
	}

	out, total, totalSet, err := streamParquetLegRows(rows, sortkey.Order{})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "iterate parquet leg rows")
	require.Contains(t, err.Error(), "forced rows iteration failure")
	require.Empty(t, out)
	require.Zero(t, total)
	require.False(t, totalSet)
}

func populateDuckDBRow(dest []any, schemaID int16, rowID uuid.UUID, createdAt int64, updatedAt int64, deletedAt *int64, attrsJSON string, totalRecords int64) error {
	if len(dest) < len(model.EntityMainColumnDescriptors)+4 {
		return fmt.Errorf("insufficient scan destinations")
	}

	for i, scanDest := range dest[:len(model.EntityMainColumnDescriptors)] {
		switch typed := scanDest.(type) {
		case *nullDuckDBUUID:
			if model.EntityMainColumnDescriptors[i].Name == "ltbase_row_id" {
				typed.UUID = rowID
				typed.Valid = true
			}
		case *sql.NullInt64:
			switch model.EntityMainColumnDescriptors[i].Name {
			case "ltbase_schema_id":
				typed.Int64 = int64(schemaID)
				typed.Valid = true
			case "ltbase_created_at":
				typed.Int64 = createdAt
				typed.Valid = true
			case "ltbase_updated_at":
				typed.Int64 = updatedAt
				typed.Valid = true
			case "ltbase_deleted_at":
				if deletedAt != nil {
					typed.Int64 = *deletedAt
					typed.Valid = true
				}
			}
		}
	}

	attrsDest, ok := dest[len(model.EntityMainColumnDescriptors)].(*sql.NullString)
	if !ok {
		return fmt.Errorf("attributes destination has unexpected type")
	}
	attrsDest.String = attrsJSON
	attrsDest.Valid = true

	totalDest, ok := dest[len(model.EntityMainColumnDescriptors)+1].(*sql.NullInt64)
	if !ok {
		return fmt.Errorf("total destination has unexpected type")
	}
	totalDest.Int64 = totalRecords
	totalDest.Valid = true

	return nil
}
