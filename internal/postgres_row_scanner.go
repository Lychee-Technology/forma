package internal

import (
	"fmt"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// scanOptimizedRow scans a single row from the optimized query that includes
// entity_main columns plus JSON-aggregated EAV attributes.
func (r *DBPersistentRecordRepository) scanOptimizedRow(rows pgx.Rows) (*PersistentRecord, int64, error) {
	var (
		attrsJSON    []byte
		totalRecords int64
		totalPages   int64
		currentPage  int32
	)

	scanBuffers := newColumnScanBuffers()
	scanArgs := buildScanArgs(scanBuffers)

	// Add JSON and pagination info scan targets
	scanArgs = append(scanArgs, &attrsJSON, &totalRecords, &totalPages, &currentPage)

	if err := rows.Scan(scanArgs...); err != nil {
		return nil, 0, fmt.Errorf("scan optimized row: %w", err)
	}

	record := buildRecordFromScanBuffers(scanBuffers)

	// Parse JSON attributes
	if err := model.ParseAttributesJSON(attrsJSON, record); err != nil {
		return nil, 0, err
	}

	model.CleanupEmptyMaps(record)

	return record, totalRecords, nil
}

// columnScanBuffers holds typed buffers for scanning main table columns
type columnScanBuffers struct {
	textVals   []pgtype.Text
	smallVals  []pgtype.Int2
	intVals    []pgtype.Int4
	bigVals    []pgtype.Int8
	doubleVals []pgtype.Float8
	uuidVals   []pgtype.UUID
	typeIndex  []int
}

// newColumnScanBuffers creates scan buffers based on model.EntityMainColumnDescriptors
func newColumnScanBuffers() *columnScanBuffers {
	textCount, smallCount, intCount, bigCount, doubleCount, uuidCount := 0, 0, 0, 0, 0, 0
	for _, desc := range model.EntityMainColumnDescriptors {
		switch desc.Kind {
		case model.ColumnKindText:
			textCount++
		case model.ColumnKindSmallint:
			smallCount++
		case model.ColumnKindInteger:
			intCount++
		case model.ColumnKindBigint:
			bigCount++
		case model.ColumnKindDouble:
			doubleCount++
		case model.ColumnKindUUID:
			uuidCount++
		}
	}

	return &columnScanBuffers{
		textVals:   make([]pgtype.Text, textCount),
		smallVals:  make([]pgtype.Int2, smallCount),
		intVals:    make([]pgtype.Int4, intCount),
		bigVals:    make([]pgtype.Int8, bigCount),
		doubleVals: make([]pgtype.Float8, doubleCount),
		uuidVals:   make([]pgtype.UUID, uuidCount),
		typeIndex:  make([]int, len(model.EntityMainColumnDescriptors)),
	}
}

// buildScanArgs creates scan arguments from buffers
func buildScanArgs(buffers *columnScanBuffers) []any {
	scanArgs := make([]any, 0, len(model.EntityMainColumnDescriptors)+4)
	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

	for i, desc := range model.EntityMainColumnDescriptors {
		switch desc.Kind {
		case model.ColumnKindText:
			scanArgs = append(scanArgs, &buffers.textVals[textIdx])
			buffers.typeIndex[i] = textIdx
			textIdx++
		case model.ColumnKindSmallint:
			scanArgs = append(scanArgs, &buffers.smallVals[smallIdx])
			buffers.typeIndex[i] = smallIdx
			smallIdx++
		case model.ColumnKindInteger:
			scanArgs = append(scanArgs, &buffers.intVals[intIdx])
			buffers.typeIndex[i] = intIdx
			intIdx++
		case model.ColumnKindBigint:
			scanArgs = append(scanArgs, &buffers.bigVals[bigIdx])
			buffers.typeIndex[i] = bigIdx
			bigIdx++
		case model.ColumnKindDouble:
			scanArgs = append(scanArgs, &buffers.doubleVals[doubleIdx])
			buffers.typeIndex[i] = doubleIdx
			doubleIdx++
		case model.ColumnKindUUID:
			scanArgs = append(scanArgs, &buffers.uuidVals[uuidIdx])
			buffers.typeIndex[i] = uuidIdx
			uuidIdx++
		}
	}

	return scanArgs
}

// buildRecordFromScanBuffers creates a PersistentRecord from scanned values
func buildRecordFromScanBuffers(buffers *columnScanBuffers) *PersistentRecord {
	record := &PersistentRecord{
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UUIDItems:    make(map[string]uuid.UUID),
	}

	for i, desc := range model.EntityMainColumnDescriptors {
		populateRecordField(record, desc, buffers, i)
	}

	return record
}

// populateRecordField sets a single field in the record from scan buffers
func populateRecordField(record *PersistentRecord, desc model.ColumnDescriptor, buffers *columnScanBuffers, idx int) {
	switch desc.Kind {
	case model.ColumnKindText:
		val := buffers.textVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.TextItems[desc.Name] = val.String
		}
	case model.ColumnKindSmallint:
		val := buffers.smallVals[buffers.typeIndex[idx]]
		if val.Valid {
			if desc.Name == "ltbase_schema_id" {
				record.SchemaID = val.Int16
			} else {
				record.Int16Items[desc.Name] = val.Int16
			}
		}
	case model.ColumnKindInteger:
		val := buffers.intVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.Int32Items[desc.Name] = val.Int32
		}
	case model.ColumnKindBigint:
		val := buffers.bigVals[buffers.typeIndex[idx]]
		if val.Valid {
			switch desc.Name {
			case "ltbase_created_at":
				record.CreatedAt = val.Int64
			case "ltbase_updated_at":
				record.UpdatedAt = val.Int64
			case "ltbase_deleted_at":
				record.DeletedAt = &val.Int64
			default:
				record.Int64Items[desc.Name] = val.Int64
			}
		}
	case model.ColumnKindDouble:
		val := buffers.doubleVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.Float64Items[desc.Name] = val.Float64
		}
	case model.ColumnKindUUID:
		val := buffers.uuidVals[buffers.typeIndex[idx]]
		if val.Valid {
			if desc.Name == "ltbase_row_id" {
				record.RowID = uuid.UUID(val.Bytes)
			} else {
				record.UUIDItems[desc.Name] = uuid.UUID(val.Bytes)
			}
		}
	}
}
