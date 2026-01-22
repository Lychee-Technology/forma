package internal

import (
	"encoding/json"
	"fmt"

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
	if err := parseAttributesJSON(attrsJSON, record); err != nil {
		return nil, 0, err
	}

	cleanupEmptyMaps(record)

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

// newColumnScanBuffers creates scan buffers based on entityMainColumnDescriptors
func newColumnScanBuffers() *columnScanBuffers {
	textCount, smallCount, intCount, bigCount, doubleCount, uuidCount := 0, 0, 0, 0, 0, 0
	for _, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			textCount++
		case columnKindSmallint:
			smallCount++
		case columnKindInteger:
			intCount++
		case columnKindBigint:
			bigCount++
		case columnKindDouble:
			doubleCount++
		case columnKindUUID:
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
		typeIndex:  make([]int, len(entityMainColumnDescriptors)),
	}
}

// buildScanArgs creates scan arguments from buffers
func buildScanArgs(buffers *columnScanBuffers) []any {
	scanArgs := make([]any, 0, len(entityMainColumnDescriptors)+4)
	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

	for i, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			scanArgs = append(scanArgs, &buffers.textVals[textIdx])
			buffers.typeIndex[i] = textIdx
			textIdx++
		case columnKindSmallint:
			scanArgs = append(scanArgs, &buffers.smallVals[smallIdx])
			buffers.typeIndex[i] = smallIdx
			smallIdx++
		case columnKindInteger:
			scanArgs = append(scanArgs, &buffers.intVals[intIdx])
			buffers.typeIndex[i] = intIdx
			intIdx++
		case columnKindBigint:
			scanArgs = append(scanArgs, &buffers.bigVals[bigIdx])
			buffers.typeIndex[i] = bigIdx
			bigIdx++
		case columnKindDouble:
			scanArgs = append(scanArgs, &buffers.doubleVals[doubleIdx])
			buffers.typeIndex[i] = doubleIdx
			doubleIdx++
		case columnKindUUID:
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

	for i, desc := range entityMainColumnDescriptors {
		populateRecordField(record, desc, buffers, i)
	}

	return record
}

// populateRecordField sets a single field in the record from scan buffers
func populateRecordField(record *PersistentRecord, desc columnDescriptor, buffers *columnScanBuffers, idx int) {
	switch desc.kind {
	case columnKindText:
		val := buffers.textVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.TextItems[desc.name] = val.String
		}
	case columnKindSmallint:
		val := buffers.smallVals[buffers.typeIndex[idx]]
		if val.Valid {
			if desc.name == "ltbase_schema_id" {
				record.SchemaID = val.Int16
			} else {
				record.Int16Items[desc.name] = val.Int16
			}
		}
	case columnKindInteger:
		val := buffers.intVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.Int32Items[desc.name] = val.Int32
		}
	case columnKindBigint:
		val := buffers.bigVals[buffers.typeIndex[idx]]
		if val.Valid {
			switch desc.name {
			case "ltbase_created_at":
				record.CreatedAt = val.Int64
			case "ltbase_updated_at":
				record.UpdatedAt = val.Int64
			case "ltbase_deleted_at":
				record.DeletedAt = &val.Int64
			default:
				record.Int64Items[desc.name] = val.Int64
			}
		}
	case columnKindDouble:
		val := buffers.doubleVals[buffers.typeIndex[idx]]
		if val.Valid {
			record.Float64Items[desc.name] = val.Float64
		}
	case columnKindUUID:
		val := buffers.uuidVals[buffers.typeIndex[idx]]
		if val.Valid {
			if desc.name == "ltbase_row_id" {
				record.RowID = uuid.UUID(val.Bytes)
			} else {
				record.UUIDItems[desc.name] = uuid.UUID(val.Bytes)
			}
		}
	}
}

// parseAttributesJSON parses JSON-aggregated EAV attributes into the record
func parseAttributesJSON(attrsJSON []byte, record *PersistentRecord) error {
	if len(attrsJSON) == 0 || string(attrsJSON) == "[]" {
		return nil
	}

	var attributes []map[string]interface{}
	if err := json.Unmarshal(attrsJSON, &attributes); err != nil {
		return fmt.Errorf("unmarshal attributes json: %w", err)
	}

	record.OtherAttributes = make([]EAVRecord, 0, len(attributes))
	for _, attrObj := range attributes {
		attr := parseEAVAttribute(attrObj)
		record.OtherAttributes = append(record.OtherAttributes, attr)
	}

	return nil
}

// parseEAVAttribute converts a JSON object to an EAVRecord
func parseEAVAttribute(attrObj map[string]interface{}) EAVRecord {
	attr := EAVRecord{
		SchemaID: int16(attrObj["schema_id"].(float64)),
		AttrID:   int16(attrObj["attr_id"].(float64)),
	}

	if rowIDStr, ok := attrObj["row_id"].(string); ok {
		if parsedUUID, err := uuid.Parse(rowIDStr); err == nil {
			attr.RowID = parsedUUID
		}
	}

	if indices, ok := attrObj["array_indices"].(string); ok {
		attr.ArrayIndices = indices
	}

	if valueText, ok := attrObj["value_text"].(string); ok {
		attr.ValueText = &valueText
	}
	if valueNumeric, ok := attrObj["value_numeric"].(float64); ok {
		attr.ValueNumeric = &valueNumeric
	}

	return attr
}

// cleanupEmptyMaps removes empty maps from the record to avoid nil-map checks
func cleanupEmptyMaps(record *PersistentRecord) {
	if len(record.TextItems) == 0 {
		record.TextItems = nil
	}
	if len(record.Int16Items) == 0 {
		record.Int16Items = nil
	}
	if len(record.Int32Items) == 0 {
		record.Int32Items = nil
	}
	if len(record.Int64Items) == 0 {
		record.Int64Items = nil
	}
	if len(record.Float64Items) == 0 {
		record.Float64Items = nil
	}
	if len(record.UUIDItems) == 0 {
		record.UUIDItems = nil
	}
}
