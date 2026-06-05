package federated

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// SeedBaseRecords creates records directly in S3 base files.
func (h *FederatedTestHarness) SeedBaseRecords(ctx context.Context, count int) ([]TestRecord, error) {
	records := GenerateTestRecords(count, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 720, // 30 days ago
		TimeOffset:     720,
	})

	if err := h.WriteParquet(ctx, "base", "base.parquet", records); err != nil {
		return nil, err
	}

	h.seededRecords["base"] = records
	return records, nil
}

// SeedDeltaRecords creates records in S3 delta files.
func (h *FederatedTestHarness) SeedDeltaRecords(ctx context.Context, count int) ([]TestRecord, error) {
	records := GenerateTestRecords(count, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 48, // 2 days ago
		TimeOffset:     48,
	})

	if err := h.WriteParquet(ctx, "delta", "delta.parquet", records); err != nil {
		return nil, err
	}

	h.seededRecords["delta"] = records
	return records, nil
}

// SeedHotRecords creates records in Postgres hot buffer (unflushed).
func (h *FederatedTestHarness) SeedHotRecords(ctx context.Context, count int) ([]TestRecord, error) {
	records := GenerateTestRecords(count, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 1, // Last hour
		TimeOffset:     0,
	})

	for _, r := range records {
		if err := h.insertHotRecord(ctx, r); err != nil {
			return nil, err
		}
	}

	h.seededRecords["hot"] = records
	return records, nil
}

// SeedAllTiers seeds data across all three tiers.
func (h *FederatedTestHarness) SeedAllTiers(ctx context.Context, base, delta, hot int) error {
	if _, err := h.SeedBaseRecords(ctx, base); err != nil {
		return fmt.Errorf("seed base: %w", err)
	}
	if _, err := h.SeedDeltaRecords(ctx, delta); err != nil {
		return fmt.Errorf("seed delta: %w", err)
	}
	if _, err := h.SeedHotRecords(ctx, hot); err != nil {
		return fmt.Errorf("seed hot: %w", err)
	}
	return nil
}

// SeedHotRecordsWithData inserts specific test records into hot buffer.
func (h *FederatedTestHarness) SeedHotRecordsWithData(ctx context.Context, records []TestRecord) error {
	for _, r := range records {
		if err := h.insertHotRecord(ctx, r); err != nil {
			return err
		}
	}
	h.seededRecords["hot"] = append(h.seededRecords["hot"], records...)
	return nil
}

// insertHotRecord inserts a single record into Postgres (entity_main, eav_data, change_log).
func (h *FederatedTestHarness) insertHotRecord(ctx context.Context, r TestRecord) error {
	now := time.Now().UnixMilli()
	if r.ChangedAt == 0 {
		r.ChangedAt = now
	}

	// Insert into entity_main
	if err := h.insertEntityMain(ctx, r); err != nil {
		return err
	}

	// Insert into eav_data for each attribute
	if err := h.insertEAVData(ctx, r); err != nil {
		return err
	}

	// Insert into change_log (unflushed)
	if err := h.insertChangeLog(ctx, r); err != nil {
		return err
	}

	return nil
}

// insertEntityMain inserts a record into the entity_main table.
func (h *FederatedTestHarness) insertEntityMain(ctx context.Context, r TestRecord) error {
	text01, text02, smallint01, bigint01, bigint02, double01, uuid01 := benchmarkMainColumnValues(r)
	_, err := h.PGDB.ExecContext(ctx, `
		INSERT INTO entity_main (
			ltbase_schema_id, ltbase_row_id,
			text_01, text_02, smallint_01, bigint_01, bigint_02, double_01, uuid_01,
			ltbase_created_at, ltbase_updated_at, ltbase_deleted_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET
			text_01 = $3,
			text_02 = $4,
			smallint_01 = $5,
			bigint_01 = $6,
			bigint_02 = $7,
			double_01 = $8,
			uuid_01 = $9,
			ltbase_updated_at = $11,
			ltbase_deleted_at = $12
	`, r.SchemaID, r.RowID, text01, text02, smallint01, bigint01, bigint02, double01, uuid01, r.ChangedAt, r.ChangedAt, sql.NullInt64{Int64: r.DeletedAt, Valid: r.DeletedAt > 0})
	if err != nil {
		return fmt.Errorf("insert entity_main: %w", err)
	}
	return nil
}

// insertEAVData inserts attribute values into the eav_data table.
func (h *FederatedTestHarness) insertEAVData(ctx context.Context, r TestRecord) error {
	keys := make([]string, 0, len(r.Attributes))
	for name := range r.Attributes {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		value := r.Attributes[name]
		valueText, valueNumeric := eavValueColumns(value)

		attrID, shouldInsert, err := h.attributeIDForRecord(r.SchemaID, name)
		if err != nil {
			return fmt.Errorf("resolve attribute id for %s: %w", name, err)
		}
		if !shouldInsert {
			continue
		}

		_, err = h.PGDB.ExecContext(ctx, `
			INSERT INTO eav_data (schema_id, row_id, attr_id, array_indices, value_text, value_numeric)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (schema_id, row_id, attr_id, array_indices) DO UPDATE SET
				value_text = EXCLUDED.value_text,
				value_numeric = EXCLUDED.value_numeric
		`, r.SchemaID, r.RowID, attrID, "", valueText, valueNumeric)
		if err != nil {
			return fmt.Errorf("insert eav_data for %s: %w", name, err)
		}
	}
	return nil
}

func eavValueColumns(value any) (sql.NullString, sql.NullFloat64) {
	var valueText sql.NullString
	var valueNumeric sql.NullFloat64

	switch v := value.(type) {
	case string:
		valueText = sql.NullString{String: v, Valid: true}
	case bool:
		if v {
			valueNumeric = sql.NullFloat64{Float64: 1, Valid: true}
		} else {
			valueNumeric = sql.NullFloat64{Float64: 0, Valid: true}
		}
	case int, int64, float64:
		valueNumeric = sql.NullFloat64{Float64: toFloat64(v), Valid: true}
	default:
		valueText = sql.NullString{String: fmt.Sprintf("%v", v), Valid: true}
	}

	return valueText, valueNumeric
}

func (h *FederatedTestHarness) attributeIDForRecord(schemaID int16, name string) (int16, bool, error) {
	if h.Registry == nil {
		return int16(DeterministicAttributeID(schemaID, name)), true, nil
	}
	_, cache, err := h.Registry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		return 0, false, err
	}
	if cache == nil {
		return 0, false, fmt.Errorf("schema %d not found in registry", schemaID)
	}
	metadata, ok := cache[name]
	if !ok {
		if isBenchmarkMetadataAttribute(name) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("attribute %s not found in schema cache for schema %d", name, schemaID)
	}
	return metadata.AttributeID, true, nil
}

func isBenchmarkMetadataAttribute(name string) bool {
	switch name {
	case "name", "version":
		return true
	default:
		return false
	}
}

// DeterministicAttributeID returns a stable test-only attribute ID for a schema/name pair.
// Deprecated: benchmark-backed seeding should use schema metadata IDs via FederatedTestHarness.Registry.
func DeterministicAttributeID(schemaID int16, name string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strconv.FormatInt(int64(schemaID), 10)))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(name))
	return int(h.Sum32()%30000) + 1
}

func benchmarkMainColumnValues(r TestRecord) (sql.NullString, sql.NullString, sql.NullInt16, sql.NullInt64, sql.NullInt64, sql.NullFloat64, sql.NullString) {
	attrs := r.Attributes
	text01 := nullableString(firstNonNil(attrs, "taxId", "symbol"))
	text02 := nullableString(firstNonNil(attrs, "region"))
	smallint01 := nullableInt16(firstNonNil(attrs, "status", "sector", "tradeType"))
	bigint01 := nullableInt64(firstNonNil(attrs, "quantity"))
	bigint02 := nullableUnixMillis(firstNonNil(attrs, "tradeTime"))
	double01 := nullableFloat64(firstNonNil(attrs, "price"))
	uuid01 := nullableUUIDString(firstNonNil(attrs, "customerId"))
	return text01, text02, smallint01, bigint01, bigint02, double01, uuid01
}

func firstNonNil(attrs map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := attrs[key]; ok {
			return value
		}
	}
	return nil
}

func nullableString(value any) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: fmt.Sprint(value), Valid: true}
}

func nullableUUIDString(value any) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	parsed := fmt.Sprint(value)
	if _, err := uuid.Parse(parsed); err != nil {
		return sql.NullString{}
	}
	return sql.NullString{String: parsed, Valid: true}
}

func nullableInt16(value any) sql.NullInt16 {
	if value == nil {
		return sql.NullInt16{}
	}
	return sql.NullInt16{Int16: int16(toFloat64(value)), Valid: true}
}

func nullableInt64(value any) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(toFloat64(value)), Valid: true}
}

func nullableFloat64(value any) sql.NullFloat64 {
	if value == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: toFloat64(value), Valid: true}
}

func nullableUnixMillis(value any) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	switch v := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return sql.NullInt64{Int64: parsed.UnixMilli(), Valid: true}
		}
	case int64:
		return sql.NullInt64{Int64: v, Valid: true}
	case int:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	case float64:
		return sql.NullInt64{Int64: int64(v), Valid: true}
	}
	return sql.NullInt64{}
}

// insertChangeLog inserts a record into the change_log table.
func (h *FederatedTestHarness) insertChangeLog(ctx context.Context, r TestRecord) error {
	_, err := h.PGDB.ExecContext(ctx, `
		INSERT INTO change_log (schema_id, row_id, changed_at, deleted_at, flushed_at)
		VALUES ($1, $2, $3, $4, 0)
		ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET
			changed_at = $3,
			deleted_at = $4
	`, r.SchemaID, r.RowID, r.ChangedAt, r.DeletedAt)
	if err != nil {
		return fmt.Errorf("insert change_log: %w", err)
	}
	return nil
}

// InsertOverlappingRecords creates multiple versions of the same row_id across tiers.
func (h *FederatedTestHarness) InsertOverlappingRecords(ctx context.Context, rowID uuid.UUID, versions int) error {
	baseTime := time.Now()

	for i := range versions {
		record := TestRecord{
			RowID:    rowID,
			SchemaID: h.SchemaID,
			Attributes: map[string]any{
				"name":    fmt.Sprintf("Version %d", i+1),
				"version": i + 1,
			},
			ChangedAt: baseTime.Add(-time.Duration(versions-i) * time.Hour).UnixMilli(),
		}

		tier := h.selectTierForVersion(i, versions)
		if err := h.insertRecordToTier(ctx, tier, record, rowID); err != nil {
			return err
		}
	}

	return nil
}

// selectTierForVersion determines which tier to use based on version index.
func (h *FederatedTestHarness) selectTierForVersion(versionIdx, totalVersions int) string {
	if versionIdx == 0 {
		return "base"
	}
	if versionIdx == 1 && totalVersions > 2 {
		return "delta"
	}
	return "hot"
}

// insertRecordToTier inserts a record into the specified tier.
func (h *FederatedTestHarness) insertRecordToTier(ctx context.Context, tier string, record TestRecord, rowID uuid.UUID) error {
	switch tier {
	case "base":
		return h.WriteParquet(ctx, "base", fmt.Sprintf("overlap_base_%s.parquet", rowID), []TestRecord{record})
	case "delta":
		return h.WriteParquet(ctx, "delta", fmt.Sprintf("overlap_delta_%s.parquet", rowID), []TestRecord{record})
	case "hot":
		return h.insertHotRecord(ctx, record)
	default:
		return fmt.Errorf("unknown tier: %s", tier)
	}
}

// MarkRecordDeleted marks a record as soft-deleted.
func (h *FederatedTestHarness) MarkRecordDeleted(ctx context.Context, rowID uuid.UUID) error {
	now := time.Now().UnixMilli()

	_, err := h.PGDB.ExecContext(ctx, `
		UPDATE entity_main SET ltbase_deleted_at = $1, ltbase_updated_at = $1
		WHERE ltbase_row_id = $2 AND ltbase_schema_id = $3
	`, now, rowID, h.SchemaID)
	if err != nil {
		return err
	}

	_, err = h.PGDB.ExecContext(ctx, `
		UPDATE change_log SET deleted_at = $1, changed_at = $1
		WHERE row_id = $2 AND schema_id = $3
	`, now, rowID, h.SchemaID)

	return err
}

// ClearChangeLog removes all records from change_log table.
func (h *FederatedTestHarness) ClearChangeLog(ctx context.Context) error {
	_, err := h.PGDB.ExecContext(ctx, `DELETE FROM change_log WHERE schema_id = $1`, h.SchemaID)
	return err
}

// ClearAllData removes all test data.
func (h *FederatedTestHarness) ClearAllData(ctx context.Context) error {
	tables := []string{"change_log", "eav_data", "entity_main"}
	for _, t := range tables {
		if _, err := h.PGDB.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 OR ltbase_schema_id = $1", t), h.SchemaID); err != nil {
			// Ignore errors for tables with different column names
			_, _ = h.PGDB.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", t))
		}
	}

	// Clear S3
	_ = h.deleteS3Prefix(ctx, h.S3Prefix)
	h.seededRecords = make(map[string][]TestRecord)
	return nil
}
