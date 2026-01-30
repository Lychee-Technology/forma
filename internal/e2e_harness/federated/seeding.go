package federated

import (
	"context"
	"database/sql"
	"fmt"
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
	_, err := h.PGDB.ExecContext(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET
			ltbase_updated_at = $4,
			ltbase_deleted_at = $5
	`, r.SchemaID, r.RowID, r.ChangedAt, r.ChangedAt, sql.NullInt64{Int64: r.DeletedAt, Valid: r.DeletedAt > 0})
	if err != nil {
		return fmt.Errorf("insert entity_main: %w", err)
	}
	return nil
}

// insertEAVData inserts attribute values into the eav_data table.
func (h *FederatedTestHarness) insertEAVData(ctx context.Context, r TestRecord) error {
	attrID := 1
	for name, value := range r.Attributes {
		var valueText sql.NullString
		var valueNumeric sql.NullFloat64

		switch v := value.(type) {
		case string:
			valueText = sql.NullString{String: v, Valid: true}
		case int, int64, float64:
			valueNumeric = sql.NullFloat64{Float64: toFloat64(v), Valid: true}
		default:
			valueText = sql.NullString{String: fmt.Sprintf("%v", v), Valid: true}
		}

		_, err := h.PGDB.ExecContext(ctx, `
			INSERT INTO eav_data (schema_id, row_id, attr_id, value_text, value_numeric)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (schema_id, row_id, attr_id) DO UPDATE SET
				value_text = $4,
				value_numeric = $5
		`, r.SchemaID, r.RowID, attrID, valueText, valueNumeric)
		if err != nil {
			return fmt.Errorf("insert eav_data for %s: %w", name, err)
		}
		attrID++
	}
	return nil
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

	for i := 0; i < versions; i++ {
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
