package internal

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

const attributesCount = 6

func buildAttributeValuesClause(attributes []model.EAVRecord) (string, []any, error) {
	if len(attributes) == 0 {
		return "", nil, nil
	}
	var values []string
	args := make([]any, 0, len(attributes)*attributesCount)
	for idx, attr := range attributes {
		placeholderBase := idx*attributesCount + 1
		placeholders := make([]string, attributesCount)
		for i := range attributesCount {
			placeholders[i] = fmt.Sprintf("$%d", placeholderBase+i)
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
		args = append(args,
			attr.SchemaID,
			attr.RowID,
			attr.AttrID,
			attr.ArrayIndices,
			attr.ValueText,
			attr.ValueNumeric,
		)
	}
	return strings.Join(values, ", "), args, nil
}

func (r *DBPersistentRecordRepository) insertEAVAttributes(ctx context.Context, tx pgx.Tx, table string, attributes []model.EAVRecord) error {
	if len(attributes) == 0 {
		return nil
	}

	const batchSize = 500
	for i := 0; i < len(attributes); i += batchSize {
		end := min(i+batchSize, len(attributes))

		valuesClause, args, err := buildAttributeValuesClause(attributes[i:end])
		if err != nil {
			return err
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric) VALUES %s",
			sanitizeIdentifier(table),
			valuesClause,
		)
		zap.S().Debugw("insert EAV attributes", "query", query, "args", args)
		// Payload-level primary-key collisions are collapsed upstream by
		// transform.dedupeEAVRecords (#312). Classify anyway so any residual
		// 23505 — a concurrent writer racing the same (schema_id, row_id) —
		// answers 409 like the entity_main sites rather than a redacted 500.
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("insert eav attributes: %w", classifyPgError(err))
		}
	}

	return nil
}

// knownAttrIDs returns the sorted attributeIDs the current schema metadata
// can address. It is the #294 tolerate-and-preserve boundary: EAV rows whose
// attrID is outside this set were removed by schema evolution and must
// survive an update untouched, so a later schema re-add restores them.
func (r *DBPersistentRecordRepository) knownAttrIDs(schemaID int16) ([]int16, error) {
	if r.metadataCache == nil {
		return nil, fmt.Errorf("no metadata cache configured: cannot scope eav delete for schema %d", schemaID)
	}
	cache, ok := r.metadataCache.GetSchemaCacheByID(schemaID)
	if !ok {
		return nil, fmt.Errorf("no cache for schema id %d: cannot scope eav delete", schemaID)
	}
	ids := make([]int16, 0, len(cache))
	for _, meta := range cache {
		ids = append(ids, meta.AttributeID)
	}
	// Map iteration order is random; sort for a deterministic bind value, then
	// compact because distinct attribute names may share an attributeID.
	slices.Sort(ids)
	return slices.Compact(ids), nil
}

func (r *DBPersistentRecordRepository) replaceEAVAttributes(ctx context.Context, tx pgx.Tx, table string, schemaID int16, rowID uuid.UUID, attributes []model.EAVRecord) error {
	knownIDs, err := r.knownAttrIDs(schemaID)
	if err != nil {
		return fmt.Errorf("resolve replace scope: %w", err)
	}
	deleteQuery := fmt.Sprintf(
		"DELETE FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = ANY($3)",
		sanitizeIdentifier(table),
	)
	if _, err := tx.Exec(ctx, deleteQuery, schemaID, rowID, knownIDs); err != nil {
		return fmt.Errorf("delete existing eav attributes: %w", err)
	}
	return r.insertEAVAttributes(ctx, tx, table, attributes)
}

func (r *DBPersistentRecordRepository) fetchAttributes(ctx context.Context, table string, schemaID int16, rowID uuid.UUID) ([]model.EAVRecord, error) {
	query := fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric FROM %s WHERE schema_id = $1 AND row_id = $2",
		sanitizeIdentifier(table),
	)
	rows, err := r.pool.Query(ctx, query, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("query eav attributes: %w", err)
	}
	defer rows.Close()

	var attributes []model.EAVRecord
	for rows.Next() {
		var attr model.EAVRecord
		if err := rows.Scan(
			&attr.SchemaID,
			&attr.RowID,
			&attr.AttrID,
			&attr.ArrayIndices,
			&attr.ValueText,
			&attr.ValueNumeric,
		); err != nil {
			return nil, fmt.Errorf("scan eav attribute: %w", err)
		}
		attributes = append(attributes, attr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate eav attributes: %w", err)
	}

	return attributes, nil
}
