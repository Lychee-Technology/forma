package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// singleRowQuerier is the read surface a single-row load needs. It is
// satisfied both by the pool (an ordinary Get) and by a pgx.Tx — which is
// what lets the update path read its merge base inside the write transaction
// (#457) through the very same loader.
type singleRowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// buildSingleRowSelect renders the one statement that answers a single-row
// read: the entity_main projection plus this row's EAV attributes aggregated
// into the same JSON shape the list path emits (advanced_query_template.go),
// so both read paths decode attributes through model.ParseAttributesJSON.
//
// One statement matters for correctness, not only for the saved round trip:
// the read it replaced ran entity_main and eav_data as two separate
// statements, possibly on two pooled connections, so a commit landing between
// them returned version N main columns beside version N+1 attributes — and
// the update path then persisted that torn merge base (#457). A single
// statement takes a single snapshot under every isolation level.
func buildSingleRowSelect(mainTable, eavTable string) string {
	return fmt.Sprintf(`SELECT %s, COALESCE(a.attributes_json, '[]') AS attributes_json
FROM %s m
LEFT JOIN LATERAL (
    SELECT JSON_AGG(
        JSON_BUILD_OBJECT(
            'schema_id', e.schema_id,
            'row_id', e.row_id,
            'attr_id', e.attr_id,
            'array_indices', e.array_indices,
            'value_text', e.value_text,
            'value_numeric', e.value_numeric
        ) ORDER BY e.attr_id, e.array_indices
    )::TEXT AS attributes_json
    FROM %s e
    WHERE e.schema_id = m.ltbase_schema_id AND e.row_id = m.ltbase_row_id
) a ON TRUE
WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2`,
		model.EntityMainProjection,
		sanitizeIdentifier(mainTable),
		sanitizeIdentifier(eavTable),
	)
}

// loadRecordWithAttributes reads one row's main columns and EAV attributes in
// a single statement on q. It answers (nil, nil) when the row does not exist.
func (r *DBPersistentRecordRepository) loadRecordWithAttributes(
	ctx context.Context,
	q singleRowQuerier,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
) (*model.PersistentRecord, error) {
	query := buildSingleRowSelect(tables.EntityMain, tables.EAVData)

	buffers := newColumnScanBuffers()
	scanArgs := buildScanArgs(buffers)
	var attrsJSON []byte
	scanArgs = append(scanArgs, &attrsJSON)

	if err := q.QueryRow(ctx, query, schemaID, rowID).Scan(scanArgs...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select entity row %s: %w", rowID, err)
	}

	record := buildRecordFromScanBuffers(buffers)
	if err := model.ParseAttributesJSON(attrsJSON, record); err != nil {
		return nil, fmt.Errorf("parse eav attributes for %s: %w", rowID, err)
	}
	model.CleanupEmptyMaps(record)

	return record, nil
}
