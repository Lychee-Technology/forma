package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// The pgx hot leg's PostgreSQL reads (design.md §6.1.1; pgx-hot-leg-plan.md
// step 4). A pgx pass reads, in one REPEATABLE READ READ ONLY snapshot, the
// dirty set (federated.readChangeLogSnapshot), the hot keys, and after the
// merge the page's hot rows. Both reads below therefore take the pass's
// transaction rather than the pool. Nothing in the engine calls them until
// the transport switch (plan step 5).
//
// The hot keys filter exactly as a PostgreSQL-only answer does: the
// condition renders through the PostgreSQL route's own builder
// (buildHybridConditions), so text ranges keep its COLLATE "C" byte order
// (#728) and every other operator its spelling, and the page's rows read
// through that route's whole-row reader (rowSelectWithAttributes).

// hotKeysRowIDIndex and hotKeysVerTSIndex are the select-list positions of
// the two values every hot key reads; the sort columns' stored values follow.
const (
	hotKeysRowIDIndex = 0
	hotKeysVerTSIndex = 1
	// hotKeysConditionArgIndex is the last placeholder before the condition's
	// first: $1 is the schema ID, $2 the cutoff.
	hotKeysConditionArgIndex = 2
)

// hotKeysSQL is the hot-keys query. hot_window is the dirty window in the
// same two index ranges as the dirty set's read (dirtySetQuery), each a
// range of the (schema_id, flushed_at) index init-db builds; a row's ver_ts
// is the changed_at of its newest entry there, as the attach path's
// pg_source ranks it. The join keeps only live rows, since a delete removes
// the entity_main row. There is no ORDER BY and no LIMIT: Go sorts the keys,
// and the dirty set bounds them (design.md §6.1.1, Ordering parity).
//
// %[1]s change_log, %[2]s entity_main, %[3]s the sort columns' projections,
// %[4]s the condition over the alias m.
const hotKeysSQL = `WITH hot_window AS (
    SELECT entries.row_id, MAX(entries.changed_at) AS ver_ts
    FROM (
        SELECT row_id, changed_at FROM %[1]s WHERE schema_id = $1 AND flushed_at = 0
        UNION ALL
        SELECT row_id, changed_at FROM %[1]s WHERE schema_id = $1 AND flushed_at >= $2
    ) entries
    GROUP BY entries.row_id
)
SELECT m.ltbase_row_id, w.ver_ts%[3]s
FROM hot_window w
JOIN %[2]s m ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = w.row_id
WHERE (%[4]s)`

// hotKeyColumn says where one sort column's stored value is read and how its
// key derives from it.
type hotKeyColumn struct {
	// system is the system column the sort column names, or empty for an
	// attribute.
	system string
	// meta is the attribute, when system is empty.
	meta forma.AttributeMetadata
	// eav reads an EAV-only attribute as the array of its eav_data values.
	eav bool
	// index is the select-list position of the stored value, or -1 when the
	// key reads none (deleted_ts derives the constant 0).
	index int
}

// hotKeysStatement is one rendered hot-keys query.
type hotKeysStatement struct {
	sql     string
	args    []any
	columns []hotKeyColumn
}

// QueryHotKeys reads the hot keys of req on tx, which must be the pass's
// snapshot: every entity_main row of the schema with a change_log entry in
// the dirty window that matches req.Condition, with its row ID, ver_ts and
// one key per sort column, derived as its next export will hold it. The keys
// come back in no particular order.
func (r *DBPersistentRecordRepository) QueryHotKeys(ctx context.Context, tx pgx.Tx, req model.HotKeysQuery) ([]model.HotKey, error) {
	stmt, err := r.buildHotKeysStatement(req)
	if err != nil {
		return nil, fmt.Errorf("build hot-keys query of schema %d: %w", req.SchemaID, err)
	}
	rows, err := tx.Query(ctx, stmt.sql, stmt.args...)
	if err != nil {
		return nil, fmt.Errorf("execute hot-keys query of schema %d: %w", req.SchemaID, err)
	}
	defer rows.Close()

	var keys []model.HotKey
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read hot key of schema %d: %w", req.SchemaID, err)
		}
		key, err := stmt.hotKey(values)
		if err != nil {
			return nil, fmt.Errorf("derive hot key of schema %d: %w", req.SchemaID, err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hot-keys query of schema %d: %w", req.SchemaID, err)
	}
	return keys, nil
}

// buildHotKeysStatement renders req's hot-keys query and its arguments.
func (r *DBPersistentRecordRepository) buildHotKeysStatement(req model.HotKeysQuery) (hotKeysStatement, error) {
	if err := validateHotLegTables(req.Tables); err != nil {
		return hotKeysStatement{}, err
	}
	if req.SchemaID <= 0 {
		return hotKeysStatement{}, fmt.Errorf("schema id must be positive, got %d", req.SchemaID)
	}
	cache, err := r.hotLegSchemaCache(req.SchemaID)
	if err != nil {
		return hotKeysStatement{}, err
	}
	columns, projections, err := hotKeyColumns(req.SortColumns, cache, req.Tables.EAVData)
	if err != nil {
		return hotKeysStatement{}, err
	}
	// Anchored on entity_main: the query reads entity_main rows, and the
	// PostgreSQL route's main anchor is the one that visits a row with no
	// eav_data row at all (#646).
	condition, conditionArgs, err := r.buildHybridConditions(
		sanitizeIdentifier(req.Tables.EAVData),
		sanitizeIdentifier(req.Tables.EntityMain),
		model.AttributeQuery{SchemaID: req.SchemaID, Condition: req.Condition},
		hotKeysConditionArgIndex,
		true,
	)
	if err != nil {
		return hotKeysStatement{}, fmt.Errorf("render condition: %w", err)
	}
	sql := fmt.Sprintf(hotKeysSQL,
		sanitizeIdentifier(req.Tables.ChangeLog),
		sanitizeIdentifier(req.Tables.EntityMain),
		projections,
		condition,
	)
	args := append([]any{req.SchemaID, req.GraceCutoffMs}, conditionArgs...)
	return hotKeysStatement{sql: sql, args: args, columns: columns}, nil
}

// validateHotLegTables reports a table the hot-keys query cannot do without.
func validateHotLegTables(tables model.StorageTables) error {
	if err := validateTables(tables); err != nil {
		return err
	}
	if tables.ChangeLog == "" {
		return errors.New("change log table name cannot be empty: the hot keys are the rows of the dirty window")
	}
	return nil
}

// hotLegSchemaCache answers the schema's attribute metadata, which both the
// condition and the sort columns resolve against.
func (r *DBPersistentRecordRepository) hotLegSchemaCache(schemaID int16) (forma.SchemaAttributeCache, error) {
	if r.metadataCache == nil {
		return nil, fmt.Errorf("schema metadata cache not available for schema_id %d", schemaID)
	}
	cache, ok := r.metadataCache.GetSchemaCacheByID(schemaID)
	if !ok {
		return nil, fmt.Errorf("no cache for schema id %d", schemaID)
	}
	return cache, nil
}

// hotKeyColumns resolves each sort column to the stored value its key derives
// from, and renders the projections that read them after row_id and ver_ts:
// a system column from entity_main, a column-bound attribute from its column,
// an EAV-only attribute as the array of its eav_data values, which the
// export collapses with MAX (sortkey.DeriveEAVAttribute).
func hotKeyColumns(names []string, cache forma.SchemaAttributeCache, eavTable string) ([]hotKeyColumn, string, error) {
	columns := make([]hotKeyColumn, 0, len(names))
	var projections strings.Builder
	next := hotKeysVerTSIndex + 1
	project := func(expr string) int {
		projections.WriteString(",\n    ")
		projections.WriteString(expr)
		next++
		return next - 1
	}
	for _, name := range names {
		if system, ok := sortkey.SystemColumnName(name); ok {
			columns = append(columns, hotKeyColumn{system: system, index: systemColumnIndex(system, project)})
			continue
		}
		meta, err := sortkey.ColumnAttribute(name, cache)
		if err != nil {
			return nil, "", fmt.Errorf("resolve sort column %q: %w", name, err)
		}
		if _, err := sortkey.AttributeType(meta); err != nil {
			return nil, "", fmt.Errorf("resolve sort column %q: %w", name, err)
		}
		if meta.ColumnBinding != nil {
			index := project("m." + sanitizeIdentifier(string(meta.ColumnBinding.ColumnName)))
			columns = append(columns, hotKeyColumn{meta: meta, index: index})
			continue
		}
		index := project(eavValuesProjection(meta, eavTable))
		columns = append(columns, hotKeyColumn{meta: meta, eav: true, index: index})
	}
	return columns, projections.String(), nil
}

// systemColumnIndex answers where a system column's stored value is read:
// row_id and ver_ts are read for every key, created_at is projected, and
// deleted_ts reads nothing, since a live row's key is the constant 0.
func systemColumnIndex(system string, project func(string) int) int {
	switch system {
	case sortkey.ColumnRowID:
		return hotKeysRowIDIndex
	case sortkey.ColumnVerTS:
		return hotKeysVerTSIndex
	case sortkey.ColumnCreatedAt:
		return project("m.ltbase_created_at")
	default:
		return -1
	}
}

// eavValuesProjection reads every eav_data value of an EAV-only attribute
// for the row: value_text for an attribute that exports as VARCHAR (text,
// uuid), value_numeric for the rest, as cdc.castEAVValue reads them. The
// attribute ID is schema metadata, rendered as an integer literal.
func eavValuesProjection(meta forma.AttributeMetadata, eavTable string) string {
	column := "value_numeric"
	if typ, _ := sortkey.AttributeType(meta); typ == sortkey.Varchar {
		column = "value_text"
	}
	return fmt.Sprintf("(SELECT ARRAY_AGG(e.%s) FROM %s e WHERE e.schema_id = m.ltbase_schema_id AND e.row_id = m.ltbase_row_id AND e.attr_id = %d)",
		column, sanitizeIdentifier(eavTable), meta.AttributeID)
}

// hotKey derives one hot key from a row of the statement.
func (s hotKeysStatement) hotKey(values []any) (model.HotKey, error) {
	id, err := sortkey.DeriveSystem(sortkey.ColumnRowID, values[hotKeysRowIDIndex])
	if err != nil {
		return model.HotKey{}, err
	}
	rowID, ok := id.(uuid.UUID)
	if !ok {
		return model.HotKey{}, errors.New("hot key has a NULL row_id")
	}
	ver, err := sortkey.DeriveSystem(sortkey.ColumnVerTS, values[hotKeysVerTSIndex])
	if err != nil {
		return model.HotKey{}, fmt.Errorf("row %s: %w", rowID, err)
	}
	verTS, ok := ver.(int64)
	if !ok {
		return model.HotKey{}, fmt.Errorf("row %s has a NULL ver_ts: change_log.changed_at is NOT NULL", rowID)
	}
	key := make([]any, len(s.columns))
	for i, c := range s.columns {
		if key[i], err = c.derive(values); err != nil {
			return model.HotKey{}, fmt.Errorf("row %s: %w", rowID, err)
		}
	}
	return model.HotKey{RowID: rowID, VerTS: verTS, Key: key}, nil
}

// derive derives the column's key from its stored value in values.
func (c hotKeyColumn) derive(values []any) (any, error) {
	var stored any
	if c.index >= 0 {
		stored = values[c.index]
	}
	switch {
	case c.system != "":
		return sortkey.DeriveSystem(c.system, stored)
	case c.eav:
		if stored == nil {
			return nil, nil
		}
		list, ok := stored.([]any)
		if !ok {
			return nil, fmt.Errorf("the eav_data values of attribute %q read as %T, not an array", c.meta.AttributeName, stored)
		}
		return sortkey.DeriveEAVAttribute(c.meta, list)
	default:
		return sortkey.DeriveAttribute(c.meta, stored)
	}
}

// ReadHotRows reads the page's hot rows on tx, the snapshot their keys came
// from, through the PostgreSQL route's whole-row reader: the entity_main
// projection and the EAV JSON_AGG, decoded as Get decodes a row. It answers
// one record per row ID, in rowIDs' order. A row ID the snapshot does not
// hold fails the read: the hot keys came from the same snapshot, so a
// missing row means the caller passed a key from another one.
func (r *DBPersistentRecordRepository) ReadHotRows(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID) ([]*model.PersistentRecord, error) {
	if err := validateTables(tables); err != nil {
		return nil, err
	}
	if len(rowIDs) == 0 {
		return nil, nil
	}
	query := rowSelectWithAttributes(tables.EntityMain, tables.EAVData) + `
WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = ANY($2)`
	rows, err := tx.Query(ctx, query, schemaID, rowIDs)
	if err != nil {
		return nil, fmt.Errorf("read %d hot rows of schema %d: %w", len(rowIDs), schemaID, err)
	}
	defer rows.Close()

	byID := make(map[uuid.UUID]*model.PersistentRecord, len(rowIDs))
	for rows.Next() {
		record, err := scanRowWithAttributes(rows)
		if err != nil {
			return nil, fmt.Errorf("read hot rows of schema %d: %w", schemaID, err)
		}
		byID[record.RowID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hot rows of schema %d: %w", schemaID, err)
	}

	records := make([]*model.PersistentRecord, len(rowIDs))
	for i, id := range rowIDs {
		record, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("hot row %s of schema %d is not in entity_main in the pass's snapshot: its hot key must come from the same snapshot", id, schemaID)
		}
		records[i] = record
	}
	return records, nil
}
