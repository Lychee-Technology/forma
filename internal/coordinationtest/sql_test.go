package coordinationtest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Direct-SQL reads of committed state. They never go through the adapter or
// a Forma transaction, so a check cannot be fooled by the code it checks.

// attrColumn is how one attribute is stored: its attribute ID, and its
// entity_main column when it is column-bound.
type attrColumn struct {
	id     int16
	column string
}

// loadColumns reads every schema's attribute file.
func loadColumns(t *testing.T) map[string]map[string]attrColumn {
	t.Helper()
	columns := make(map[string]map[string]attrColumn, len(schemaIDs))
	for schema := range schemaIDs {
		data, err := os.ReadFile(filepath.Join(schemaDir, schema+"_attributes.json"))
		require.NoError(t, err)
		var attrs map[string]struct {
			AttributeID   int16 `json:"attributeID"`
			ColumnBinding *struct {
				Column string `json:"col_name"`
			} `json:"column_binding"`
		}
		require.NoError(t, json.Unmarshal(data, &attrs))
		columns[schema] = make(map[string]attrColumn, len(attrs))
		for name, a := range attrs {
			c := attrColumn{id: a.AttributeID}
			if a.ColumnBinding != nil {
				c.column = a.ColumnBinding.Column
			}
			columns[schema][name] = c
		}
	}
	return columns
}

// column answers the entity_main column of a column-bound attribute.
func (e *env) column(t *testing.T, schema, attr string) string {
	t.Helper()
	c, ok := e.columns[schema][attr]
	require.True(t, ok && c.column != "", "%s.%s is a column-bound attribute", schema, attr)
	return c.column
}

// stored answers the committed values of the column-bound attributes attrs
// of row, read from entity_main, or nil when the row is not live. A text
// value is a string, a number or an instant (Unix ms) an int64.
func (e *env) stored(t *testing.T, schema string, row uuid.UUID, attrs ...string) map[string]any {
	t.Helper()
	columns := ""
	for i, a := range attrs {
		if i > 0 {
			columns += ", "
		}
		columns += e.column(t, schema, a)
	}
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 AND ltbase_deleted_at IS NULL",
		columns, e.tables.entityMain), schemaIDs[schema], row)
	require.NoError(t, err)
	defer rows.Close()
	if !rows.Next() {
		require.NoError(t, rows.Err())
		return nil
	}
	values, err := rows.Values()
	require.NoError(t, err)
	out := make(map[string]any, len(attrs))
	for i, a := range attrs {
		out[a] = normalize(values[i])
	}
	return out
}

// normalize answers a scanned column value as stored compares it.
func normalize(v any) any {
	switch n := v.(type) {
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	}
	return v
}

// liveRows answers how many live rows schema has.
func (e *env) liveRows(t *testing.T, schema string) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL", e.tables.entityMain),
		schemaIDs[schema]).Scan(&n))
	return n
}

// liveIDs answers the IDs of schema's live rows whose column-bound attr
// equals value.
func (e *env) liveIDs(t *testing.T, schema, attr string, value any) []uuid.UUID {
	t.Helper()
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(
		"SELECT ltbase_row_id FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL AND %s = $2 ORDER BY 1",
		e.tables.entityMain, e.column(t, schema, attr)), schemaIDs[schema], value)
	require.NoError(t, err)
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

// changeLogEntries answers every change_log entry of schema's row, as
// (flushed_at, changed_at) pairs.
func (e *env) changeLogEntries(t *testing.T, schema string, row uuid.UUID) [][2]int64 {
	t.Helper()
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(
		"SELECT flushed_at, changed_at FROM %s WHERE schema_id = $1 AND row_id = $2 ORDER BY 1", e.tables.changeLog),
		schemaIDs[schema], row)
	require.NoError(t, err)
	defer rows.Close()
	var entries [][2]int64
	for rows.Next() {
		var entry [2]int64
		require.NoError(t, rows.Scan(&entry[0], &entry[1]))
		entries = append(entries, entry)
	}
	require.NoError(t, rows.Err())
	return entries
}

// changeLogRows answers how many change_log entries schema has in all.
func (e *env) changeLogRows(t *testing.T, schema string) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE schema_id = $1", e.tables.changeLog),
		schemaIDs[schema]).Scan(&n))
	return n
}

// stamps answers the hot change_log stamp of each row; a row without one
// is absent.
func (e *env) stamps(t *testing.T, schema string, rows ...uuid.UUID) map[uuid.UUID]int64 {
	t.Helper()
	out := make(map[uuid.UUID]int64, len(rows))
	for _, row := range rows {
		for _, entry := range e.changeLogEntries(t, schema, row) {
			if entry[0] == 0 {
				out[row] = entry[1]
			}
		}
	}
	return out
}

// requireWritten asserts that each of rows, written since before was taken,
// has exactly one hot change_log entry, stamped later than its entry in
// before (#593's change_log rule).
func (e *env) requireWritten(t *testing.T, schema string, before map[uuid.UUID]int64, rows ...uuid.UUID) {
	t.Helper()
	for _, row := range rows {
		var hot []int64
		for _, entry := range e.changeLogEntries(t, schema, row) {
			if entry[0] == 0 {
				hot = append(hot, entry[1])
			}
		}
		require.Len(t, hot, 1, "%s row %s has one hot change_log entry", schema, row)
		require.Greater(t, hot[0], before[row], "%s row %s: its stamp is later than its previous one", schema, row)
	}
}

// eavTexts answers the text EAV attribute attr of every live row of schema
// that has it.
func (e *env) eavTexts(t *testing.T, schema, attr string) map[uuid.UUID]string {
	t.Helper()
	c, ok := e.columns[schema][attr]
	require.True(t, ok && c.column == "", "%s.%s is an EAV attribute", schema, attr)
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(`SELECT d.row_id, d.value_text FROM %s d
		JOIN %s m ON m.ltbase_schema_id = d.schema_id AND m.ltbase_row_id = d.row_id AND m.ltbase_deleted_at IS NULL
		WHERE d.schema_id = $1 AND d.attr_id = $2`, e.tables.eavData, e.tables.entityMain), schemaIDs[schema], c.id)
	require.NoError(t, err)
	defer rows.Close()
	out := map[uuid.UUID]string{}
	for rows.Next() {
		var row uuid.UUID
		var value string
		require.NoError(t, rows.Scan(&row, &value))
		out[row] = value
	}
	require.NoError(t, rows.Err())
	return out
}

// eavValues answers the EAV attributes of schema's row by name, each as its
// stored text: value_text, or value_numeric cast to text.
func (e *env) eavValues(t *testing.T, schema string, row uuid.UUID) map[string]string {
	t.Helper()
	names := map[int16]string{}
	for name, c := range e.columns[schema] {
		if c.column == "" {
			names[c.id] = name
		}
	}
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(`SELECT attr_id, coalesce(value_text, value_numeric::text) FROM %s
		WHERE schema_id = $1 AND row_id = $2`, e.tables.eavData), schemaIDs[schema], row)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var attr int16
		var value *string
		require.NoError(t, rows.Scan(&attr, &value))
		name, ok := names[attr]
		require.True(t, ok, "%s row %s has EAV attribute %d", schema, row, attr)
		if value != nil {
			out[name] = *value
		}
	}
	require.NoError(t, rows.Err())
	return out
}

// requireOneHotEntryPerRow asserts that every live row of schema has a hot
// change_log entry, and that no hot entry names a row that is not stored:
// the residue a rolled-back write would leave.
func (e *env) requireOneHotEntryPerRow(t *testing.T, schema string) {
	t.Helper()
	var missing, orphaned int
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(`SELECT count(*) FROM %s m
		WHERE m.ltbase_schema_id = $1 AND m.ltbase_deleted_at IS NULL AND NOT EXISTS (
			SELECT 1 FROM %s c WHERE c.schema_id = m.ltbase_schema_id AND c.row_id = m.ltbase_row_id AND c.flushed_at = 0)`,
		e.tables.entityMain, e.tables.changeLog), schemaIDs[schema]).Scan(&missing))
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(`SELECT count(*) FROM %s c
		WHERE c.schema_id = $1 AND c.flushed_at = 0 AND c.deleted_at IS NULL AND NOT EXISTS (
			SELECT 1 FROM %s m WHERE m.ltbase_schema_id = c.schema_id AND m.ltbase_row_id = c.row_id)`,
		e.tables.changeLog, e.tables.entityMain), schemaIDs[schema]).Scan(&orphaned))
	require.Zero(t, missing, "%s: live rows without a hot change_log entry", schema)
	require.Zero(t, orphaned, "%s: hot change_log entries of rows that are not stored", schema)
}
