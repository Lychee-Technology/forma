package internal

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The task652 schema and fixture of the predicate mutation tests (#652): a
// workflow task with column-bound and EAV attributes, over temporary tables
// on real PostgreSQL. Post-state is read by direct SQL or through the pool,
// never through the Tx under test.

const (
	taskSchemaName       = "task652"
	taskSchemaID   int16 = 652
)

func taskSchemaCache() forma.SchemaAttributeCache {
	bind := func(col string, enc forma.MainColumnEncoding) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}
	}
	return forma.SchemaAttributeCache{
		"instance":            {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: bind("text_01", "")},
		"status":              {AttributeID: 2, ValueType: forma.ValueTypeText},
		"expected_event":      {AttributeID: 3, ValueType: forma.ValueTypeText},
		"priority":            {AttributeID: 4, ValueType: forma.ValueTypeInteger, ColumnBinding: bind("integer_01", "")},
		"counter":             {AttributeID: 5, ValueType: forma.ValueTypeInteger},
		"resolved_at":         {AttributeID: 6, ValueType: forma.ValueTypeDateTime},
		"eligible_cleanup_at": {AttributeID: 7, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("bigint_01", forma.MainColumnEncodingUnixMs)},
		"version":             {AttributeID: 8, ValueType: forma.ValueTypeBigInt, ColumnBinding: bind("bigint_02", "")},
		"state":               {AttributeID: 9, ValueType: forma.ValueTypeText},
		"updated_at":          {AttributeID: 10, ValueType: forma.ValueTypeDateTime},
		"token_hash":          {AttributeID: 11, ValueType: forma.ValueTypeText},
		"group":               {AttributeID: 12, ValueType: forma.ValueTypeText},
	}
}

type taskPG struct{ *condPG }

func newTaskPG(t *testing.T) *taskPG {
	t.Helper()
	return &taskPG{newSchemaPG(t, taskSchemaName, taskSchemaID, taskSchemaCache())}
}

// task answers a pending task of instance with the given priority.
func task(instance, status string, priority int) map[string]any {
	return map[string]any{"instance": instance, "status": status, "priority": priority, "version": int64(1), "counter": 0}
}

func (e *taskPG) create(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName}, Data: data})
	require.NoError(t, err)
	return record.RowID
}

// createN creates n tasks, the i-th from data(i), and answers their row IDs.
func (e *taskPG) createN(t *testing.T, n int, data func(i int) map[string]any) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, n)
	for i := range n {
		ids[i] = e.create(t, data(i))
	}
	return ids
}

// attrs answers the row's committed attributes, read on the pool, or nil
// when the row does not exist.
func (e *taskPG) attrs(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: taskSchemaName, RowID: &rowID})
	if err != nil {
		require.ErrorIs(t, err, forma.ErrNotFound)
		return nil
	}
	return record.Attributes
}

func (e *taskPG) snapshot(t *testing.T, rowID uuid.UUID) rowSnapshot {
	t.Helper()
	return e.snapshotOf(t, taskSchemaID, rowID)
}

func (e *taskPG) snapshots(t *testing.T, ids []uuid.UUID) map[uuid.UUID]rowSnapshot {
	t.Helper()
	snaps := make(map[uuid.UUID]rowSnapshot, len(ids))
	for _, id := range ids {
		snaps[id] = e.snapshot(t, id)
	}
	return snaps
}

// requireUnchanged asserts every row in before is as it was, by direct SQL.
func (e *taskPG) requireUnchanged(t *testing.T, before map[uuid.UUID]rowSnapshot, msg string) {
	t.Helper()
	for id, snap := range before {
		require.Equal(t, snap, e.snapshot(t, id), "%s: %s", msg, id)
	}
}

// requireStoredAs asserts record holds what entity_main and EAV store for
// its row, read by direct SQL: a column-bound text and bigint, and an EAV
// text.
func (e *taskPG) requireStoredAs(t *testing.T, record *forma.DataRecord) {
	t.Helper()
	var instance, status *string
	var version *int64
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(`SELECT m.text_01, m.bigint_02,
		(SELECT value_text FROM %s v WHERE v.schema_id = $1 AND v.row_id = $2 AND v.attr_id = 2)
		FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2`,
		sanitizeIdentifier(e.tables.EAVData), sanitizeIdentifier(e.tables.EntityMain)),
		taskSchemaID, record.RowID).Scan(&instance, &version, &status))
	require.NotNil(t, instance)
	require.Equal(t, *instance, record.Attributes["instance"], "instance (entity_main text_01)")
	require.NotNil(t, version)
	require.EqualValues(t, *version, record.Attributes["version"], "version (entity_main bigint_02)")
	require.NotNil(t, status)
	require.Equal(t, *status, record.Attributes["status"], "status (EAV)")
}

// tombstone answers the row's hot change_log entry: its stamp, and whether it
// marks a delete.
func (e *taskPG) tombstone(t *testing.T, rowID uuid.UUID) (int64, bool) {
	t.Helper()
	var changedAt int64
	var deletedAt *int64
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0",
		sanitizeIdentifier(e.tables.ChangeLog)), taskSchemaID, rowID).Scan(&changedAt, &deletedAt))
	return changedAt, deletedAt != nil
}

// updateWhere runs one UpdateWhere in its own InTx.
func (e *taskPG) updateWhere(req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	var result *forma.MutateWhereResult
	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		var err error
		result, err = tx.UpdateWhere(ctx, req)
		return err
	})
	return result, err
}

// deleteWhere runs one DeleteWhere in its own InTx.
func (e *taskPG) deleteWhere(req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	var result *forma.MutateWhereResult
	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		var err error
		result, err = tx.DeleteWhere(ctx, req)
		return err
	})
	return result, err
}

// pendingOf is the condition "instance = instance AND status = pending".
func pendingOf(instance string) forma.Condition {
	return and(kv("instance", "equals:"+instance), kv("status", "equals:pending"))
}

// byRowID answers ids in row ID order, the candidates' final tiebreak.
func byRowID(ids []uuid.UUID) []uuid.UUID {
	sorted := slices.Clone(ids)
	slices.SortFunc(sorted, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return sorted
}
