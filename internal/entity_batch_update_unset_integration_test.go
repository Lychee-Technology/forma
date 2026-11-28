package internal

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Real-PostgreSQL tests of Unset in BatchUpdate (#940), atomic and
// best-effort: a batch stores what Update stores for each operation, an
// atomic batch with one refused unset commits nothing, and a best-effort
// batch fails that operation only.

// unsetBatchMutation is one operation's write, over a row of unsetBaseDoc.
type unsetBatchMutation struct {
	name    string
	updates map[string]any
	ops     []forma.UpdateOp
	// cleared are the targets the write leaves without an eav_data row, and
	// columns the main columns it leaves NULL.
	cleared []string
	columns []string
}

// unsetBatchMutations mix unset-only operations, unsets beside a literal Set
// and a literal-only operation, over bound, EAV, list and nested targets.
var unsetBatchMutations = []unsetBatchMutation{
	{name: "unset alone, bound", ops: []forma.UpdateOp{forma.Unset("label")}, columns: []string{"text_01"}},
	{name: "unset alone, EAV", ops: []forma.UpdateOp{forma.Unset("note")}, cleared: []string{"note"}},
	{name: "unset alone, list", ops: []forma.UpdateOp{forma.Unset("tags")}, cleared: []string{"tags"}},
	{name: "unsets beside a Set, nested list and bound", updates: map[string]any{"short": "new"},
		ops:     []forma.UpdateOp{forma.Unset("profile.aliases"), forma.Unset("profile.title")},
		cleared: []string{"profile.aliases"}, columns: []string{"text_02"}},
	{name: "literal only", updates: map[string]any{"note": "changed", "tags": []any{"z"}}},
}

// requireCleared asserts m's targets are gone from rowID: no eav_data row
// of an EAV or list target, a NULL column for a bound one.
func (e *txEnv) requireCleared(t *testing.T, rowID uuid.UUID, m unsetBatchMutation) {
	t.Helper()
	for _, attr := range m.cleared {
		assert.Zero(t, e.eavRowCount(t, unsetSchemaName, rowID, e.attrID(t, unsetSchemaName, attr)), "no eav_data row of %s is left", attr)
	}
	columns := e.mainColumns(t, unsetSchemaName, rowID)
	for _, column := range m.columns {
		require.Contains(t, columns, column)
		assert.Nil(t, columns[column], "the bound column %s is NULL", column)
	}
}

// TestBatchUpdateUnsetStoresWhatUpdateStores gives pairs of rows of one
// document the same mutation, one row through Update and the other through
// one BatchUpdate carrying every mutation: entity_main and eav_data are the
// same, the clears included, the batch advances the version, and change_log
// holds what Update leaves, normalised for row IDs and write timestamps.
func TestBatchUpdateUnsetStoresWhatUpdateStores(t *testing.T) {
	for _, atomic := range []bool{true, false} {
		t.Run(batchModeName(atomic), func(t *testing.T) {
			env := setupUnsetDocPG(t)
			references := make([]uuid.UUID, len(unsetBatchMutations))
			rows := make([]uuid.UUID, len(unsetBatchMutations))
			versions := make([]int64, len(unsetBatchMutations))
			batch := &forma.BatchOperation{Atomic: atomic}
			for i, m := range unsetBatchMutations {
				references[i] = env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
				_, err := env.manager.Update(env.ctx, unsetOp(references[i], m.updates, m.ops...))
				require.NoError(t, err, m.name)
				rows[i] = env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
				versions[i], _ = env.hotVersion(t, env.postgresPool, unsetSchemaName, rows[i])
				batch.Operations = append(batch.Operations, *unsetOp(rows[i], m.updates, m.ops...))
			}

			result, err := env.manager.BatchUpdate(env.ctx, batch)
			require.NoError(t, err)
			require.Empty(t, result.Failed)
			require.Len(t, result.Successful, len(unsetBatchMutations))

			for i, m := range unsetBatchMutations {
				assert.Equal(t, env.storedImage(t, unsetSchemaName, references[i]), env.storedImage(t, unsetSchemaName, rows[i]),
					"%s: entity_main and eav_data as Update stores them", m.name)
				env.requireCleared(t, rows[i], m)
				after, hot := env.hotVersion(t, env.postgresPool, unsetSchemaName, rows[i])
				assert.Greater(t, after, versions[i], "%s: the batch advances the version", m.name)
				assert.Contains(t, hot, after, "%s: change_log carries the new version", m.name)
				assert.Equal(t, env.changeLogImage(t, unsetSchemaName, references[i]), env.changeLogImage(t, unsetSchemaName, rows[i]),
					"%s: change_log as Update leaves it, no entry more or less", m.name)
				assert.Equal(t, env.attrs(t, unsetSchemaName, rows[i]), result.Successful[i].Attributes,
					"%s: the response is the stored row", m.name)
			}
		})
	}
}

// changeLogImage answers rowID's change_log entries in a form two rows'
// entries compare by: without row_id, and with a changed_at that is the
// row's version (entity_main's ltbase_updated_at) written as "row version",
// since each write stamps its own. Any other changed_at is kept as stored.
func (e *txEnv) changeLogImage(t *testing.T, schema string, rowID uuid.UUID) []map[string]any {
	t.Helper()
	version, _ := e.hotVersion(t, e.postgresPool, schema, rowID)
	decoder := json.NewDecoder(strings.NewReader(e.snapshot(t, schema, rowID).changeLog))
	decoder.UseNumber()
	var entries []map[string]any
	require.NoError(t, decoder.Decode(&entries))
	for _, entry := range entries {
		require.Contains(t, entry, "row_id")
		delete(entry, "row_id")
		if entry["changed_at"] == json.Number(strconv.FormatInt(version, 10)) {
			entry["changed_at"] = "row version"
		}
	}
	return entries
}

// unsetBatchRefusal is an operation on row a refusal fails: by its target
// or an overlap at intake, by validation of the merged document, or by its
// missing row.
type unsetBatchRefusal struct {
	name string
	op   func(rowID uuid.UUID) *forma.EntityOperation
	is   error
	want string
}

var unsetBatchRefusals = []unsetBatchRefusal{
	{"unknown target", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("nope")) },
		forma.ErrInvalidInput, "unset names attribute 'nope'"},
	{"overlap with Updates", func(id uuid.UUID) *forma.EntityOperation {
		return unsetOp(id, map[string]any{"note": "x"}, forma.Unset("note"))
	}, forma.ErrInvalidInput, "note"},
	{"required_always", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("name")) },
		forma.ErrInvalidInput, "name"},
	{"missing row", func(uuid.UUID) *forma.EntityOperation { return unsetOp(uuid.New(), nil, forma.Unset("note")) },
		forma.ErrNotFound, "entity not found"},
}

// TestAtomicBatchUpdateWithARefusedUnsetCommitsNothing runs a valid clear
// ahead of each refusal in one atomic batch: the batch fails, attributed to
// the refused operation, and neither row changes in any table.
func TestAtomicBatchUpdateWithARefusedUnsetCommitsNothing(t *testing.T) {
	env := setupUnsetDocPG(t)
	for _, tc := range unsetBatchRefusals {
		t.Run(tc.name, func(t *testing.T) {
			a, b := env.mustCreate(t, unsetSchemaName, unsetBaseDoc()), env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
			before := []rowSnapshot{env.snapshot(t, unsetSchemaName, a), env.snapshot(t, unsetSchemaName, b)}

			_, err := env.manager.BatchUpdate(env.ctx, unsetBatch(true,
				unsetOp(a, map[string]any{"short": "new"}, forma.Unset("label"), forma.Unset("tags")), tc.op(b)))
			require.ErrorIs(t, err, tc.is)
			message, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "the refusal publishes its message: %v", err)
			assert.Contains(t, message, "operation[1]")
			assert.Contains(t, message, tc.want)

			assert.Equal(t, before, []rowSnapshot{env.snapshot(t, unsetSchemaName, a), env.snapshot(t, unsetSchemaName, b)},
				"entity_main, eav_data and change_log as stored")
		})
	}
}

// TestBestEffortBatchUpdateFailsOnlyTheRefusedUnsetInPostgres runs each
// refusal between two valid clears in a best-effort batch: the refused
// operation fails, attributed to its operation, index and RowID, and writes
// nothing; the others commit.
func TestBestEffortBatchUpdateFailsOnlyTheRefusedUnsetInPostgres(t *testing.T) {
	env := setupUnsetDocPG(t)
	first, last := unsetBatchMutations[0], unsetBatchMutations[2]
	for _, tc := range unsetBatchRefusals {
		t.Run(tc.name, func(t *testing.T) {
			logs := observeGlobalLog(t)
			a, b, c := env.mustCreate(t, unsetSchemaName, unsetBaseDoc()), env.mustCreate(t, unsetSchemaName, unsetBaseDoc()),
				env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
			before := env.snapshot(t, unsetSchemaName, b)
			refused := tc.op(b)

			result, err := env.manager.BatchUpdate(env.ctx, unsetBatch(false,
				unsetOp(a, first.updates, first.ops...), refused, unsetOp(c, last.updates, last.ops...)))
			require.NoError(t, err)
			require.Len(t, result.Failed, 1)
			failure := result.Failed[0]
			assert.Equal(t, refused.RowID, failure.Operation.RowID)
			assert.Equal(t, 1, failure.Details[forma.OperationErrorIndexDetail],
				"the result names the failed operation's index in the request")
			assert.Equal(t, refused.UpdateOps, failure.Operation.UpdateOps)
			assert.Equal(t, "UPDATE_FAILED", failure.Code)
			assert.Contains(t, failure.Error, tc.want)
			lines := logs.FilterField(zap.String("error_id", failure.ErrorID)).All()
			require.Len(t, lines, 1)
			assert.EqualValues(t, 1, lines[0].ContextMap()["operation_index"], "the failure is the second operation's")

			require.Len(t, result.Successful, 2)
			assert.Equal(t, []uuid.UUID{a, c}, []uuid.UUID{result.Successful[0].RowID, result.Successful[1].RowID})
			env.requireCleared(t, a, first)
			env.requireCleared(t, c, last)
			assert.Equal(t, before, env.snapshot(t, unsetSchemaName, b), "the refused operation writes nothing")
		})
	}
}

// TestBestEffortBatchUpdateIndexesAFailureOnARepeatedRow names one row three
// times in a best-effort batch, the middle operation refused: the RowID
// alone matches every operation, so the result's index is what names the
// failed one, and the two valid clears around it commit.
func TestBestEffortBatchUpdateIndexesAFailureOnARepeatedRow(t *testing.T) {
	env := setupUnsetDocPG(t)
	a := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())

	result, err := env.manager.BatchUpdate(env.ctx, unsetBatch(false,
		unsetOp(a, nil, forma.Unset("note")), unsetOp(a, nil, forma.Unset("nope")), unsetOp(a, nil, forma.Unset("tags"))))
	require.NoError(t, err)
	require.Len(t, result.Failed, 1)
	failure := result.Failed[0]
	assert.Equal(t, a, failure.Operation.RowID)
	assert.Equal(t, 1, failure.Details[forma.OperationErrorIndexDetail], "the index names the refused operation")
	assert.Contains(t, failure.Error, "unset names attribute 'nope'")
	require.Len(t, result.Successful, 2)
	env.requireCleared(t, a, unsetBatchMutation{cleared: []string{"note", "tags"}})
}
