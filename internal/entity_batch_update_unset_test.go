package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Unit tests of Unset in BatchUpdate (#940) over unsetdoc and the mock
// repository: the request-level gate admits Unset, and only Unset, on
// BatchUpdate; the atomic path judges every operation's target before the
// batch merges any row and merges each clear as Update does; the best-effort
// path fails the one operation whose target its schema refuses.

func unsetBatch(atomic bool, ops ...*forma.EntityOperation) *forma.BatchOperation {
	batch := &forma.BatchOperation{Atomic: atomic}
	for _, op := range ops {
		batch.Operations = append(batch.Operations, *op)
	}
	return batch
}

// requireNoWrite asserts the mock saw no merge, batch merge, insert or
// delete.
func (h *unsetHarness) requireNoWrite(t *testing.T) {
	t.Helper()
	assert.Zero(t, h.repo.mergeCalls, "no merge may run")
	assert.Zero(t, h.repo.batchUpdateCalls, "no batch merge may run")
	assert.Zero(t, h.repo.deleteCalls, "no delete may run")
	assert.Empty(t, h.repo.insertedRecords, "no insert may run")
}

func requireBatchRefusal(t *testing.T, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "the refusal publishes its message: %v", err)
	assert.Contains(t, message, want)
}

func TestBatchUpdateAdmitsUnsetOnly(t *testing.T) {
	rowID := uuid.New()
	refused := []struct {
		name string
		op   *forma.EntityOperation
		want string
	}{
		{"increment", unsetOp(rowID, nil, forma.Increment("counter", 1)),
			"operation[1]: batches do not accept update operators (increment on 'counter'); BatchUpdate accepts unset only"},
		{"server_time", unsetOp(rowID, nil, forma.ServerTime("due", time.Second)),
			"operation[1]: batches do not accept update operators (server_time on 'due')"},
		{"unset beside an increment", unsetOp(rowID, nil, forma.Unset("note"), forma.Increment("counter", 1)),
			"operation[1]: batches do not accept update operators (increment on 'counter')"},
		{"unknown kind", unsetOp(rowID, nil, forma.UpdateOp{Attribute: "note", Kind: "drop"}),
			"operation[1]: batches do not accept update operators (drop on 'note')"},
	}
	withPrecondition := unsetOp(rowID, nil, forma.Unset("note"))
	withPrecondition.Preconditions = &forma.KvCondition{Attr: "note", Value: "equals:N"}
	refused = append(refused, struct {
		name string
		op   *forma.EntityOperation
		want string
	}{"unset under a precondition", withPrecondition, "operation[1]: batches do not accept preconditions (the precondition tests 'note')"})

	for _, atomic := range []bool{false, true} {
		mode := batchModeName(atomic)
		for _, tc := range refused {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				h := newUnsetHarness(t, unsetReportOnly)
				_, err := h.em.BatchUpdate(h.ctx, unsetBatch(atomic, unsetOp(uuid.New(), nil, forma.Unset("note")), tc.op))
				requireBatchRefusal(t, err, tc.want)
				h.requireNoWrite(t)
			})
		}
		for name, run := range map[string]func(forma.EntityManager, context.Context, *forma.BatchOperation) (*forma.BatchResult, error){
			"BatchCreate": forma.EntityManager.BatchCreate,
			"BatchDelete": forma.EntityManager.BatchDelete,
		} {
			t.Run(mode+"/"+name+" refuses unset", func(t *testing.T) {
				h := newUnsetHarness(t, unsetReportOnly)
				op := unsetOp(uuid.New(), nil, forma.Unset("note"))
				op.Data = unsetBaseDoc()
				_, err := run(h.em, h.ctx, unsetBatch(atomic, op))
				requireBatchRefusal(t, err, "operation[0]: batches do not accept update operators (unset on 'note'); run them through Update or Transactor.InTx")
				h.requireNoWrite(t)
			})
		}
	}
}

func batchModeName(atomic bool) string {
	if atomic {
		return "atomic"
	}
	return "best-effort"
}

func TestBatchUpdateUnsetKeepsTheBatchSizeCap(t *testing.T) {
	for _, atomic := range []bool{false, true} {
		h := newUnsetHarnessWith(t, unsetReportOnly, func(c *forma.Config) { c.Performance.MaxBatchSize = 1 })
		_, err := h.em.BatchUpdate(h.ctx, unsetBatch(atomic,
			unsetOp(uuid.New(), nil, forma.Unset("note")), unsetOp(uuid.New(), nil, forma.Unset("note"))))
		requireBatchRefusal(t, err, "batch of 2 operations exceeds the maximum batch size of 1")
		h.requireNoWrite(t)
	}
}

// TestAtomicBatchUpdateMergesEachUnset pins the atomic merge: an
// operation of an unset alone and one beside a literal Set each store the
// document Update's merge makes, and the merged record names the main
// columns its clears empty.
func TestAtomicBatchUpdateMergesEachUnset(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	alone, beside, literal := h.create(t, unsetBaseDoc()), h.create(t, unsetBaseDoc()), h.create(t, unsetBaseDoc())

	result, err := h.em.BatchUpdate(h.ctx, unsetBatch(true,
		unsetOp(alone, nil, forma.Unset("label")),
		unsetOp(beside, map[string]any{"short": "new"}, forma.Unset("tags"), forma.Unset("profile.title")),
		unsetOp(literal, map[string]any{"note": "changed"})))
	require.NoError(t, err)
	require.Len(t, result.Successful, 3)
	assert.Empty(t, result.Failed)

	besideWant := withoutPath(withoutPath(unsetBaseDoc(), "tags"), "profile.title")
	besideWant["short"] = "new"
	literalWant := unsetBaseDoc()
	literalWant["note"] = "changed"
	for _, row := range []struct {
		rowID   uuid.UUID
		want    map[string]any
		cleared []string
	}{
		{alone, withoutPath(unsetBaseDoc(), "label"), []string{"text_01"}},
		{beside, besideWant, []string{"text_02"}},
		{literal, literalWant, nil},
	} {
		assert.Equal(t, unsetLogical(t, row.want), unsetLogical(t, h.stored(t, row.rowID)))
		assert.Equal(t, row.cleared, h.record(row.rowID).ClearedColumns)
	}
	assert.Equal(t, unsetLogical(t, withoutPath(unsetBaseDoc(), "label")), unsetLogical(t, result.Successful[0].Attributes),
		"the response is the stored record")
}

// TestAtomicBatchUpdateRefusesAnUnsetBeforeAnyMerge pins that a target or
// an overlap intake refuses on one operation fails the whole batch before
// it merges any row, attributed to the operation's index.
func TestAtomicBatchUpdateRefusesAnUnsetBeforeAnyMerge(t *testing.T) {
	cases := []struct {
		name string
		op   func(rowID uuid.UUID) *forma.EntityOperation
		want string
	}{
		{"unknown target", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("nope")) },
			"operation[1]: unset names attribute 'nope', which schema 'unsetdoc' does not define"},
		{"object", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("profile")) },
			"operation[1]: unset names 'profile', an object"},
		{"inside an array of objects", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("items.sku")) },
			"operation[1]: attribute 'items.sku' is inside the array of objects 'items'"},
		{"overlap with Updates", func(id uuid.UUID) *forma.EntityOperation {
			return unsetOp(id, map[string]any{"note": "x"}, forma.Unset("note"))
		}, "operation[1]:"},
		{"second unset of the attribute", func(id uuid.UUID) *forma.EntityOperation {
			return unsetOp(id, nil, forma.Unset("note"), forma.Unset("note"))
		}, "operation[1]:"},
		{"neither updates nor operators", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil) },
			"operation[1]: updates are required for update operation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newUnsetHarness(t, unsetReportOnly)
			_, err := h.em.BatchUpdate(h.ctx, unsetBatch(true, unsetOp(uuid.New(), nil, forma.Unset("note")), tc.op(uuid.New())))
			requireBatchRefusal(t, err, tc.want)
			h.requireNoWrite(t)
		})
	}
}

// TestAtomicBatchUpdateRefusesARequiredUnsetWholly pins that an unset the
// merged document's validation refuses, under the batch's locks, leaves
// every row of the batch as stored.
func TestAtomicBatchUpdateRefusesARequiredUnsetWholly(t *testing.T) {
	h := newUnsetHarness(t, unsetReportOnly)
	a, b := h.create(t, unsetBaseDoc()), h.create(t, unsetBaseDoc())
	before := map[uuid.UUID]map[string]any{a: h.stored(t, a), b: h.stored(t, b)}

	_, err := h.em.BatchUpdate(h.ctx, unsetBatch(true, unsetOp(a, nil, forma.Unset("note")), unsetOp(b, nil, forma.Unset("name"))))
	requireBatchRefusal(t, err, "operation[1]:")
	for rowID, doc := range before {
		assert.Equal(t, doc, h.stored(t, rowID), "nothing commits")
	}
}

// TestBestEffortBatchUpdateFailsOnlyTheRefusedUnset pins that a refusal
// depending on an operation's schema or row fails that operation only,
// attributed to it, and the others commit.
func TestBestEffortBatchUpdateFailsOnlyTheRefusedUnset(t *testing.T) {
	cases := []struct {
		name string
		op   func(rowID uuid.UUID) *forma.EntityOperation
		want string
	}{
		{"unknown target", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("nope")) },
			"unset names attribute 'nope'"},
		{"overlap with Updates", func(id uuid.UUID) *forma.EntityOperation {
			return unsetOp(id, map[string]any{"note": "x"}, forma.Unset("note"))
		}, "note"},
		{"required_always", func(id uuid.UUID) *forma.EntityOperation { return unsetOp(id, nil, forma.Unset("name")) }, "name"},
		{"missing row", func(uuid.UUID) *forma.EntityOperation { return unsetOp(uuid.New(), nil, forma.Unset("note")) }, "entity not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newUnsetHarness(t, unsetReportOnly)
			logs := observeGlobalLog(t)
			a, b, c := h.create(t, unsetBaseDoc()), h.create(t, unsetBaseDoc()), h.create(t, unsetBaseDoc())
			before := h.stored(t, b)
			refused := tc.op(b)

			result, err := h.em.BatchUpdate(h.ctx, unsetBatch(false,
				unsetOp(a, nil, forma.Unset("label")), refused, unsetOp(c, map[string]any{"short": "new"}, forma.Unset("tags"))))
			require.NoError(t, err)
			require.Len(t, result.Failed, 1)
			failure := result.Failed[0]
			assert.Equal(t, refused.RowID, failure.Operation.RowID)
			assert.Equal(t, refused.UpdateOps, failure.Operation.UpdateOps)
			assert.Equal(t, "UPDATE_FAILED", failure.Code)
			assert.Contains(t, failure.Error, tc.want)
			lines := logs.FilterField(zap.String("error_id", failure.ErrorID)).All()
			require.Len(t, lines, 1)
			assert.EqualValues(t, 1, lines[0].ContextMap()["operation_index"])

			require.Len(t, result.Successful, 2)
			assert.Equal(t, unsetLogical(t, withoutPath(unsetBaseDoc(), "label")), unsetLogical(t, h.stored(t, a)))
			cWant := withoutPath(unsetBaseDoc(), "tags")
			cWant["short"] = "new"
			assert.Equal(t, unsetLogical(t, cWant), unsetLogical(t, h.stored(t, c)))
			assert.Equal(t, before, h.stored(t, b), "the refused operation writes nothing")
		})
	}
}
