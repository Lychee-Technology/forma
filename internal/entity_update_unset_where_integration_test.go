package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the unset operator in Tx.UpdateWhere (#939): the
// clear is applied to each row the predicate selects and the re-check keeps,
// and to no other; MaxAffected and the re-check behave as they do without
// it.

// unsetWhereRows creates n unsetdoc rows tagged tag (their memo) and one
// tagged otherwise, and answers the tagged ones and the other.
func (e *txEnv) unsetWhereRows(t *testing.T, tag string, n int) ([]uuid.UUID, uuid.UUID) {
	t.Helper()
	tagged := make([]uuid.UUID, n)
	for i := range tagged {
		tagged[i] = e.mustCreate(t, unsetSchemaName, unsetTaggedDoc(tag))
	}
	return tagged, e.mustCreate(t, unsetSchemaName, unsetTaggedDoc(tag+"-other"))
}

// unsetTaggedDoc is unsetBaseDoc with memo tag, which UpdateWhere selects
// on.
func unsetTaggedDoc(tag string) map[string]any {
	doc := unsetBaseDoc()
	doc["memo"] = tag
	return doc
}

func (e *txEnv) updateWhereIn(req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	var result *forma.MutateWhereResult
	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		var err error
		if result, err = tx.UpdateWhere(ctx, req); err != nil {
			return fmt.Errorf("failed to update %s rows where %v: %w", req.SchemaName, req.Condition, err)
		}
		return nil
	})
	return result, err
}

func memoIs(tag string) forma.Condition {
	return &forma.KvCondition{Attr: "memo", Value: "equals:" + tag}
}

// TestUpdateWhereUnsetClearsEveryMatchedRowAndNoOther runs UpdateWhere with
// an unset of a bound attribute and of a list, alone and beside a Set and an
// Increment: every matched row is stored as the update makes it, with
// version and change_log advanced, and the unmatched row is untouched.
func TestUpdateWhereUnsetClearsEveryMatchedRowAndNoOther(t *testing.T) {
	env := setupUnsetDocPG(t)
	tagsID := env.attrID(t, unsetSchemaName, "tags")
	for _, companion := range unsetCompanions[:3] {
		t.Run(companion.name, func(t *testing.T) {
			tag := uuid.NewString()
			matched, other := env.unsetWhereRows(t, tag, 2)
			otherBefore := env.snapshot(t, unsetSchemaName, other)
			versions := map[uuid.UUID]int64{}
			for _, rowID := range matched {
				versions[rowID], _ = env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
			}

			ops := append([]forma.UpdateOp{forma.Unset("label"), forma.Unset("tags")}, companion.ops...)
			result, err := env.updateWhereIn(&forma.MutateWhereRequest{SchemaName: unsetSchemaName, Condition: memoIs(tag),
				Updates: companion.updates, UpdateOps: ops, Returning: true})
			require.NoError(t, err)
			assert.ElementsMatch(t, matched, result.RowIDs)

			want := withoutPath(withoutPath(unsetTaggedDoc(tag), "label"), "tags")
			companion.apply(want)
			for i, rowID := range result.RowIDs {
				got := unsetLogical(t, env.attrs(t, unsetSchemaName, rowID))
				assert.Equal(t, unsetLogical(t, want), got, "row %s read back", rowID)
				assert.Equal(t, got, unsetLogical(t, result.Records[i].Attributes), "the answer is the stored row")
				assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"], "the bound column is NULL")
				assert.Zero(t, env.eavRowCount(t, unsetSchemaName, rowID, tagsID), "no eav_data row of the list is left")
				after, hot := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
				assert.Greater(t, after, versions[rowID], "the update advances the version")
				assert.Contains(t, hot, after, "change_log carries the new version")
			}
			assert.Equal(t, otherBefore, env.snapshot(t, unsetSchemaName, other), "the unmatched row is untouched")
		})
	}
}

// TestUpdateWhereUnsetLeavesARecheckMissUntouched_PostgresOnly changes a
// candidate between the candidate select and its lock so that it no longer
// matches: the re-check drops it, and it keeps the concurrent writer's row,
// the values the unset targets included.
func TestUpdateWhereUnsetLeavesARecheckMissUntouched_PostgresOnly(t *testing.T) {
	env := setupUnsetDocPG(t)
	tag := uuid.NewString()
	matched, _ := env.unsetWhereRows(t, tag, 2)
	victim, survivor := matched[0], matched[1]
	ran := false
	var victimAfterMove rowSnapshot
	env.txr.testHooks = &txTestHooks{afterCandidates: func(candidates []uuid.UUID) {
		if ran {
			return
		}
		ran = true
		require.Contains(t, candidates, victim, "the victim was a candidate")
		_, err := env.manager.Update(env.ctx, unsetOp(victim, map[string]any{"memo": "moved"}))
		require.NoError(t, err)
		victimAfterMove = env.snapshot(t, unsetSchemaName, victim)
	}}
	t.Cleanup(func() { env.txr.testHooks = nil })

	result, err := env.updateWhereIn(&forma.MutateWhereRequest{SchemaName: unsetSchemaName, Condition: memoIs(tag),
		UpdateOps: []forma.UpdateOp{forma.Unset("label"), forma.Unset("note")}})
	require.NoError(t, err)
	require.True(t, ran)

	assert.Equal(t, []uuid.UUID{survivor}, result.RowIDs, "only the row the re-check kept")
	got := env.attrs(t, unsetSchemaName, victim)
	assert.Equal(t, "L", got["label"], "the dropped row keeps its bound value")
	assert.Equal(t, "N", got["note"], "the dropped row keeps its EAV value")
	assert.Equal(t, "moved", got["memo"], "the concurrent writer's value stands")
	assert.Equal(t, victimAfterMove, env.snapshot(t, unsetSchemaName, victim), "UpdateWhere wrote nothing to the dropped row")
	assert.NotContains(t, env.attrs(t, unsetSchemaName, survivor), "label")
	assert.NotContains(t, env.attrs(t, unsetSchemaName, survivor), "note")
}

// TestUpdateWhereUnsetOverMaxAffectedCommitsNothing pins that an UpdateWhere
// with an unset that matches more rows than MaxAffected removes nothing and
// leaves the Tx usable: the Tx commits a later write.
func TestUpdateWhereUnsetOverMaxAffectedCommitsNothing(t *testing.T) {
	env := setupUnsetDocPG(t)
	tag := uuid.NewString()
	matched, other := env.unsetWhereRows(t, tag, 3)
	before := map[uuid.UUID]rowSnapshot{}
	for _, rowID := range matched {
		before[rowID] = env.snapshot(t, unsetSchemaName, rowID)
	}

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: unsetSchemaName, Condition: memoIs(tag),
			MaxAffected: 2, Updates: map[string]any{"short": "new"},
			UpdateOps: []forma.UpdateOp{forma.Unset("label"), forma.Unset("tags"), forma.Increment("counter", 1)}})
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		assert.Contains(t, err.Error(), "more than MaxAffected 2")
		if _, err := tx.Update(ctx, unsetOp(other, nil, forma.Unset("note"))); err != nil {
			return fmt.Errorf("failed to update %s after the overflow: %w", other, err)
		}
		return nil
	})
	require.NoError(t, err, "the overflow leaves the Tx usable")

	for _, rowID := range matched {
		assert.Equal(t, before[rowID], env.snapshot(t, unsetSchemaName, rowID), "row %s is untouched", rowID)
	}
	assert.NotContains(t, env.attrs(t, unsetSchemaName, other), "note", "the later write committed")
}
