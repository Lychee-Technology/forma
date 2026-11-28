package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the unset operator (#938): what an update that
// unsets an attribute commits to entity_main, eav_data and change_log, on
// both single-row update routes.

// unsetPGTargets are the targets of the persistence matrix: base is the row
// the update starts from, column the main column holding attr, if any.
var unsetPGTargets = []struct {
	name   string
	attr   string
	column string
	base   func() map[string]any
}{
	{"optional bound scalar", "label", "text_01", unsetBaseDoc},
	{"optional EAV scalar", "note", "", unsetBaseDoc},
	{"list with elements", "tags", "", unsetBaseDoc},
	{"list holding only its marker", "tags", "", func() map[string]any {
		doc := unsetBaseDoc()
		doc["tags"] = []any{}
		return doc
	}},
	{"nested scalar", "profile.nick", "", unsetBaseDoc},
	{"nested bound scalar", "profile.title", "text_02", unsetBaseDoc},
	{"nested list", "profile.aliases", "", unsetBaseDoc},
}

// unsetCompanions are what the update does beside the unset, with the
// change each makes to the expected document.
var unsetCompanions = []struct {
	name    string
	updates map[string]any
	ops     []forma.UpdateOp
	apply   func(doc map[string]any)
}{
	{"alone", nil, nil, func(map[string]any) {}},
	{"beside a Set", map[string]any{"short": "new"}, nil, func(doc map[string]any) { doc["short"] = "new" }},
	{"beside an Increment", nil, []forma.UpdateOp{forma.Increment("counter", 2)}, func(doc map[string]any) { doc["counter"] = float64(5) }},
	{"beside a ServerTime", nil, []forma.UpdateOp{forma.ServerTime("due", 0)}, func(map[string]any) {}},
}

func TestUnsetCommitsTheClear(t *testing.T) {
	env := setupUnsetDocPG(t)
	for _, surface := range unsetSurfaces {
		for _, target := range unsetPGTargets {
			for _, companion := range unsetCompanions {
				t.Run(surface.name+"/"+target.name+"/"+companion.name, func(t *testing.T) {
					rowID := env.mustCreate(t, unsetSchemaName, target.base())
					attrID := env.attrID(t, unsetSchemaName, target.attr)
					beforeVersion, _ := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
					if target.column != "" {
						require.NotNil(t, env.mainColumns(t, unsetSchemaName, rowID)[target.column], "the bound value is stored first")
					} else {
						require.NotZero(t, env.eavRowCount(t, unsetSchemaName, rowID, attrID), "the EAV value is stored first")
					}

					op := unsetOp(rowID, companion.updates, append([]forma.UpdateOp{forma.Unset(target.attr)}, companion.ops...)...)
					answered, err := surface.update(env, op)
					require.NoError(t, err)

					want := withoutPath(target.base(), target.attr)
					companion.apply(want)
					got := unsetLogical(t, env.attrs(t, unsetSchemaName, rowID))
					if companion.name == "beside a ServerTime" {
						assert.Contains(t, got, "due", "the ServerTime beside the unset is written")
						delete(got, "due")
					}
					assert.Equal(t, unsetLogical(t, want), got, "read back")
					answer := unsetLogical(t, answered.Attributes)
					delete(answer, "due")
					assert.Equal(t, got, answer, "the answer is the stored row")

					main := env.mainColumns(t, unsetSchemaName, rowID)
					if target.column != "" {
						assert.Contains(t, main, target.column)
						assert.Nil(t, main[target.column], "the bound column is NULL")
					}
					assert.Zero(t, env.eavRowCount(t, unsetSchemaName, rowID, attrID), "no eav_data row of the target is left, a marker included")

					afterVersion, hot := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
					assert.Greater(t, afterVersion, beforeVersion, "the update advances the version")
					assert.Contains(t, hot, afterVersion, "change_log carries the new version")
				})
			}
		}
	}
}

// TestUnsetOfABoundAttributeRemovesItsRetainedEAVRows pins that a bound
// attribute that also has eav_data rows, retained from before its binding,
// leaves neither the column value nor the rows behind: the EAV write replaces
// every active attribute of the row.
func TestUnsetOfABoundAttributeRemovesItsRetainedEAVRows(t *testing.T) {
	env := setupUnsetDocPG(t)
	rowID := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	attrID := env.attrID(t, unsetSchemaName, "label")
	_, err := env.postgresPool.Exec(env.ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text) VALUES ($1, $2, $3, '', 'retained')",
		sanitizeIdentifier(env.tables.EAVData)), env.schemaID(t, unsetSchemaName), rowID, attrID)
	require.NoError(t, err)
	require.Equal(t, 1, env.eavRowCount(t, unsetSchemaName, rowID, attrID))

	_, err = env.manager.Update(env.ctx, unsetOp(rowID, nil, forma.Unset("label")))
	require.NoError(t, err)

	assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"])
	assert.Zero(t, env.eavRowCount(t, unsetSchemaName, rowID, attrID))
	assert.NotContains(t, env.attrs(t, unsetSchemaName, rowID), "label")
}

// TestBoundAndEAVUnsetGiveTheSameLogicalRecord pins that the storage kind
// does not show through a clear: the bound label and the EAV note, holding
// the same value and given the same mutation, leave rows that read back
// alike.
func TestBoundAndEAVUnsetGiveTheSameLogicalRecord(t *testing.T) {
	env := setupUnsetDocPG(t)
	base := unsetBaseDoc()
	base["label"], base["note"] = "same", "same"
	bound := env.mustCreate(t, unsetSchemaName, base)
	eav := env.mustCreate(t, unsetSchemaName, base)

	_, err := env.manager.Update(env.ctx, unsetOp(bound, map[string]any{"note": "kept"}, forma.Unset("label")))
	require.NoError(t, err)
	_, err = env.manager.Update(env.ctx, unsetOp(eav, map[string]any{"label": "kept"}, forma.Unset("note")))
	require.NoError(t, err)

	boundDoc, eavDoc := unsetLogical(t, env.attrs(t, unsetSchemaName, bound)), unsetLogical(t, env.attrs(t, unsetSchemaName, eav))
	assert.Equal(t, "kept", boundDoc["note"])
	assert.Equal(t, "kept", eavDoc["label"])
	delete(boundDoc, "note")
	delete(eavDoc, "label")
	assert.Equal(t, boundDoc, eavDoc, "each row lacks its cleared attribute and is otherwise the same")
}

// TestEmptyListMarkerAndUnsetCommitDifferently pins that []any{} writes the
// empty-list marker and unset removes elements and marker alike.
func TestEmptyListMarkerAndUnsetCommitDifferently(t *testing.T) {
	env := setupUnsetDocPG(t)
	tagsID := env.attrID(t, unsetSchemaName, "tags")
	emptied := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	cleared := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())

	_, err := env.manager.Update(env.ctx, unsetOp(emptied, map[string]any{"tags": []any{}}))
	require.NoError(t, err)
	_, err = env.manager.Update(env.ctx, unsetOp(cleared, nil, forma.Unset("tags")))
	require.NoError(t, err)

	assert.Equal(t, 1, env.eavRowCount(t, unsetSchemaName, emptied, tagsID), "the marker")
	assert.Equal(t, []any{}, env.attrs(t, unsetSchemaName, emptied)["tags"])
	assert.Zero(t, env.eavRowCount(t, unsetSchemaName, cleared, tagsID))
	assert.NotContains(t, env.attrs(t, unsetSchemaName, cleared), "tags")
}

// TestUnsetRepeatRecreatesNothing pins S7 on storage: unsetting an absent
// attribute is an ordinary update, version and change_log included, that
// writes back neither the column nor any EAV row.
func TestUnsetRepeatRecreatesNothing(t *testing.T) {
	env := setupUnsetDocPG(t)
	rowID := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	ops := []forma.UpdateOp{forma.Unset("label"), forma.Unset("tags")}
	_, err := env.manager.Update(env.ctx, unsetOp(rowID, nil, ops...))
	require.NoError(t, err)
	first := env.snapshot(t, unsetSchemaName, rowID)
	firstVersion, _ := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)

	_, err = env.manager.Update(env.ctx, unsetOp(rowID, nil, ops...))
	require.NoError(t, err)
	assert.Equal(t, first.eav, env.snapshot(t, unsetSchemaName, rowID).eav, "no EAV row is recreated")
	assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"])
	secondVersion, hot := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
	assert.Greater(t, secondVersion, firstVersion)
	assert.Contains(t, hot, secondVersion)
}

// TestUnsetFailureRemovesNothing pins that a failed precondition and a
// missing entity keep their errors and leave the row exactly as stored.
func TestUnsetFailureRemovesNothing(t *testing.T) {
	env := setupUnsetDocPG(t)
	rowID := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	before := env.snapshot(t, unsetSchemaName, rowID)
	for _, surface := range unsetSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			op := unsetOp(rowID, nil, forma.Unset("label"), forma.Unset("tags"))
			op.Preconditions = &forma.KvCondition{Attr: "note", Value: "equals:other"}
			_, err := surface.update(env, op)
			require.ErrorIs(t, err, forma.ErrPreconditionFailed)
			assert.Equal(t, before, env.snapshot(t, unsetSchemaName, rowID))

			_, err = surface.update(env, unsetOp(uuid.New(), nil, forma.Unset("label")))
			require.ErrorIs(t, err, forma.ErrNotFound)
			assert.Equal(t, before, env.snapshot(t, unsetSchemaName, rowID))
		})
	}
}

// TestUnsetRefusalsLeaveATxUsable pins that every intake refusal, a refused
// target or an overlap, sends no statement: the Tx goes on to commit an
// unset, and the row is otherwise untouched.
func TestUnsetRefusalsLeaveATxUsable(t *testing.T) {
	env := setupUnsetDocPG(t)
	rowID := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	before := env.snapshot(t, unsetSchemaName, rowID)
	refused := []*forma.EntityOperation{
		unsetOp(rowID, nil, forma.Unset("missing")),
		unsetOp(rowID, nil, forma.Unset("created")),
		unsetOp(rowID, nil, forma.Unset("profile")),
		unsetOp(rowID, nil, forma.Unset("items")),
		unsetOp(rowID, nil, forma.Unset("items.sku")),
		unsetOp(rowID, nil, forma.Unset("tags[0]")),
		unsetOp(rowID, nil, forma.Unset("note"), forma.Unset("note")),
		unsetOp(rowID, map[string]any{"note": "x"}, forma.Unset("note")),
		unsetOp(rowID, map[string]any{"profile": map[string]any{"nick": "x"}}, forma.Unset("profile.nick")),
		unsetOp(rowID, map[string]any{"profile.nick": "x"}, forma.Unset("profile.nick")),
		unsetOp(rowID, map[string]any{"profile": "flat"}, forma.Unset("profile.nick")),
		unsetOp(rowID, nil, forma.Increment("counter", 1), forma.Unset("counter")),
		unsetOp(rowID, nil, forma.ServerTime("due", 0), forma.Unset("due")),
	}
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		for i, op := range refused {
			if _, err := tx.Update(ctx, op); !errors.Is(err, forma.ErrInvalidInput) {
				return fmt.Errorf("refusal %d answered %v", i, err)
			}
		}
		_, err := tx.Update(ctx, unsetOp(rowID, nil, forma.Unset("label")))
		return err
	})
	require.NoError(t, err)
	after := env.snapshot(t, unsetSchemaName, rowID)
	assert.NotEqual(t, before.main, after.main, "the Tx committed the unset")
	assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"])
	assert.Equal(t, before.eav, after.eav, "no refused operation wrote EAV")
}
