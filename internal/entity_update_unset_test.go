package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests of the unset operator (#938) over the mock repository: the
// targets intake accepts and refuses, the one-writer rule, and the document
// the merge builds. What PostgreSQL stores is pinned by
// entity_update_unset_integration_test.go.

// unsetTargets are the targets the contract supports, each with whether a
// main column holds it.
var unsetTargets = []struct {
	attr   string
	column string
}{
	{"label", "text_01"},
	{"note", ""},
	{"tags", ""},
	{"profile.nick", ""},
	{"profile.aliases", ""},
	{"profile.title", "text_02"},
}

func TestUnsetRemovesItsTarget(t *testing.T) {
	for _, target := range unsetTargets {
		t.Run(target.attr, func(t *testing.T) {
			h := newUnsetHarness(t, unsetStrict)
			rowID := h.create(t, unsetBaseDoc())

			got, err := h.update(rowID, nil, forma.Unset(target.attr))
			require.NoError(t, err)

			want := withoutPath(unsetBaseDoc(), target.attr)
			assert.Equal(t, unsetLogical(t, want), unsetLogical(t, h.stored(t, rowID)), "stored")
			assert.Equal(t, unsetLogical(t, want), unsetLogical(t, got.Attributes), "answered")
			var cleared []string
			if target.column != "" {
				cleared = []string{target.column}
			}
			assert.Equal(t, cleared, h.record(rowID).ClearedColumns, "the merged record carries the column it clears")
		})
	}
}

// TestUnsetPrunesTheObjectsItEmpties pins that the validated document is the
// stored one: an object left without members is gone, not kept as {}.
func TestUnsetPrunesTheObjectsItEmpties(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	rowID := h.create(t, unsetBaseDoc())

	_, err := h.update(rowID, map[string]any{"note": "changed"},
		forma.Unset("profile.nick"), forma.Unset("profile.aliases"), forma.Unset("profile.title"))
	require.NoError(t, err)

	got := h.stored(t, rowID)
	assert.NotContains(t, got, "profile")
	assert.Equal(t, "changed", got["note"])
	assert.Equal(t, []string{"text_02"}, h.record(rowID).ClearedColumns)
}

// TestUnsetOfAnAbsentAttributeIsANoOp pins S7: the update still runs, and a
// repeat recreates nothing.
func TestUnsetOfAnAbsentAttributeIsANoOp(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	rowID := h.create(t, unsetBaseDoc())
	for range 2 {
		_, err := h.update(rowID, nil, forma.Unset("note"), forma.Unset("profile.title"))
		require.NoError(t, err)
		assert.Equal(t, unsetLogical(t, withoutPath(withoutPath(unsetBaseDoc(), "note"), "profile.title")), unsetLogical(t, h.stored(t, rowID)))
	}
	assert.Equal(t, 2, h.repo.mergeCalls, "the repeat is an update like any other")

	// Never held at all: the update writes the row as it was.
	before := h.stored(t, rowID)
	_, err := h.update(rowID, nil, forma.Unset("due"))
	require.NoError(t, err)
	assert.Equal(t, before, h.stored(t, rowID))
}

// TestUnsetRefusesItsTargetAtIntake pins S1: each refusal is ErrInvalidInput
// naming the attribute, and no write reaches the repository.
func TestUnsetRefusesItsTargetAtIntake(t *testing.T) {
	cases := []struct {
		name string
		op   forma.UpdateOp
		want string
	}{
		{"unknown attribute", forma.Unset("missing"), "unset names attribute 'missing', which schema 'unsetdoc' does not define"},
		{"unknown nested attribute", forma.Unset("profile.missing"), "unset names attribute 'profile.missing'"},
		{"system column alias", forma.Unset("created"), "unset names datetime attribute 'created', which is bound to system column 'ltbase_created_at'"},
		{"physical system column", forma.Unset("ltbase_created_at"), "unset names 'ltbase_created_at', an entity_main column"},
		{"physical main column", forma.Unset("text_01"), "unset names 'text_01', an entity_main column"},
		{"object subtree", forma.Unset("profile"), "unset names 'profile', an object in schema 'unsetdoc'"},
		{"array of objects", forma.Unset("items"), "unset names 'items', an array of objects in schema 'unsetdoc'"},
		{"member under an array of objects", forma.Unset("items.sku"), "attribute 'items.sku' is inside the array of objects 'items'"},
		{"indexed spelling", forma.Unset("tags[0]"), "unset names 'tags[0]', which is not an attribute's dotted name"},
		{"indexed member spelling", forma.Unset("items[0].sku"), "unset names 'items[0].sku'"},
		{"wildcard spelling", forma.Unset("profile.*"), "unset names 'profile.*'"},
		{"JSONPath spelling", forma.Unset("$.note"), "unset names '$.note'"},
		{"empty segment", forma.Unset("profile..nick"), "unset names 'profile..nick'"},
		{"no attribute", forma.Unset(""), "update operator unset names no attribute"},
		{"with By", forma.UpdateOp{Attribute: "note", Kind: forma.UpdateOpUnset, By: 1}, "unset on attribute 'note' sets By or Offset"},
		{"with Offset", forma.UpdateOp{Attribute: "note", Kind: forma.UpdateOpUnset, Offset: time.Second}, "unset on attribute 'note' sets By or Offset"},
	}
	for _, mode := range unsetModes {
		for _, tc := range cases {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				h := newUnsetHarness(t, mode)
				_, err := h.update(uuid.New(), nil, tc.op)
				requireUnsetRefusedBeforeRepository(t, h, err, tc.want)
			})
		}
	}
}

// TestUnsetRefusesANestedTargetWithoutDerivedAncestry pins that a nested
// attribute whose ArrayAncestors is nil cannot be placed, so it is refused:
// cond649's registry derives no ancestry.
func TestUnsetRefusesANestedTargetWithoutDerivedAncestry(t *testing.T) {
	f := newCondFixture(t)
	op := condOp(uuid.New())
	op.UpdateOps = []forma.UpdateOp{forma.Unset("stats.note")}
	_, err := f.em.Update(f.ctx, op)
	requireRefusedBeforeRepository(t, f, err, "unset names nested attribute 'stats.note' of schema 'cond649', whose array ancestry the registry did not derive")

	// A top-level attribute needs no ancestry to be placed.
	rowID := f.seed(t, map[string]any{"name": "a", "label": "l"})
	op = condOp(rowID)
	op.UpdateOps = []forma.UpdateOp{forma.Unset("label")}
	_, err = f.em.Update(f.ctx, op)
	require.NoError(t, err)
	assert.NotContains(t, f.stored(t, rowID), "label")
}

// TestUnsetRefusesARelationAttribute pins that an x-relation subtree, derived
// on read, is never unset, its root included.
func TestUnsetRefusesARelationAttribute(t *testing.T) {
	for _, attr := range []string{"contactSnapshot.birthday", "contactSnapshot"} {
		t.Run(attr, func(t *testing.T) {
			h := newShippedSchemaHarness(t)
			op := &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: uuid.New()},
				UpdateOps:        []forma.UpdateOp{forma.Unset(attr)},
			}
			_, err := h.manager.Update(context.Background(), op)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			message, published := forma.ResolvePublicMessage(err)
			require.True(t, published)
			assert.Contains(t, message, "attribute '"+attr+"' belongs to relation 'contactSnapshot'")
			assert.Zero(t, h.repo.mergeCalls)
		})
	}
}

// TestUnsetOverlapsAreRefused pins S3: an unset is its attribute's only
// writer, in either spelling, and the refusal precedes any write.
func TestUnsetOverlapsAreRefused(t *testing.T) {
	cases := []struct {
		name    string
		updates map[string]any
		ops     []forma.UpdateOp
		want    string
	}{
		{"duplicate unset", nil, []forma.UpdateOp{forma.Unset("note"), forma.Unset("note")},
			"attribute 'note' has more than one update operator"},
		{"set and unset", map[string]any{"note": "x"}, []forma.UpdateOp{forma.Unset("note")},
			"attribute 'note' appears in both updates (as 'note') and update operators"},
		{"set and unset of a list", map[string]any{"tags": []any{}}, []forma.UpdateOp{forma.Unset("tags")},
			"attribute 'tags' appears in both updates (as 'tags')"},
		{"nested set and dotted unset", map[string]any{"profile": map[string]any{"nick": "x"}}, []forma.UpdateOp{forma.Unset("profile.nick")},
			"attribute 'profile.nick' appears in both updates (as 'profile.nick')"},
		{"literal dotted set and unset", map[string]any{"profile.nick": "x"}, []forma.UpdateOp{forma.Unset("profile.nick")},
			"attribute 'profile.nick' appears in both updates (as 'profile.nick')"},
		{"increment and unset", nil, []forma.UpdateOp{forma.Increment("counter", 1), forma.Unset("counter")},
			"attribute 'counter' has more than one update operator"},
		{"unset and server_time", nil, []forma.UpdateOp{forma.Unset("due"), forma.ServerTime("due", 0)},
			"attribute 'due' has more than one update operator"},
		{"parent replaced by a scalar", map[string]any{"profile": "flat"}, []forma.UpdateOp{forma.Unset("profile.nick")},
			"attribute 'profile.nick' appears in both updates (as 'profile')"},
		{"parent replaced by null", map[string]any{"profile": nil}, []forma.UpdateOp{forma.Unset("profile.nick")},
			"attribute 'profile.nick' appears in both updates (as 'profile')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newUnsetHarness(t, unsetStrict)
			_, err := h.update(uuid.New(), tc.updates, tc.ops...)
			requireUnsetRefusedBeforeRepository(t, h, err, tc.want)
		})
	}
}

// TestUnsetBesideAMergedParentIsNoOverlap pins the other side of S3: an
// object in Updates that merges into the row's object writes its own
// members only, so it may sit beside an unset of another member.
func TestUnsetBesideAMergedParentIsNoOverlap(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	rowID := h.create(t, unsetBaseDoc())
	updates := map[string]any{"profile": map[string]any{"title": "new"}}
	_, err := h.update(rowID, updates, forma.Unset("profile.nick"))
	require.NoError(t, err)
	profile := h.stored(t, rowID)["profile"].(map[string]any)
	assert.Equal(t, "new", profile["title"])
	assert.NotContains(t, profile, "nick")
	assert.Equal(t, map[string]any{"profile": map[string]any{"title": "new"}}, updates, "the caller's Updates are not written to")
}

// TestRemoveDocumentValue pins the merge's removal on its own: the caller's
// maps are copied, not written to, an array on the way is refused as the
// backstop for one the metadata does not show, and a missing path is a
// no-op.
func TestRemoveDocumentValue(t *testing.T) {
	caller := map[string]any{"b": 1, "c": 2}
	doc := map[string]any{"a": caller}
	require.NoError(t, removeDocumentValue(doc, "a.b"))
	assert.Equal(t, map[string]any{"a": map[string]any{"c": 2}}, doc)
	assert.Equal(t, map[string]any{"b": 1, "c": 2}, caller, "the caller's map is not written to")

	doc = map[string]any{"a": map[string]any{"b": map[string]any{"c": 1}}, "d": 1}
	require.NoError(t, removeDocumentValue(doc, "a.b.c"))
	assert.Equal(t, map[string]any{"d": 1}, doc, "every object the removal empties goes, the root excepted")

	doc = map[string]any{"a": map[string]any{}}
	require.NoError(t, removeDocumentValue(doc, "a.b"))
	require.NoError(t, removeDocumentValue(doc, "x.y"))
	assert.Equal(t, map[string]any{"a": map[string]any{}}, doc, "an absent path removes nothing, an empty object it did not empty included")

	err := removeDocumentValue(map[string]any{"items": []any{map[string]any{"sku": "s"}}}, "items.sku")
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	assert.Contains(t, err.Error(), "attribute 'items.sku' is inside the array 'items'")
}

// TestUnsetDiscardsTheStoredValueUndecoded pins that unset, like server_time
// (#590), clears a stored value the read refuses: the value is removed before
// anything is decoded.
func TestUnsetDiscardsTheStoredValueUndecoded(t *testing.T) {
	for _, attr := range []string{"seenAt", "seenOn"} {
		for _, legacy := range legacyDateImages {
			t.Run(attr+"/"+legacy.name, func(t *testing.T) {
				row := seedLegacyDateRow(t, forma.RequiredPolicyOptional, attr, legacy.image)
				_, err := row.em.Update(context.Background(), dateDestUpdate(row.rowID, map[string]any{"visit": map[string]any{"note": "x"}}))
				assertLegacyDateReadError(t, err, attr, legacy.rule, row.rowID)

				op := dateDestUpdate(row.rowID, nil)
				op.UpdateOps = []forma.UpdateOp{forma.Unset(attr)}
				_, err = row.em.Update(context.Background(), op)
				require.NoError(t, err)
				stored, err := row.stored(t)
				require.NoError(t, err, "the cleared row reads")
				assert.NotContains(t, stored, attr)
				assert.Equal(t, keptInstant, stored["openedAt"])
			})
		}
	}
}

// requireUnsetRefusedBeforeRepository asserts err is an ErrInvalidInput
// whose published message holds want, and that no write reached the
// repository.
func requireUnsetRefusedBeforeRepository(t *testing.T, h *unsetHarness, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "an intake refusal publishes its message: %v", err)
	assert.Contains(t, message, want)
	assert.Zero(t, h.repo.mergeCalls, "no merge may run")
}
