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

// Real-PostgreSQL tests of the unset operator on Tx.UpdateWhere, Tx.Claim
// and Tx.Upsert's update action (#939): each stores what Tx.Update stores
// for the same mutation, refuses at intake what Update refuses, and releases
// a cleared unique-key entry in its own transaction.

// unsetDocMutation is the parity mutation of an unsetdoc row tagged tag: a
// Set, an Increment and the unset of target.
func unsetDocMutation(rowID uuid.UUID, tag, target string) unsetMutation {
	return unsetMutation{schema: unsetSchemaName, rowID: rowID, create: unsetTaggedDoc(tag),
		selector: unsetTagged("memo", tag, target), updates: map[string]any{"short": "new"},
		ops: []forma.UpdateOp{forma.Increment("counter", 2), forma.Unset(target)}}
}

// TestUnsetSurfacesStoreWhatUpdateStores gives two rows of one document the
// same mutation, one through Tx.Update and one through the surface: for a
// bound and an EAV target, the rows' entity_main columns and eav_data rows
// are the same, the clear included, and the surface advances version and
// change_log as Update does. unsetdoc declares no unique key, so neither row
// holds an entry; TestUnsetSurfacesReleaseTheUniqueKeyEntry covers keys.
func TestUnsetSurfacesStoreWhatUpdateStores(t *testing.T) {
	env := setupUnsetDocPG(t)
	targets := []struct{ name, attr, column string }{{"bound", "label", "text_01"}, {"EAV", "note", ""}}
	for _, target := range targets {
		attrID := env.attrID(t, unsetSchemaName, target.attr)
		for _, surface := range unsetWriteSurfaces {
			t.Run(target.name+"/"+surface.name, func(t *testing.T) {
				tag := uuid.NewString()
				// The reference row is cleared first, so it no longer matches
				// the selector when the surface runs.
				reference := env.mustCreate(t, unsetSchemaName, unsetTaggedDoc(tag))
				require.NoError(t, env.applyIn(unsetViaUpdate, unsetDocMutation(reference, tag, target.attr)))
				rowID := env.mustCreate(t, unsetSchemaName, unsetTaggedDoc(tag))
				beforeVersion, _ := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)

				require.NoError(t, env.applyIn(surface, unsetDocMutation(rowID, tag, target.attr)))

				assert.Equal(t, env.storedImage(t, unsetSchemaName, reference), env.storedImage(t, unsetSchemaName, rowID),
					"entity_main and eav_data as Update stores them")
				if target.column != "" {
					assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)[target.column], "the bound column is NULL")
				}
				assert.Zero(t, env.eavRowCount(t, unsetSchemaName, rowID, attrID), "no eav_data row of the target is left")
				assert.Empty(t, env.keyEntriesOfRow(t, rowID))
				afterVersion, hot := env.hotVersion(t, env.postgresPool, unsetSchemaName, rowID)
				assert.Greater(t, afterVersion, beforeVersion, "the surface advances the version")
				assert.Contains(t, hot, afterVersion, "change_log carries the new version")
			})
		}
	}
}

// unsetSurfaceRefusal is a mutation every surface refuses at intake, and
// the text its published message holds.
type unsetSurfaceRefusal struct {
	updates map[string]any
	ops     []forma.UpdateOp
	want    string
}

// unsetSurfaceRefusals are the refused targets and the S3 overlaps, as
// Update refuses them (TestUnsetRefusalsLeaveATxUsable).
var unsetSurfaceRefusals = []unsetSurfaceRefusal{
	{nil, []forma.UpdateOp{forma.Unset("missing")}, "unset names attribute 'missing'"},
	{nil, []forma.UpdateOp{forma.Unset("created")}, "attribute 'created', which is bound to system column 'ltbase_created_at'"},
	{nil, []forma.UpdateOp{forma.Unset("ltbase_row_id")}, "unset names 'ltbase_row_id', an entity_main column"},
	{nil, []forma.UpdateOp{forma.Unset("profile")}, "unset names 'profile', an object"},
	{nil, []forma.UpdateOp{forma.Unset("items")}, "unset names 'items', an array of objects"},
	{nil, []forma.UpdateOp{forma.Unset("items.sku")}, "attribute 'items.sku' is inside the array of objects 'items'"},
	{nil, []forma.UpdateOp{forma.Unset("tags[0]")}, "unset names 'tags[0]'"},
	{nil, []forma.UpdateOp{{Attribute: "note", Kind: forma.UpdateOpUnset, By: 1}}, "unset on attribute 'note' sets By or Offset"},
	{nil, []forma.UpdateOp{forma.Unset("note"), forma.Unset("note")}, "attribute 'note' has more than one update operator"},
	{map[string]any{"note": "x"}, []forma.UpdateOp{forma.Unset("note")}, "attribute 'note' appears in both updates"},
	{map[string]any{"profile": map[string]any{"nick": "x"}}, []forma.UpdateOp{forma.Unset("profile.nick")}, "attribute 'profile.nick' appears in both updates"},
	{nil, []forma.UpdateOp{forma.Increment("counter", 1), forma.Unset("counter")}, "attribute 'counter' has more than one update operator"},
	{nil, []forma.UpdateOp{forma.ServerTime("due", 0), forma.Unset("due")}, "attribute 'due' has more than one update operator"},
}

// TestUnsetSurfacesRefuseAtIntake runs every refusal through each surface in
// one Tx: each is ErrInvalidInput naming its target and sends no statement,
// so the Tx goes on to commit an unset through the same surface, and the
// row holds that unset alone.
func TestUnsetSurfacesRefuseAtIntake(t *testing.T) {
	env := setupUnsetDocPG(t)
	sent := env.countStatements(t)
	for _, surface := range unsetWriteSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			tag := uuid.NewString()
			rowID := env.mustCreate(t, unsetSchemaName, unsetTaggedDoc(tag))
			before := env.snapshot(t, unsetSchemaName, rowID)
			mutation := func(updates map[string]any, ops ...forma.UpdateOp) unsetMutation {
				return unsetMutation{schema: unsetSchemaName, rowID: rowID, create: unsetTaggedDoc(tag),
					selector: unsetTagged("memo", tag, "label"), updates: updates, ops: ops}
			}
			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				for i, refusal := range unsetSurfaceRefusals {
					requireRefusedBeforeSQL(t, sent, fmt.Sprintf("refusal %d", i), refusal.want, func() error {
						return surface.apply(ctx, tx, mutation(refusal.updates, refusal.ops...))
					})
				}
				return surface.apply(ctx, tx, mutation(nil, forma.Unset("label")))
			})
			require.NoError(t, err, "the Tx commits after its refusals")
			after := env.snapshot(t, unsetSchemaName, rowID)
			assert.Nil(t, env.mainColumns(t, unsetSchemaName, rowID)["text_01"], "the Tx committed the unset")
			assert.Equal(t, before.eav, after.eav, "no refused mutation wrote EAV")
			assert.Equal(t, withoutPath(unsetLogical(t, unsetTaggedDoc(tag)), "label"), unsetLogical(t, env.attrs(t, unsetSchemaName, rowID)))
		})
	}
}

// requireRefusedBeforeSQL asserts that call answers ErrInvalidInput whose
// published message holds want, without sending a statement.
func requireRefusedBeforeSQL(t *testing.T, sent interface{ Load() int64 }, name, want string, call func() error) {
	t.Helper()
	before := sent.Load()
	err := call()
	if !assert.ErrorIs(t, err, forma.ErrInvalidInput, name) {
		return
	}
	message, published := forma.ResolvePublicMessage(err)
	assert.True(t, published, "%s publishes its message: %v", name, err)
	assert.Contains(t, message, want, "%s names its target", name)
	assert.Equal(t, before, sent.Load(), "%s sent a statement", name)
}

// unsetKeysMutation clears attr of the unsetkeys row tagged tag (its note).
func unsetKeysMutation(rowID uuid.UUID, tag, attr string) unsetMutation {
	doc := unsetKeysDoc()
	doc["note"] = tag
	return unsetMutation{schema: unsetKeysSchemaName, rowID: rowID, create: doc,
		selector: unsetTagged("note", tag, attr), ops: []forma.UpdateOp{forma.Unset(attr)}}
}

// unsetKeySurfaces are the surfaces that may clear a key component: Claim
// refuses to (TestClaimRefusesAnUnsetOfAUniqueKeyComponent).
var unsetKeySurfaces = []unsetTxSurface{unsetViaUpdateWhere, unsetViaUpsert}

// TestUnsetSurfacesReleaseTheUniqueKeyEntry clears each key component of
// unsetkeys through each surface: the entry is released in the clearing
// transaction, the other keys keep theirs as with Update, and another
// entity can then take the value.
func TestUnsetSurfacesReleaseTheUniqueKeyEntry(t *testing.T) {
	for _, surface := range unsetKeySurfaces {
		for _, tc := range unsetKeyCases {
			t.Run(surface.name+"/"+tc.name, func(t *testing.T) {
				env := setupUnsetKeysPG(t)
				doc := unsetKeysDoc()
				doc["note"] = "a"
				a := env.mustCreate(t, unsetKeysSchemaName, doc)
				require.ElementsMatch(t, []int16{1, 2, 3}, env.heldKeys(t, a))

				require.NoError(t, env.applyIn(surface, unsetKeysMutation(a, "a", tc.attr)))
				assert.NotContains(t, env.heldKeys(t, a), tc.key, "the cleared component releases the entry")
				assert.Len(t, env.heldKeys(t, a), 2, "the other keys keep theirs")
				assert.NotContains(t, env.attrs(t, unsetKeysSchemaName, a), tc.attr)
				env.assertKeyTableMatchesRows(t)

				retake := map[string]any{"note": "b"}
				for k, v := range tc.retake {
					retake[k] = v
				}
				b := env.mustCreate(t, unsetKeysSchemaName, retake)
				assert.Contains(t, env.heldKeys(t, b), tc.key, "another entity takes the value")
				env.assertKeyTableMatchesRows(t)
			})
		}
	}
}

// TestUnsetSurfacesRolledBackKeepTheReservation clears a key component
// through each surface in a Tx that rolls back: the row and the key table
// are as before, and another entity still conflicts on the value.
func TestUnsetSurfacesRolledBackKeepTheReservation(t *testing.T) {
	rollback := errors.New("roll back")
	for _, surface := range unsetKeySurfaces {
		for _, tc := range unsetKeyCases {
			t.Run(surface.name+"/"+tc.name, func(t *testing.T) {
				env := setupUnsetKeysPG(t)
				doc := unsetKeysDoc()
				doc["note"] = "a"
				a := env.mustCreate(t, unsetKeysSchemaName, doc)
				before := env.snapshot(t, unsetKeysSchemaName, a)
				entries := env.keyEntries(t)

				err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
					if err := surface.apply(ctx, tx, unsetKeysMutation(a, "a", tc.attr)); err != nil {
						return err
					}
					return rollback
				})
				require.ErrorIs(t, err, rollback)

				assert.Equal(t, before, env.snapshot(t, unsetKeysSchemaName, a), "no partial state")
				assert.Equal(t, entries, env.keyEntries(t), "the reservation is kept")
				retake := map[string]any{"note": "b"}
				for k, v := range tc.retake {
					retake[k] = v
				}
				_, err = env.manager.Create(env.ctx, txCreateOp(unsetKeysSchemaName, retake))
				require.ErrorIs(t, err, forma.ErrUniqueConflict)
				env.assertKeyTableMatchesRows(t)
			})
		}
	}
}

// TestUpsertCreateBranchDoesNotRunUnset upserts a row ID no row holds with
// an update action that unsets an attribute the create sets: the create
// runs as given and the unset does not, while an unset intake refuses is
// refused all the same, before any SQL, and creates nothing.
func TestUpsertCreateBranchDoesNotRunUnset(t *testing.T) {
	env := setupUnsetDocPG(t)
	sent := env.countStatements(t)
	upsert := func(rowID uuid.UUID, ops ...forma.UpdateOp) *forma.UpsertRequest {
		return &forma.UpsertRequest{
			Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetSchemaName, RowID: rowID}, Data: unsetBaseDoc()},
			Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, UpdateOps: ops}
	}
	refused, created := uuid.New(), uuid.New()
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		requireRefusedBeforeSQL(t, sent, "the create branch's invalid unset", "unset names 'items', an array of objects", func() error {
			_, err := tx.Upsert(ctx, upsert(refused, forma.Unset("items")))
			return err
		})
		result, err := tx.Upsert(ctx, upsert(created, forma.Unset("label"), forma.Unset("tags")))
		if err != nil {
			return err
		}
		assert.Equal(t, forma.UpsertCreated, result.Outcome)
		return nil
	})
	require.NoError(t, err)

	assert.False(t, env.mainRowExists(t, unsetSchemaName, refused), "the refused upsert created nothing")
	assert.Equal(t, unsetLogical(t, unsetBaseDoc()), unsetLogical(t, env.attrs(t, unsetSchemaName, created)), "the create runs without the unset")
	assert.Equal(t, "L", env.mainColumns(t, unsetSchemaName, created)["text_01"])
}

// TestUpsertDoNothingRefusesUnset pins that do_nothing takes no UpdateOps,
// an unset included: the refusal comes before any SQL, the existing row is
// untouched, and the Tx stays usable.
func TestUpsertDoNothingRefusesUnset(t *testing.T) {
	env := setupUnsetDocPG(t)
	rowID := env.mustCreate(t, unsetSchemaName, unsetBaseDoc())
	before := env.snapshot(t, unsetSchemaName, rowID)
	sent := env.countStatements(t)
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		requireRefusedBeforeSQL(t, sent, "do_nothing with an unset", "takes no Preconditions, Updates or UpdateOps", func() error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetSchemaName, RowID: rowID}, Data: unsetBaseDoc()},
				Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertDoNothing, UpdateOps: []forma.UpdateOp{forma.Unset("label")}})
			return err
		})
		result, err := tx.Upsert(ctx, &forma.UpsertRequest{
			Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetSchemaName, RowID: rowID}, Data: unsetBaseDoc()},
			Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertDoNothing})
		if err != nil {
			return err
		}
		assert.Equal(t, forma.UpsertExisting, result.Outcome)
		return nil
	})
	require.NoError(t, err, "the Tx commits after the refusal")
	assert.Equal(t, before, env.snapshot(t, unsetSchemaName, rowID), "the existing row is untouched")
}
