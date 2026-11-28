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

// Real-PostgreSQL tests of the unset operator on Tx.UpdateWhere, Tx.Claim
// and Tx.Upsert's update action over unsetkeys, whose rows hold unique-key
// entries (#939): a successful clear leaves the key table as Tx.Update
// leaves it, and Upsert's failed precondition clears nothing.

// unsetKeysParityDoc is unsetKeysDoc tagged a (its note), with the non-key
// attributes label (bound to text_03) and memo (EAV) present. The row holds
// an entry of each of its three keys.
func unsetKeysParityDoc() map[string]any {
	doc := unsetKeysDoc()
	doc["note"], doc["label"], doc["memo"] = "a", "L", "M"
	return doc
}

// unsetKeysParityTargets are the attributes the keyed parity clears: the two
// that belong to no key, which every surface may clear, and a bound and an
// EAV key component, which Claim refuses to clear
// (TestClaimRefusesAnUnsetOfAUniqueKeyComponent).
var unsetKeysParityTargets = []struct {
	name, attr string
	column     string // the bound target's entity_main column; empty for EAV
	keyed      bool
	held       []int16 // the keys the row holds an entry of afterwards
}{
	{"bound non-key", "label", "text_03", false, []int16{1, 2, 3}},
	{"EAV non-key", "memo", "", false, []int16{1, 2, 3}},
	{"bound key component", "slug", "text_01", true, []int16{2, 3}},
	{"EAV key component", "handle", "", true, []int16{1, 3}},
}

// keyedOutcome is what a mutation of an unsetKeysParityDoc row commits: the
// row's image and its key entries, without the row ID either holds.
type keyedOutcome struct {
	image storedImage
	keys  []keyEntry
}

// keyedOutcomeOf creates an unsetKeysParityDoc row, checks it holds its
// three entries, clears attr through surface beside a Set of note, and
// answers what was committed. It checks that version and change_log
// advanced and that the key table still matches the live rows.
func (e *txEnv) keyedOutcomeOf(t *testing.T, surface unsetTxSurface, attr string) keyedOutcome {
	t.Helper()
	rowID := e.mustCreate(t, unsetKeysSchemaName, unsetKeysParityDoc())
	require.ElementsMatch(t, []int16{1, 2, 3}, e.heldKeys(t, rowID), "the row holds its reservations before the mutation")
	beforeVersion, _ := e.hotVersion(t, e.postgresPool, unsetKeysSchemaName, rowID)

	require.NoError(t, e.applyIn(surface, unsetMutation{schema: unsetKeysSchemaName, rowID: rowID, create: unsetKeysParityDoc(),
		selector: unsetTagged("note", "a", attr), updates: map[string]any{"note": "done"}, ops: []forma.UpdateOp{forma.Unset(attr)}}))

	afterVersion, hot := e.hotVersion(t, e.postgresPool, unsetKeysSchemaName, rowID)
	assert.Greater(t, afterVersion, beforeVersion, "%s advances the version", surface.name)
	assert.Contains(t, hot, afterVersion, "change_log carries the new version")
	e.assertKeyTableMatchesRows(t)
	outcome := keyedOutcome{image: e.storedImage(t, unsetKeysSchemaName, rowID)}
	for _, entry := range e.keyEntriesOfRow(t, rowID) {
		entry.rowID = uuid.Nil
		outcome.keys = append(outcome.keys, entry)
	}
	return outcome
}

// TestUnsetSurfacesKeepTheKeysUpdateKeeps clears a bound and an EAV
// attribute of a row that holds three key entries, once through Tx.Update
// and once through the surface, each on fresh tables so that the two rows
// can hold the same key values: the committed entity_main columns, eav_data
// rows and key entries are the same. A non-key clear keeps every entry
// with its value; a key-component clear, which Claim refuses, releases that
// key's entry only.
func TestUnsetSurfacesKeepTheKeysUpdateKeeps(t *testing.T) {
	for _, target := range unsetKeysParityTargets {
		surfaces := unsetWriteSurfaces
		if target.keyed {
			surfaces = unsetKeySurfaces
		}
		for _, surface := range surfaces {
			t.Run(target.name+"/"+surface.name, func(t *testing.T) {
				reference := setupUnsetKeysPG(t)
				want := reference.keyedOutcomeOf(t, unsetViaUpdate, target.attr)
				var held []int16
				for _, entry := range want.keys {
					held = append(held, entry.keyID)
				}
				require.ElementsMatch(t, target.held, held, "Tx.Update keeps these entries")
				if target.column != "" {
					require.Contains(t, want.image.main, target.column)
					require.Nil(t, want.image.main[target.column], "Tx.Update writes the bound column NULL")
				}
				attrID := float64(reference.attrID(t, unsetKeysSchemaName, target.attr))
				for _, row := range want.image.eav {
					require.NotEqual(t, attrID, row["attr_id"], "Tx.Update leaves no eav_data row of the target")
				}

				got := setupUnsetKeysPG(t).keyedOutcomeOf(t, surface, target.attr)
				assert.Equal(t, want.image, got.image, "entity_main and eav_data as Update stores them")
				assert.Equal(t, want.keys, got.keys, "the key entries as Update keeps them")
			})
		}
	}
}

// TestUpsertFailedPreconditionUnsetsNothing sends Upsert's update action to
// a live row with a precondition the row fails, and unsets of bound and EAV
// key components and non-key attributes: the upsert is
// ErrPreconditionFailed, the row, its version, change_log and key entries
// are as before, and the same Tx goes on to commit a write of another row.
func TestUpsertFailedPreconditionUnsetsNothing(t *testing.T) {
	env := setupUnsetKeysPG(t)
	rowID := env.mustCreate(t, unsetKeysSchemaName, unsetKeysParityDoc())
	other := env.mustCreate(t, unsetKeysSchemaName, map[string]any{"note": "b", "slug": "s2"})
	before := env.snapshot(t, unsetKeysSchemaName, rowID)
	beforeVersion, beforeHot := env.hotVersion(t, env.postgresPool, unsetKeysSchemaName, rowID)
	entries := env.keyEntries(t)
	require.ElementsMatch(t, []int16{1, 2, 3}, env.heldKeys(t, rowID), "the row holds its reservations before the upsert")

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Upsert(ctx, &forma.UpsertRequest{
			Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetKeysSchemaName, RowID: rowID},
				Data: unsetKeysParityDoc()},
			Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate,
			Preconditions: &forma.KvCondition{Attr: "note", Value: "equals:other"},
			Updates:       map[string]any{"note": "done"},
			UpdateOps: []forma.UpdateOp{forma.Unset("slug"), forma.Unset("handle"), forma.Unset("tenant"),
				forma.Unset("label"), forma.Unset("memo")}})
		require.ErrorIs(t, err, forma.ErrPreconditionFailed)
		if _, err := tx.Update(ctx, unsetKeysOp(other, map[string]any{"note": "c"})); err != nil {
			return fmt.Errorf("failed to update %s after the failed precondition: %w", other, err)
		}
		return nil
	})
	require.NoError(t, err, "the expected refusal leaves the Tx usable")

	assert.Equal(t, before, env.snapshot(t, unsetKeysSchemaName, rowID), "entity_main, eav_data and change_log are as before")
	afterVersion, afterHot := env.hotVersion(t, env.postgresPool, unsetKeysSchemaName, rowID)
	assert.Equal(t, beforeVersion, afterVersion, "the version does not move")
	assert.Equal(t, beforeHot, afterHot)
	assert.Equal(t, entries, env.keyEntries(t), "every reservation is kept")
	assert.Equal(t, "c", env.attrs(t, unsetKeysSchemaName, other)["note"], "the later write committed")
	env.assertKeyTableMatchesRows(t)
}
