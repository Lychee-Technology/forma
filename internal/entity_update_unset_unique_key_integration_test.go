package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the unset operator on unique keys (#938): a key is
// planned from the final record, so a cleared component releases its entry in
// the update's own transaction, whether a main column holds it or EAV does.
// There is no NULL encoding: a key with a component absent holds no entry.

// unsetKeyCases are unsetkeys' keys, each with the component to unset and
// the values another row takes once the entry is released.
var unsetKeyCases = []struct {
	name   string
	attr   string
	key    int16
	retake map[string]any
}{
	{"bound single-attribute key", "slug", 1, map[string]any{"slug": "s1"}},
	{"EAV single-attribute key", "handle", 2, map[string]any{"handle": "h1"}},
	{"composite key, bound component", "tenant", 3, map[string]any{"tenant": "t1", "email": "e1"}},
	{"composite key, EAV component", "email", 3, map[string]any{"tenant": "t1", "email": "e1"}},
}

func unsetKeysDoc() map[string]any {
	return map[string]any{"note": "n", "slug": "s1", "handle": "h1", "tenant": "t1", "email": "e1"}
}

func unsetKeysOp(rowID uuid.UUID, updates map[string]any, ops ...forma.UpdateOp) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: unsetKeysSchemaName, RowID: rowID},
		Updates: updates, UpdateOps: ops}
}

// heldKeys answers the key IDs rowID holds an entry of.
func (e *txEnv) heldKeys(t *testing.T, rowID uuid.UUID) []int16 {
	t.Helper()
	var keys []int16
	for _, entry := range e.keyEntriesOfRow(t, rowID) {
		keys = append(keys, entry.keyID)
	}
	return keys
}

func TestUnsetReleasesTheUniqueKeyEntry(t *testing.T) {
	for _, surface := range unsetSurfaces {
		for _, tc := range unsetKeyCases {
			t.Run(surface.name+"/"+tc.name, func(t *testing.T) {
				env := setupUnsetKeysPG(t)
				a := env.mustCreate(t, unsetKeysSchemaName, unsetKeysDoc())
				require.ElementsMatch(t, []int16{1, 2, 3}, env.heldKeys(t, a))

				_, err := surface.update(env, unsetKeysOp(a, nil, forma.Unset(tc.attr)))
				require.NoError(t, err)
				assert.NotContains(t, env.heldKeys(t, a), tc.key, "the cleared component releases the entry")
				assert.Len(t, env.heldKeys(t, a), 2, "the other keys keep theirs")
				env.assertKeyTableMatchesRows(t)

				// Another entity can now take the value.
				retake := map[string]any{"note": "b"}
				for k, v := range tc.retake {
					retake[k] = v
				}
				b := env.mustCreate(t, unsetKeysSchemaName, retake)
				assert.Contains(t, env.heldKeys(t, b), tc.key)

				// Setting the value on a again conflicts with b, and writes
				// nothing.
				before := env.snapshot(t, unsetKeysSchemaName, a)
				_, err = surface.update(env, unsetKeysOp(a, map[string]any{tc.attr: unsetKeysDoc()[tc.attr]}))
				require.ErrorIs(t, err, forma.ErrUniqueConflict)
				assert.Equal(t, before, env.snapshot(t, unsetKeysSchemaName, a))
				env.assertKeyTableMatchesRows(t)
			})
		}
	}
}

// TestRolledBackUnsetKeepsTheReservation pins that a clear that does not
// commit, refused by its precondition or rolled back with its Tx, leaves the
// original entry held: another entity still conflicts on the value.
func TestRolledBackUnsetKeepsTheReservation(t *testing.T) {
	rollback := errors.New("roll back")
	for _, tc := range unsetKeyCases {
		t.Run(tc.name, func(t *testing.T) {
			env := setupUnsetKeysPG(t)
			a := env.mustCreate(t, unsetKeysSchemaName, unsetKeysDoc())
			before := env.snapshot(t, unsetKeysSchemaName, a)
			entries := env.keyEntries(t)

			op := unsetKeysOp(a, nil, forma.Unset(tc.attr))
			op.Preconditions = &forma.KvCondition{Attr: "note", Value: "equals:other"}
			_, err := env.manager.Update(env.ctx, op)
			require.ErrorIs(t, err, forma.ErrPreconditionFailed)

			err = env.inTx(func(ctx context.Context, tx forma.Tx) error {
				if _, err := tx.Update(ctx, unsetKeysOp(a, nil, forma.Unset(tc.attr))); err != nil {
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

// TestUnsetThenRetakeInOneTx pins that the release happens in the clearing
// transaction itself: a later write of the same Tx takes the value.
func TestUnsetThenRetakeInOneTx(t *testing.T) {
	env := setupUnsetKeysPG(t)
	a := env.mustCreate(t, unsetKeysSchemaName, unsetKeysDoc())
	var b uuid.UUID
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, unsetKeysOp(a, nil, forma.Unset("slug"), forma.Unset("email"))); err != nil {
			return err
		}
		record, err := tx.Create(ctx, txCreateOp(unsetKeysSchemaName, map[string]any{"note": "b", "slug": "s1", "tenant": "t1", "email": "e1"}))
		if err != nil {
			return err
		}
		b = record.RowID
		return nil
	}))
	assert.ElementsMatch(t, []int16{2}, env.heldKeys(t, a))
	assert.ElementsMatch(t, []int16{1, 3}, env.heldKeys(t, b))
	env.assertKeyTableMatchesRows(t)
}
