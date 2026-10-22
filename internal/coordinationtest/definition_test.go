package coordinationtest

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Workflow definitions: (name, version) and (name, activation_sequence)
// are unique keys (#651).

func TestDefinitions(t *testing.T) {
	save := func(e *env, def *Definition, active bool) (int64, error) {
		var seq int64
		err := e.inTx(func(uow UnitOfWork) error {
			var err error
			if active {
				seq, err = uow.Definitions().SaveActive(e.ctx, def)
				return err
			}
			return uow.Definitions().SaveDraft(e.ctx, def)
		})
		return seq, err
	}

	t.Run("a duplicate (name, version) is ErrUniqueConflict", func(t *testing.T) {
		e := newEnv(t)
		_, err := save(e, &Definition{Name: "approval", Version: 1, Body: "first"}, false)
		require.NoError(t, err)
		_, err = save(e, &Definition{Name: "approval", Version: 1, Body: "second"}, false)
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		_, err = save(e, &Definition{Name: "approval", Version: 1, Body: "third"}, true)
		require.ErrorIs(t, err, forma.ErrUniqueConflict, "an activation of a saved version too")
		var got *Definition
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			got, err = uow.Definitions().GetByVersion(e.ctx, "approval", 1)
			return err
		}))
		require.Equal(t, "first", got.Body)
		require.Equal(t, DefinitionDraft, got.Status)
		e.requireWritten(t, DefinitionSchema, nil, got.ID)
		e.requireOneHotEntryPerRow(t, DefinitionSchema)
	})

	t.Run("activations take the next sequence; a duplicate sequence is ErrUniqueConflict", func(t *testing.T) {
		e := newEnv(t)
		for version, want := range map[int64]int64{2: 1} {
			seq, err := save(e, &Definition{Name: "approval", Version: version}, true)
			require.NoError(t, err)
			require.Equal(t, want, seq)
		}
		seq, err := save(e, &Definition{Name: "approval", Version: 3}, true)
		require.NoError(t, err)
		require.Equal(t, int64(2), seq)

		activate := func(version int64) func(UnitOfWork) error {
			return func(uow UnitOfWork) error {
				_, err := uow.Definitions().SaveActive(e.ctx, &Definition{Name: "approval", Version: version})
				return err
			}
		}
		first := e.hold(t, activate(4))
		done := async(func() error { return e.inTx(activate(5)) })
		answered, err := e.settle(t, done)
		firstErr := first.commit()
		second := e.collect(t, done, answered, err)
		t.Run("contract: two activations reading one latest sequence do not both commit", func(t *testing.T) {
			require.True(t, (firstErr == nil) != (second.err == nil), "first %v, second %v", firstErr, second.err)
			for _, err := range []error{firstErr, second.err} {
				if err != nil {
					requireLoser(t, err, forma.ErrUniqueConflict)
				}
			}
			require.Equal(t, 3, e.liveRows(t, DefinitionSchema))
			e.requireWritten(t, DefinitionSchema, nil, e.liveIDs(t, DefinitionSchema, "name", "approval")...)
			e.requireOneHotEntryPerRow(t, DefinitionSchema)
		})
		t.Run("mechanism: the second waits on the key, then answers ErrUniqueConflict", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.False(t, second.early)
			require.NoError(t, firstErr)
			requireMechanismLoser(t, second.err, forma.ErrUniqueConflict)
		})
	})
}
