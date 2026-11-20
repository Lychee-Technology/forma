package publication

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestReadRefusesIdentityNoTransitionWrites pins that Read checks a record's
// identity, not only the fields its state allows: a row at an epoch below 1,
// with a negative generation counter, parent or horizon, a nil token, or a
// published generation that does not follow its parent is one no protocol
// transition writes. Read refuses it as ErrRecordInvalid, carrying the row as
// read, so a reader fails closed on it and a startup pin names its epoch.
func TestReadRefusesIdentityNoTransitionWrites(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cases := []struct {
		name string
		set  string
		args []any
	}{
		{"epoch zero, generation -1", "epoch = 0, generation = -1", nil},
		{"epoch zero", "epoch = 0", nil},
		{"negative epoch", "epoch = -3", nil},
		{"negative generation counter", "generation = -1", nil},
		{"negative parent", "parent_generation = -1", nil},
		{"nil token", "cas_token = $2", []any{uuid.Nil}},
		{"negative horizon", "gc_horizon = -5", nil},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id := int16(300 + i)
			f.initialized(t, id)
			args := append([]any{id}, c.args...)
			_, err := f.db.ExecContext(ctx, "UPDATE "+f.t.Record+" SET "+c.set+" WHERE schema_id = $1", args...)
			require.NoError(t, err)

			_, err = Read(ctx, DB(f.db), f.t, id)
			require.ErrorIs(t, err, ErrRecordInvalid)
			var invalid *InvalidRecordError
			require.ErrorAs(t, err, &invalid)
			require.Equal(t, id, invalid.Record.SchemaID)
		})
	}

	t.Run("published generation not after its parent", func(t *testing.T) {
		rec := f.initialized(t, 320)
		_, err := f.publish(ctx, rec, f.next(t, rec, KindInit), nil)
		require.NoError(t, err)
		_, err = f.db.ExecContext(ctx, "UPDATE "+f.t.Record+" SET parent_generation = generation WHERE schema_id = $1", int16(320))
		require.NoError(t, err)
		_, err = Read(ctx, DB(f.db), f.t, 320)
		require.ErrorIs(t, err, ErrRecordInvalid)
	})

	t.Run("an untouched record still reads", func(t *testing.T) {
		rec := f.initialized(t, 321)
		require.Equal(t, int64(1), rec.Epoch)
		require.Zero(t, rec.Generation)
	})
}
