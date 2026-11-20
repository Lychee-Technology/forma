package publication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// pinnedThenLost registers schema 100, publishes one generation at epoch 1
// when published is set, drops it and registers it again, so its record
// reaches epoch 3 with no generation row of epoch 2 or 3: what a running
// reader or writer that loaded the re-registered schema pins. Then the
// record row is lost. It answers that pin.
func pinnedThenLost(t *testing.T, f *fixture, published bool) int64 {
	t.Helper()
	ctx := context.Background()
	cur := f.initialized(t, 100)
	if published {
		var err error
		cur, err = f.publish(ctx, cur, f.next(t, cur, KindInit), nil)
		require.NoError(t, err)
	}
	active := f.activate(t, f.reset(t, cur))
	require.Equal(t, int64(3), active.Epoch)
	_, err := f.db.Exec("DELETE FROM "+f.t.Record+" WHERE schema_id = $1", 100)
	require.NoError(t, err)
	return active.Epoch
}

// recoverAfterLoss runs InitializeAfterLoss for schema 100 in state, in a
// transaction of its own, and answers the record it wrote.
func recoverAfterLoss(t *testing.T, f *fixture, state State) (Record, error) {
	t.Helper()
	ctx := context.Background()
	var rec Record
	err := f.inTx(ctx, func(tx Tx) error {
		h, err := ReadHistory(ctx, tx, f.t, 100)
		if err != nil {
			return err
		}
		var wrote bool
		rec, wrote, err = InitializeAfterLoss(ctx, tx, f.t, 100, h, state)
		if err == nil {
			require.True(t, wrote)
		}
		return err
	})
	return rec, err
}

// A record lost after a drop and a re-registration, with generation history
// or without, is recovered above every epoch a record of its id carried: the
// generation history names epoch 1 at most, the lost record was at epoch 3,
// and a reader or writer still pinned to 3 would take a record at 2 or 3 for
// the schema it loaded. Initialize refuses the id rather than start it at
// epoch 1 again, and no later drop or re-registration reaches the pin.
func TestALostRecordIsRecoveredAboveEveryEpochItsIdCarried(t *testing.T) {
	for name, published := range map[string]bool{"with generation history": true, "without generation history": false} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			pin := pinnedThenLost(t, f, published)

			_, err := f.initialize(t, 100)
			require.ErrorIs(t, err, ErrRecordLost, "a record at epoch 1 would carry an epoch the id carried before")
			h, err := ReadHistory(context.Background(), DB(f.db), f.t, 100)
			require.NoError(t, err)
			require.Equal(t, pin, h.Epoch, "the id's lifecycle epoch outlives its record")
			require.True(t, h.Lost())
			if published {
				require.Equal(t, History{Epoch: 3, Generations: 1, MaxEpoch: 1, MaxGeneration: 1}, h)
			}

			state := Unpublished
			if published {
				state = AwaitingInit
			}
			rec, err := recoverAfterLoss(t, f, state)
			require.NoError(t, err)
			require.Equal(t, pin+1, rec.Epoch)
			read := f.read(t, 100)
			require.Equal(t, [3]any{rec.Epoch, rec.State, rec.Token}, [3]any{read.Epoch, read.State, read.Token})
			rec = read
			seen := []int64{rec.Epoch}
			for range 2 {
				rec = f.activate(t, f.reset(t, rec))
				seen = append(seen, rec.Epoch)
			}
			for _, epoch := range seen {
				require.Greater(t, epoch, pin, "a record of the id is at %d, which a stale process may pin", pin)
			}
		})
	}
}

// A record lost while it was DROPPED is recovered DROPPED above the id's
// lifecycle epoch, and Activate then starts the re-registration one epoch
// on.
func TestALostDroppedRecordIsActivatedAboveItsEpoch(t *testing.T) {
	f := newFixture(t)
	dropped := f.reset(t, f.initialized(t, 100))
	_, err := f.db.Exec("DELETE FROM "+f.t.Record+" WHERE schema_id = $1", 100)
	require.NoError(t, err)
	rec, err := recoverAfterLoss(t, f, Dropped)
	require.NoError(t, err)
	require.Equal(t, Dropped, rec.State)
	require.Equal(t, dropped.Epoch+1, rec.Epoch)
	require.Equal(t, dropped.Epoch+2, f.activate(t, rec).Epoch)
}

// A record whose lifecycle epoch is lost with it cannot be recovered: its
// generation history names epoch 1, yet a drop and a re-registration took
// the record to epoch 3 without a generation row, so no epoch above the
// history is known to be free of a stale pin. Once the operator, with every
// process stopped, writes the id's lifecycle epoch, recovery goes above it.
func TestRecoveryRefusesAnIdWhoseLifecycleEpochIsLost(t *testing.T) {
	f := newFixture(t)
	pinnedThenLost(t, f, true)
	_, err := f.db.Exec("DELETE FROM "+f.t.Epoch+" WHERE schema_id = $1", 100)
	require.NoError(t, err)

	_, err = recoverAfterLoss(t, f, AwaitingInit)
	require.ErrorIs(t, err, ErrLifecycleLost)
	_, err = Read(context.Background(), DB(f.db), f.t, 100)
	require.ErrorIs(t, err, ErrNoRecord, "a refused recovery writes nothing")

	_, err = f.db.Exec("INSERT INTO "+f.t.Epoch+" (schema_id, epoch, updated_at) VALUES ($1, $2, 0)", 100, 7)
	require.NoError(t, err)
	rec, err := recoverAfterLoss(t, f, AwaitingInit)
	require.NoError(t, err)
	require.Equal(t, int64(8), rec.Epoch)
	require.Equal(t, int64(1), rec.Generation, "the counter keeps the history's highest generation")
}

// A record that went back to an epoch its id carried before (restored from
// an older state) cannot move on to another one: Reset and Activate refuse
// it with ErrEpochReused, and roll back, so no record of the id is written at
// an epoch a stale process may pin.
func TestATransitionRefusesAnEpochItsIdCarried(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	active := f.activate(t, f.reset(t, f.initialized(t, 100)))
	_, err := f.db.Exec("UPDATE "+f.t.Record+" SET epoch = 1 WHERE schema_id = $1", 100)
	require.NoError(t, err)
	restored := active
	restored.Epoch = 1

	err = f.inTx(ctx, func(tx Tx) error {
		_, err := Reset(ctx, tx, f.t, restored)
		return err
	})
	require.ErrorIs(t, err, ErrEpochReused, "Reset would move the record to epoch 2, which the id carried")
	require.Equal(t, restored, f.read(t, 100), "the refused reset rolled back")

	_, err = f.db.Exec("UPDATE "+f.t.Record+" SET state = $2 WHERE schema_id = $1", 100, string(Dropped))
	require.NoError(t, err)
	restored.State = Dropped
	err = f.inTx(ctx, func(tx Tx) error {
		_, err := Activate(ctx, tx, f.t, restored, nil)
		return err
	})
	require.ErrorIs(t, err, ErrEpochReused, "Activate would move the record to epoch 2, which the id carried")
	require.Equal(t, restored, f.read(t, 100), "the refused activation rolled back")
}
