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

// Real-PostgreSQL tests of a unique key with a component bound to
// ltbase_created_at (#651). Forma stamps that column itself when it creates
// the row, so every create surface must compute the row's entry from the
// stamp the row is stored with.

// ukCreatedBatch is a BatchCreate of one ukcreated row per owner.
func ukCreatedBatch(atomic bool, owners ...string) *forma.BatchOperation {
	ops := make([]forma.EntityOperation, len(owners))
	for i, owner := range owners {
		ops[i] = *ukCreateOp(ukCreated, uuid.Nil, map[string]any{"owner": owner})
	}
	return &forma.BatchOperation{Atomic: atomic, Operations: ops}
}

func TestUniqueKeyOverTheCreationStampHoldsTheStoredStamp(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	// Every create below reads this one clock value, so two rows of one
	// owner collide whichever surfaces created them.
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.manager.(*entityManager).repository.(*DBPersistentRecordRepository).withClock(func() time.Time { return stamp })
	create := func(owner string) *forma.EntityOperation {
		return ukCreateOp(ukCreated, uuid.Nil, map[string]any{"owner": owner})
	}

	// An atomic batch stamps all its rows with one clock read, so two rows of
	// one owner collide within it, and the batch writes nothing.
	_, err := env.manager.BatchCreate(env.ctx, ukCreatedBatch(true, "alice", "alice"))
	requireKeyConflict(t, err, ukCreated, "owner_created", "alice")
	assert.Zero(t, env.mainRowCount(t, ukCreated), "a refused atomic batch writes nothing")
	assert.Empty(t, env.keyEntries(t))

	_, err = env.manager.BatchCreate(env.ctx, ukCreatedBatch(true, "alice", "bob"))
	require.NoError(t, err)
	env.assertKeyTableMatchesRows(t)

	// Every other create surface meets the atomic batch's entries...
	_, err = env.manager.Create(env.ctx, create("alice"))
	requireKeyConflict(t, err, ukCreated, "owner_created", "alice")
	result, err := env.manager.BatchCreate(env.ctx, ukCreatedBatch(false, "bob"))
	require.NoError(t, err)
	require.Len(t, result.Failed, 1)
	assert.Equal(t, "UNIQUE_CONFLICT", result.Failed[0].Code)
	var txErr error
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, txErr = tx.Create(ctx, create("alice"))
		return nil
	}))
	requireKeyConflict(t, txErr, ukCreated, "owner_created", "alice")

	// ...and an atomic batch meets theirs.
	carol := env.mustCreate(t, ukCreated, map[string]any{"owner": "carol"})
	_, err = env.manager.BatchCreate(env.ctx, ukCreatedBatch(true, "dave", "carol"))
	requireKeyConflict(t, err, ukCreated, "owner_created", "carol")

	// An update keeps the row's stored stamp, so a row moved onto another
	// owner meets that owner's entry, and a free move takes an entry that
	// holds the stored stamp.
	_, err = env.manager.Update(env.ctx, txUpdateOp(ukCreated, carol, map[string]any{"owner": "alice"}))
	requireKeyConflict(t, err, ukCreated, "owner_created", "alice")
	move := *txUpdateOp(ukCreated, carol, map[string]any{"owner": "erin"})
	_, err = env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{move}})
	require.NoError(t, err)

	assert.Equal(t, 3, env.mainRowCount(t, ukCreated))
	env.assertKeyTableMatchesRows(t)
}

// ukCreatedUpsert is a do_nothing Upsert of one ukcreated row on its
// owner_created key.
func ukCreatedUpsert(owner string) *forma.UpsertRequest {
	return &forma.UpsertRequest{
		Create: *ukCreateOp(ukCreated, uuid.Nil, map[string]any{"owner": owner}),
		Target: forma.ConflictTarget{Key: "owner_created"},
		Action: forma.UpsertDoNothing,
	}
}

// An upsert's target over the creation stamp is the stamp its create would
// store, so a held value resolves to its holder rather than being looked up
// under one stamp and reserved under another.
func TestUpsertOnACreationStampKeyResolvesItsHolder(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	env.txr.repo.withClock(func() time.Time { return stamp })
	holder := env.mustCreate(t, ukCreated, map[string]any{"owner": "alice"})

	res, err := env.upsertInTx(t, ukCreatedUpsert("alice"))
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertExisting, res.Outcome)
	assert.Equal(t, holder, res.Record.RowID)

	// A free value is created, and the Tx's own entry is the holder a second
	// upsert of it resolves to.
	var created, again *forma.UpsertResult
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if created, err = tx.Upsert(ctx, ukCreatedUpsert("bob")); err != nil {
			return err
		}
		again, err = tx.Upsert(ctx, ukCreatedUpsert("bob"))
		return err
	}))
	assert.Equal(t, forma.UpsertCreated, created.Outcome)
	assert.Equal(t, forma.UpsertExisting, again.Outcome)
	assert.Equal(t, created.Record.RowID, again.Record.RowID)
	env.requireStored(t, created.Record)
	assert.Equal(t, 2, env.mainRowCount(t, ukCreated))
	env.assertKeyTableMatchesRows(t)
}

// The clock moving between the lookup and the create does not move the
// target: the create stores the stamp the upsert looked up, so it does not
// meet the holder of the clock's newer value.
func TestUpsertStoresTheCreationStampItLookedUp(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	looked := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := looked.Add(time.Second)
	now := later
	env.txr.repo.withClock(func() time.Time { return now })
	env.mustCreate(t, ukCreated, map[string]any{"owner": "alice"})

	now = looked
	onTargetLookup(t, env, func(int, uuid.UUID, bool) { now = later })
	res, err := env.upsertInTx(t, ukCreatedUpsert("alice"))
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertCreated, res.Outcome)
	assert.Equal(t, looked, res.Record.Attributes["created"])
	env.requireStored(t, res.Record)
	env.assertKeyTableMatchesRows(t)
}
