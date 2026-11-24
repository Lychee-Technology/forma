package publication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

var (
	writersTarget = StorageTarget{Endpoint: "https://lake.s3.us-east-1.amazonaws.com", Bucket: "lake", Prefix: "data"}
	// writersSpelling is writersTarget in another spelling Same accepts.
	writersSpelling = StorageTarget{Endpoint: "HTTPS://Lake.S3.US-East-1.AmazonAWS.com/", Bucket: "lake", Prefix: "data/"}
	otherTarget     = StorageTarget{Endpoint: "https://copy.s3.us-east-1.amazonaws.com", Bucket: "copy", Prefix: "data"}
)

func (f *fixture) binding(t *testing.T) (StorageBinding, bool) {
	t.Helper()
	b, ok, err := ReadStorage(context.Background(), DB(f.db), f.t)
	require.NoError(t, err)
	return b, ok
}

// TestCheckStorageBindsNothing: the writers' check before staging passes an
// unbound deployment without binding it, passes the bound target in any
// spelling Same accepts, and refuses another target and a missing table.
func TestCheckStorageBindsNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	engine := forma.EnginePostgreSQL
	require.NoError(t, CheckStorage(ctx, DB(f.db), f.t, writersTarget, engine))
	_, ok := f.binding(t)
	require.False(t, ok, "the check binds nothing")

	_, err := BindStorage(ctx, DB(f.db), f.t, writersTarget)
	require.NoError(t, err)
	require.NoError(t, CheckStorage(ctx, DB(f.db), f.t, writersSpelling, engine))
	err = CheckStorage(ctx, DB(f.db), f.t, otherTarget, engine)
	require.ErrorIs(t, err, ErrStorageBoundElsewhere)
	require.ErrorContains(t, err, otherTarget.String())
	require.ErrorContains(t, err, writersTarget.String())

	missing := TablesFor(fmt.Sprintf("no_such_%d", time.Now().UnixNano()))
	err = CheckStorage(ctx, DB(f.db), missing, writersTarget, forma.EnginePostgreSQL)
	require.ErrorIs(t, err, ErrStorageTableMissing)
	require.ErrorContains(t, err, "rerun init-db")
	if !f.cluster && engine == forma.EnginePostgreSQL {
		err = CheckStorage(ctx, DB(f.db), missing, writersTarget, forma.EngineAuroraDSQL)
		require.ErrorIs(t, err, ErrStorageTableMissing)
		require.ErrorContains(t, err, "--bind-storage", "DSQL's remedy is not init-db")
	}
}

// TestBindStorageTxBindsOnlyWithItsTransaction: the in-transaction bind of a
// first publication is durable exactly when its transaction commits.
func TestBindStorageTxBindsOnlyWithItsTransaction(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	rolledBack := f.inTx(ctx, func(tx Tx) error {
		require.NoError(t, BindStorageTx(ctx, tx, f.t, otherTarget))
		return fmt.Errorf("the publication fails after its bind")
	})
	require.Error(t, rolledBack)
	_, ok := f.binding(t)
	require.False(t, ok, "a rolled-back publication binds nothing")

	require.NoError(t, f.inTx(ctx, func(tx Tx) error { return BindStorageTx(ctx, tx, f.t, writersTarget) }))
	b, ok := f.binding(t)
	require.True(t, ok)
	require.Equal(t, writersTarget, b.Target)

	require.NoError(t, f.inTx(ctx, func(tx Tx) error { return BindStorageTx(ctx, tx, f.t, writersSpelling) }))
	err := f.inTx(ctx, func(tx Tx) error { return BindStorageTx(ctx, tx, f.t, otherTarget) })
	require.ErrorIs(t, err, ErrStorageBoundElsewhere)
	b, _ = f.binding(t)
	require.Equal(t, writersTarget, b.Target, "a mismatch keeps the binding")
}

// TestConcurrentFirstBindersCommitOneBinding: two first publishers at
// different targets serialize on the binding's row. The second waits for
// the first's transaction, then fails closed once the first commits, or
// binds its own target once the first rolls back.
func TestConcurrentFirstBindersCommitOneBinding(t *testing.T) {
	for _, firstCommits := range []bool{true, false} {
		t.Run(fmt.Sprintf("first commits %v", firstCommits), func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			if f.cluster {
				t.Skip("nothing waits on a row lock on an Aurora DSQL cluster")
			}
			first, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = first.Rollback() }()
			require.NoError(t, BindStorageTx(ctx, SQLTx(first), f.t, writersTarget))

			second := make(chan error, 1)
			go func() {
				second <- f.inTx(ctx, func(tx Tx) error { return BindStorageTx(ctx, tx, f.t, otherTarget) })
			}()
			select {
			case err := <-second:
				t.Fatalf("the second binder did not wait for the first: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
			want := otherTarget
			if firstCommits {
				require.NoError(t, first.Commit())
				want = writersTarget
			} else {
				require.NoError(t, first.Rollback())
			}
			err = <-second
			if firstCommits {
				require.ErrorIs(t, err, ErrStorageBoundElsewhere)
			} else {
				require.NoError(t, err)
			}
			b, ok := f.binding(t)
			require.True(t, ok)
			require.Equal(t, want, b.Target, "exactly one target is bound")
		})
	}
}
