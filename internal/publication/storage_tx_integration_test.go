package publication

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestBindStorageTxBindsOnlyWithItsTransaction pins R2-F1 of #911: a
// writer binds an unbound deployment inside its publication transaction, so
// the binding is durable exactly when that transaction commits. A rolled-back
// transaction binds nothing; the first committed one binds; the same target in
// another spelling passes; another target fails closed in the transaction and
// leaves the binding as it was.
func TestBindStorageTxBindsOnlyWithItsTransaction(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	bind := func(target StorageTarget, commit bool) (StorageBinding, error) {
		tx, err := f.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		b, err := BindStorageTx(ctx, SQLTx(tx), f.t, target)
		if err != nil || !commit {
			return b, err
		}
		return b, tx.Commit()
	}
	unbound := func(msg string) {
		t.Helper()
		_, ok, err := ReadStorage(ctx, DB(f.db), f.t)
		require.NoError(t, err)
		require.False(t, ok, msg)
	}

	_, err := bind(StorageTarget{Endpoint: "https://lake.s3.example/tenant//", Bucket: "lake", Prefix: "data"}, true)
	require.ErrorIs(t, err, ErrStorageTargetInvalid)
	unbound("an invalid target binds nothing")

	misconfigured := StorageTarget{Endpoint: "https://lake.s3.example/tenant", Bucket: "pond", Prefix: "data"}
	b, err := bind(misconfigured, false)
	require.NoError(t, err)
	require.True(t, b.Target.Same(misconfigured), "the transaction reads its own binding")
	unbound("a publication transaction that rolls back binds nothing")

	first := StorageTarget{Endpoint: "https://lake.s3.example/tenant", Bucket: "lake", Prefix: "data"}
	b, err = bind(first, true)
	require.NoError(t, err)
	require.True(t, b.Target.Same(first))
	require.NotZero(t, b.BoundAt)
	kept, ok, err := ReadStorage(ctx, DB(f.db), f.t)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, b, kept)

	again, err := bind(StorageTarget{Endpoint: "HTTPS://Lake.S3.Example/tenant", Bucket: "lake", Prefix: "data/"}, true)
	require.NoError(t, err, "the same target, spelled otherwise")
	require.Equal(t, b, again, "a check rewrites nothing")

	for name, other := range map[string]StorageTarget{
		"another endpoint":   {Endpoint: "https://s3.example/tenant/lake", Bucket: "lake", Prefix: "data"},
		"another bucket":     misconfigured,
		"another prefix":     {Endpoint: "https://lake.s3.example/tenant", Bucket: "lake", Prefix: "data2"},
		"the default origin": {Endpoint: "", Bucket: "lake", Prefix: "data"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := bind(other, true)
			require.ErrorIs(t, err, ErrStorageBoundElsewhere)
			require.Equal(t, b, got, "the refusal answers the binding it kept")
			kept, ok, err := ReadStorage(ctx, DB(f.db), f.t)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, b, kept)
		})
	}
}

// TestConcurrentFirstBindersCommitOneBinding pins R2-F1 of #911 for two
// first publishers of an unbound deployment at different targets: while the
// first one's transaction is open, the second cannot bind past it. If the
// first rolls back, the second binds its own target; if the first commits,
// the second fails closed (ErrStorageBoundElsewhere, or a serialization
// conflict that a writer sends again and that then fails closed), and the
// first's binding is kept.
func TestConcurrentFirstBindersCommitOneBinding(t *testing.T) {
	a := StorageTarget{Endpoint: "https://lake.s3.example", Bucket: "lake", Prefix: "data"}
	b := StorageTarget{Endpoint: "https://lake.s3.example", Bucket: "pond", Prefix: "data"}
	for _, firstCommits := range []bool{false, true} {
		name := map[bool]string{false: "the first rolls back", true: "the first commits"}[firstCommits]
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if f.cluster {
				t.Skip("nothing waits on a row lock on an Aurora DSQL cluster")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			first, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = first.Rollback() }()
			_, err = BindStorageTx(ctx, SQLTx(first), f.t, a)
			require.NoError(t, err)

			second := make(chan error, 1)
			go func() { second <- bindAndCommit(ctx, f.db, f.t, b) }()
			// PostgreSQL holds the second's insert on the first's row.
			select {
			case err := <-second:
				t.Fatalf("the second binder finished while the first was open: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			if firstCommits {
				require.NoError(t, first.Commit())
			} else {
				require.NoError(t, first.Rollback())
			}
			err = <-second
			want := b
			if firstCommits {
				want = a
				require.Error(t, err)
				if !serialization(err) {
					require.ErrorIs(t, err, ErrStorageBoundElsewhere)
				}
				require.ErrorIs(t, bindAndCommit(ctx, f.db, f.t, b), ErrStorageBoundElsewhere, "sent again, it fails closed")
			} else {
				require.NoError(t, err, "the second binds what the first left unbound")
			}
			kept, ok, err := ReadStorage(ctx, DB(f.db), f.t)
			require.NoError(t, err)
			require.True(t, ok)
			require.True(t, kept.Target.Same(want), "bound to %s, want %s", kept.Target, want)
		})
	}
}

func bindAndCommit(ctx context.Context, db *sql.DB, t Tables, target StorageTarget) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := BindStorageTx(ctx, SQLTx(tx), t, target); err != nil {
		return err
	}
	return tx.Commit()
}

func serialization(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}
