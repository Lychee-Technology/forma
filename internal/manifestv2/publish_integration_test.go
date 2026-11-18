package manifestv2

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // the pgx database/sql driver
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// TestPublishCommitsWhatPutManifestStaged runs the writer's and the reader's
// halves of a generation end to end on the integration PostgreSQL: a staged
// manifest's key and digest are what Publish accepts, and what the record
// and the history row then name fetches through the cache, for an epoch's
// first flush and for a flush over it.
func TestPublishCommitsWhatPutManifestStaged(t *testing.T) {
	ctx := context.Background()
	testdb.Connect(t, ctx) // skips when Postgres is absent locally
	db, err := sql.Open("pgx", testdb.ResolveDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	entityMain := fmt.Sprintf("mv2_test_%d", time.Now().UnixNano())
	tables := publication.TablesFor(entityMain)
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, entityMain) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{tables.Record, tables.Generation} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	inTx(t, db, func(tx publication.Tx) error {
		_, err := publication.Initialize(ctx, tx, tables, 100)
		return err
	})

	st := &FSStore{Dir: t.TempDir()}
	cache, err := NewCache(st, 8)
	require.NoError(t, err)
	lineage := uuid.New()

	first := publishFlush(t, db, tables, st, nil, lineage, testFile(t, TierDelta, 1))
	require.Equal(t, int64(1), first.Generation)
	second := publishFlush(t, db, tables, st, first, lineage, testFile(t, TierDelta, 2))
	require.Equal(t, int64(2), second.Generation)
	require.Equal(t, append(first.Keys(), testFile(t, TierDelta, 2).Key), second.Keys())

	snap, err := publication.Read(ctx, publication.DB(db), tables, 100)
	require.NoError(t, err)
	ref, err := RecordRef(snap.Record)
	require.NoError(t, err)
	got, err := cache.Get(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, second, got)

	retained, err := publication.Retained(ctx, publication.DB(db), tables, 100, time.Hour)
	require.NoError(t, err)
	require.Len(t, retained, 2)
	for _, g := range retained {
		m, err := cache.Get(ctx, GenerationRef(100, g))
		require.NoError(t, err, "generation %d", g.Generation)
		require.Equal(t, g.Generation, m.Generation)
		raw, err := st.Get(ctx, g.ManifestKey)
		require.NoError(t, err)
		keys, err := publication.ManifestFiles(raw, 100, g)
		require.NoError(t, err, "the collector verifies the same bytes")
		require.Equal(t, m.Keys(), keys)
	}
}

// publishFlush stages a flush of file (of epoch 1, as testFile builds it)
// over parent, as of the record, and commits it with Publish, answering the
// manifest it staged.
func publishFlush(t *testing.T, db *sql.DB, tables publication.Tables, st Store, parent *Manifest, lineage uuid.UUID, file File) *Manifest {
	t.Helper()
	ctx := context.Background()
	snap, err := publication.Read(ctx, publication.DB(db), tables, 100)
	require.NoError(t, err)
	var stagedAt int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&stagedAt))
	h, err := Stage(snap.Record, publication.KindFlush, lineage, uuid.New())
	require.NoError(t, err)
	m, err := NewFlush(parent, h, []File{file})
	require.NoError(t, err)
	key, digest, err := PutManifest(ctx, st, testLayout, m)
	require.NoError(t, err)
	inTx(t, db, func(tx publication.Tx) error {
		_, err := publication.Publish(ctx, tx, tables, snap.Record, publication.Next{
			Kind: publication.KindFlush, Token: h.Token, ManifestKey: key, ManifestSHA256: digest,
			Lineage: lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
		})
		return err
	})
	return &m
}

func inTx(t *testing.T, db *sql.DB, fn func(publication.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	if err := fn(publication.SQLTx(tx)); err != nil {
		_ = tx.Rollback()
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}
