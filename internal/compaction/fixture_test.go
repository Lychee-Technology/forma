package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/parquetcheck"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/testdb"
)

const fixtureSchema int16 = 100

// v2Fixture is one test's publication tables on the integration
// PostgreSQL, a local write-once store and an in-memory DuckDB that writes
// and merges parquet objects in it.
type v2Fixture struct {
	db     *sql.DB
	duck   *sql.DB
	tables publication.Tables
	main   string
	store  *manifestv2.FSStore
	layout manifestv2.Layout
}

func newV2Fixture(t *testing.T) *v2Fixture {
	t.Helper()
	testdb.Connect(t, context.Background()) // skips when Postgres is absent locally
	db, err := sql.Open("pgx", testdb.ResolveDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	duck, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })
	main := fmt.Sprintf("compaction_test_%d", time.Now().UnixNano())
	f := &v2Fixture{db: db, duck: duck, tables: publication.TablesFor(main), main: main,
		store: &manifestv2.FSStore{Dir: t.TempDir()}, layout: manifestv2.Layout{Prefix: "data"}}
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, main) {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{f.tables.Record, f.tables.Generation, f.tables.Epoch} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Initialize(context.Background(), publication.SQLTx(tx), f.tables, fixtureSchema)
		return err
	})
	return f
}

func (f *v2Fixture) inTx(t *testing.T, fn func(*sql.Tx) error) {
	t.Helper()
	tx, err := f.db.Begin()
	require.NoError(t, err)
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func (f *v2Fixture) record(t *testing.T) publication.Record {
	t.Helper()
	s, err := publication.Read(context.Background(), publication.DB(f.db), f.tables, fixtureSchema)
	require.NoError(t, err)
	return s.Record
}

// path is where the store holds key: the fixture's keys need no escaping.
func (f *v2Fixture) path(key string) string {
	return filepath.Join(f.store.Dir, filepath.FromSlash(key))
}

// object writes rows as a parquet object of tier under a fresh key, once,
// and answers its manifest entry.
func (f *v2Fixture) object(t *testing.T, tier manifestv2.Tier, rows []mergeFixtureRow) manifestv2.File {
	t.Helper()
	scratch := filepath.Join(t.TempDir(), "object.parquet")
	writeParquetFixture(t, f.duck, scratch, rows)
	raw, err := os.ReadFile(scratch)
	require.NoError(t, err)
	key, err := f.layout.NewParquetKey(fixtureSchema, f.record(t).Epoch, tier)
	require.NoError(t, err)
	require.NoError(t, f.store.Put(context.Background(), key, bytes.NewReader(raw)))
	cols, err := parquetcheck.DescribeColumns(context.Background(), f.duck, scratch)
	require.NoError(t, err)
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.rowID
	}
	lo, hi := ids[0], ids[0]
	for _, id := range ids {
		lo, hi = min(lo, id), max(hi, id)
	}
	return manifestv2.File{Tier: tier, Key: key, SizeBytes: int64(len(raw)), SHA256: manifestv2.Digest(raw),
		RowCount: int64(len(rows)), RowIDMin: uuid.MustParse(lo), RowIDMax: uuid.MustParse(hi), Columns: cols}
}

// publish commits the generation of kind after the head that lists files
// (an init's base files, or a flush's new deltas), as cdc-init and
// cdc-flush do.
func (f *v2Fixture) publish(t *testing.T, kind publication.Kind, files ...manifestv2.File) publication.Record {
	t.Helper()
	ctx := context.Background()
	head := f.record(t)
	lineage := head.Lineage
	if kind == publication.KindInit || head.State != publication.Published {
		lineage = uuid.New()
	}
	h, err := manifestv2.Stage(head, kind, lineage, uuid.New())
	require.NoError(t, err)
	var m manifestv2.Manifest
	if kind == publication.KindInit {
		m, err = manifestv2.NewInit(h, files)
	} else {
		var parent *manifestv2.Manifest
		if head.State == publication.Published {
			parent = f.manifest(t, head)
		}
		m, err = manifestv2.NewFlush(parent, h, files)
	}
	require.NoError(t, err)
	key, digest, err := manifestv2.PutManifest(ctx, f.store, f.layout, m)
	require.NoError(t, err)
	var stagedAt int64
	require.NoError(t, f.db.QueryRow("SELECT floor(extract(epoch from now()) * 1000)::bigint").Scan(&stagedAt))
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Publish(ctx, publication.SQLTx(tx), f.tables, head, publication.Next{
			Kind: kind, Token: h.Token, ManifestKey: key, ManifestSHA256: digest,
			Lineage: lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
		})
		return err
	})
	return f.record(t)
}

func (f *v2Fixture) manifest(t *testing.T, rec publication.Record) *manifestv2.Manifest {
	t.Helper()
	ref, err := manifestv2.RecordRef(rec)
	require.NoError(t, err)
	m, err := manifestv2.Fetch(context.Background(), f.store, ref)
	require.NoError(t, err)
	return m
}

// compactor answers a compactor over the fixture that merges with DuckDB
// and verifies its sources against the store, adjusted by mods.
func (f *v2Fixture) compactor(t *testing.T, cfg cdc.CompactionConfig, mods ...func(*Compactor)) *Compactor {
	t.Helper()
	cfg.SchemaID = fixtureSchema
	w, err := NewWriter(WriterConfig{DB: f.db, Engine: forma.EnginePostgreSQL, EntityMain: f.main,
		Layout: f.layout, Store: f.store, Compaction: cfg})
	require.NoError(t, err)
	c := &Compactor{
		Config: cfg, Writer: w, Store: f.store, Merger: &DuckMerger{DB: f.duck},
		SourceURI:    f.path,
		ObjectReader: &objectReader{read: func(key string) ([]byte, error) { return f.store.Get(context.Background(), key) }},
	}
	for _, mod := range mods {
		mod(c)
	}
	return c
}

// rowsOf reads the object at key: each row's id, version and deleted_at.
func (f *v2Fixture) rowsOf(t *testing.T, key string) map[string][2]int64 {
	t.Helper()
	rows, err := f.duck.Query(fmt.Sprintf("SELECT CAST(row_id AS VARCHAR), changed_at, deleted_at FROM read_parquet('%s')",
		sqlutil.EscapeLiteral(f.path(key))))
	require.NoError(t, err)
	defer rows.Close()
	out := map[string][2]int64{}
	for rows.Next() {
		var id string
		var v, d int64
		require.NoError(t, rows.Scan(&id, &v, &d))
		out[id] = [2]int64{v, d}
	}
	require.NoError(t, rows.Err())
	return out
}

// kinds answers the kinds of the schema's generations, oldest first.
func (f *v2Fixture) kinds(t *testing.T) string {
	t.Helper()
	rows, err := f.db.Query(fmt.Sprintf("SELECT kind FROM %s WHERE schema_id = $1 ORDER BY generation", f.tables.Generation), fixtureSchema)
	require.NoError(t, err)
	defer rows.Close()
	var kinds []string
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		kinds = append(kinds, k)
	}
	require.NoError(t, rows.Err())
	return strings.Join(kinds, ",")
}
