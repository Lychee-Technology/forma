package reconcile

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// memObject is one object of a memStore.
type memObject struct {
	data []byte
	// lastModified is epoch milliseconds on the database clock, which the
	// tests also stand S3's clock for.
	lastModified int64
}

// memStore is an ObjectStore and a write-once manifestv2.Store in memory,
// with the faults the tests inject.
type memStore struct {
	mu      sync.Mutex
	objects map[string]memObject
	now     func() int64
	// failDelete, when set, fails the delete of a key it answers an error
	// for.
	failDelete func(key string) error
	// getErr fails a Get of the key with the error.
	getErr map[string]error
	// beforeGet, when set, runs before each Get, outside the store's lock.
	beforeGet func(key string)
	deletes   []string
}

func newMemStore(now func() int64) *memStore {
	return &memStore{objects: map[string]memObject{}, now: now, getErr: map[string]error{}}
}

func (m *memStore) Put(_ context.Context, key string, body io.ReadSeeker) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read the body of %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key]; ok {
		return fmt.Errorf("put %s: %w", key, manifestv2.ErrKeyExists)
	}
	m.objects[key] = memObject{data: data, lastModified: m.now()}
	return nil
}

// set writes or overwrites key, as an operator or a fault would.
func (m *memStore) set(key string, data []byte, lastModified int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = memObject{data: data, lastModified: lastModified}
}

// remove deletes key behind the collector's back, as a loss would.
func (m *memStore) remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
}

func (m *memStore) List(_ context.Context, prefix string) ([]publication.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []publication.Object
	for key, o := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, publication.Object{Key: key, LastModified: o.lastModified})
		}
	}
	slices.SortFunc(out, func(a, b publication.Object) int { return strings.Compare(a.Key, b.Key) })
	return out, nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	if m.beforeGet != nil {
		m.beforeGet(key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.getErr[key]; err != nil {
		return nil, err
	}
	o, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("get %s: %w", key, manifestv2.ErrObjectNotFound)
	}
	return bytes.Clone(o.data), nil
}

func (m *memStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok, nil
}

func (m *memStore) Hash(_ context.Context, key string) (string, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return "", 0, fmt.Errorf("hash %s: %w", key, manifestv2.ErrObjectNotFound)
	}
	return manifestv2.Digest(o.data), int64(len(o.data)), nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failDelete != nil {
		if err := m.failDelete(key); err != nil {
			return err
		}
	}
	delete(m.objects, key)
	m.deletes = append(m.deletes, key)
	return nil
}

// keys answers every key the store holds, sorted.
func (m *memStore) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.objects))
}

// fixture is one test's publication tables and bucket.
type fixture struct {
	db     *sql.DB
	tables publication.Tables
	layout manifestv2.Layout
	store  *memStore
}

const testSchema int16 = 7

func newFixture(t *testing.T) *fixture {
	t.Helper()
	testdb.Connect(t, context.Background()) // skips when Postgres is absent locally
	db, err := sql.Open("pgx", testdb.ResolveDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	entityMain := fmt.Sprintf("gc_test_%d", time.Now().UnixNano())
	f := &fixture{db: db, tables: publication.TablesFor(entityMain), layout: manifestv2.Layout{Prefix: "data"}}
	f.store = newMemStore(func() int64 { return f.serverMillis(t) })
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, entityMain) {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{f.tables.Record, f.tables.Generation, f.tables.Epoch} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	return f
}

func (f *fixture) serverMillis(t *testing.T) int64 {
	t.Helper()
	var ms int64
	require.NoError(t, f.db.QueryRow("SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms))
	return ms
}

// inTx runs fn in a transaction it commits when fn succeeds.
func (f *fixture) inTx(t *testing.T, fn func(publication.Tx) error) error {
	t.Helper()
	tx, err := f.db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	if err := fn(publication.SQLTx(tx)); err != nil {
		return err
	}
	return tx.Commit()
}

func (f *fixture) record(t *testing.T) publication.Record {
	t.Helper()
	s, err := publication.Read(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	return s.Record
}

// initialized writes the schema's UNPUBLISHED record.
func (f *fixture) initialized(t *testing.T) publication.Record {
	t.Helper()
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Initialize(context.Background(), tx, f.tables, testSchema)
		return err
	}))
	return f.record(t)
}

// file stages one parquet object of the tier in cur's epoch and answers the
// manifest entry that lists it.
func (f *fixture) file(t *testing.T, epoch int64, tier manifestv2.Tier) manifestv2.File {
	t.Helper()
	key, err := f.layout.NewParquetKey(testSchema, epoch, tier)
	require.NoError(t, err)
	data := make([]byte, 64)
	_, _ = rand.Read(data)
	require.NoError(t, f.store.Put(context.Background(), key, bytes.NewReader(data)))
	row := uuid.Must(uuid.NewV7())
	return manifestv2.File{
		Tier: tier, Key: key, SizeBytes: int64(len(data)), SHA256: manifestv2.Digest(data), RowCount: 1,
		RowIDMin: row, RowIDMax: row, Columns: map[string]string{"ltbase_row_id": "UUID"},
	}
}

// staged is a generation staged in the store and not committed yet.
type staged struct {
	cur      publication.Record
	manifest manifestv2.Manifest
	next     publication.Next
}

// stage builds and stores the manifest of a generation of kind after cur.
// build answers its files from the header.
func (f *fixture) stage(t *testing.T, cur publication.Record, kind publication.Kind, build func(manifestv2.Header) (manifestv2.Manifest, error)) staged {
	t.Helper()
	lineage := cur.Lineage
	if kind == publication.KindInit || cur.State != publication.Published {
		lineage = uuid.New()
	}
	stagedAt := f.serverMillis(t)
	h, err := manifestv2.Stage(cur, kind, lineage, uuid.New())
	require.NoError(t, err)
	m, err := build(h)
	require.NoError(t, err)
	key, digest, err := manifestv2.PutManifest(context.Background(), f.store, f.layout, m)
	require.NoError(t, err)
	return staged{cur: cur, manifest: m, next: publication.Next{
		Kind: kind, Token: h.Token, ManifestKey: key, ManifestSHA256: digest, Lineage: lineage,
		StagedAt: stagedAt, StagingBudget: time.Hour,
	}}
}

// commit publishes s in a transaction of its own.
func (f *fixture) commit(t *testing.T, s staged) (publication.Record, error) {
	t.Helper()
	var out publication.Record
	err := f.inTx(t, func(tx publication.Tx) error {
		var err error
		out, err = publication.Publish(context.Background(), tx, f.tables, s.cur, s.next)
		return err
	})
	return out, err
}

// generation is a committed generation and its manifest.
type generation struct {
	rec      publication.Record
	manifest manifestv2.Manifest
}

func (f *fixture) publish(t *testing.T, s staged) generation {
	t.Helper()
	rec, err := f.commit(t, s)
	require.NoError(t, err)
	return generation{rec: rec, manifest: s.manifest}
}

// publishInit publishes an init generation of n base files after cur.
func (f *fixture) publishInit(t *testing.T, cur publication.Record, n int) generation {
	t.Helper()
	return f.publish(t, f.stage(t, cur, publication.KindInit, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		var files []manifestv2.File
		for range n {
			files = append(files, f.file(t, h.Epoch, manifestv2.TierBase))
		}
		return manifestv2.NewInit(h, files)
	}))
}

// stageFlush stages a flush of n delta files over g.
func (f *fixture) stageFlush(t *testing.T, g generation, n int) staged {
	t.Helper()
	return f.stage(t, g.rec, publication.KindFlush, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		var files []manifestv2.File
		for range n {
			files = append(files, f.file(t, h.Epoch, manifestv2.TierDelta))
		}
		return manifestv2.NewFlush(&g.manifest, h, files)
	})
}

// publishCompact publishes a compaction of every file of g into one base.
func (f *fixture) publishCompact(t *testing.T, g generation) generation {
	t.Helper()
	return f.publish(t, f.stage(t, g.rec, publication.KindCompact, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		h.InputGeneration = g.rec.Generation
		return manifestv2.NewCompact(&g.manifest, h, g.manifest.Keys(), []manifestv2.File{f.file(t, h.Epoch, manifestv2.TierBase)})
	}))
}

// keysOf answers the manifest key and every listed key of g, sorted.
func (f *fixture) keysOf(g generation) []string {
	keys := append(g.manifest.Keys(), f.layout.ManifestKey(testSchema, g.rec.Epoch, g.rec.Generation, g.rec.Token))
	slices.Sort(keys)
	return keys
}

// stagedKeys answers the keys s staged over its parent: its manifest and
// the files the parent does not list.
func (f *fixture) stagedKeys(s staged, parent generation) []string {
	keys := []string{s.next.ManifestKey}
	for _, key := range s.manifest.Keys() {
		if !slices.Contains(parent.manifest.Keys(), key) {
			keys = append(keys, key)
		}
	}
	return keys
}

// testOptions are short protocol times: a grace of 300 ms over a reader
// deadline of 200 ms, so a test waits out an expiry, and an orphan age of a
// millisecond, so an object staged before the pass is below its horizon.
func testOptions(gc bool) Options {
	return Options{
		GC: gc, RetentionGrace: 300 * time.Millisecond, ReaderDeadline: 200 * time.Millisecond,
		ClockMargin: 50 * time.Millisecond, OrphanAge: time.Millisecond,
	}
}

// errDiagnosisLocked fails a diagnosis that takes the schema lock, which on
// Aurora DSQL writes a lease row.
var errDiagnosisLocked = errors.New("the read-only diagnosis took the schema lock")

// reconciler answers a Reconciler over the fixture whose collection always
// takes the schema lock, and whose diagnosis fails if it asks for it.
func (f *fixture) reconciler(opts Options) *Reconciler {
	return &Reconciler{
		DB: f.db, Tables: f.tables, Layout: f.layout, Objects: f.store,
		Locker: cdc.SchemaLockerFunc(func(ctx context.Context, _ int16) (context.Context, func(), bool, error) {
			if !opts.GC {
				return nil, nil, false, errDiagnosisLocked
			}
			return ctx, func() {}, true, nil
		}),
		Schemas: fixedSchemas{testSchema},
		Opts:    opts,
	}
}

// pass runs one pass of the schema and answers its report.
func (f *fixture) pass(t *testing.T, opts Options) SchemaReport {
	t.Helper()
	report, err := f.reconciler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Schemas, 1)
	return report.Schemas[0]
}

// waitOutGrace sleeps past the test grace and margin.
func waitOutGrace() { time.Sleep(400 * time.Millisecond) }

type fixedSchemas []int16

func (s fixedSchemas) SchemaIDs(context.Context) ([]int16, error) { return s, nil }

// kinds answers the kinds of the findings, in order.
func kinds(findings []Finding) []FindingKind {
	out := make([]FindingKind, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.Kind)
	}
	return out
}
