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

type memObject struct {
	data         []byte
	lastModified int64
}

type memStore struct {
	mu         sync.Mutex
	objects    map[string]memObject
	now        func() int64
	failDelete func(key string) error
	getErr     map[string]error
	beforeGet  func(key string)
	beforeHash func(key string)
	deletes    []string
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

func (m *memStore) set(key string, data []byte, lastModified int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = memObject{data: data, lastModified: lastModified}
}

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
	if m.beforeHash != nil {
		m.beforeHash(key)
	}
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

func (m *memStore) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.objects))
}

type fixture struct {
	db     *sql.DB
	tables publication.Tables
	layout manifestv2.Layout
	store  *memStore
}

const testSchema int16 = 7

func newFixture(t *testing.T) *fixture {
	t.Helper()
	testdb.Connect(t, context.Background())
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
		for _, table := range []string{f.tables.Record, f.tables.Generation, f.tables.Epoch, f.tables.Storage} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	_, err = db.Exec(publication.StorageStatement(forma.EnginePostgreSQL, entityMain))
	require.NoError(t, err)
	_, err = publication.BindStorage(context.Background(), publication.DB(db), f.tables, f.target())
	require.NoError(t, err)
	return f
}

func (f *fixture) target() publication.StorageTarget {
	return publication.StorageTarget{Bucket: "writers-bucket", Prefix: f.layout.Prefix}
}

func (f *fixture) serverMillis(t *testing.T) int64 {
	t.Helper()
	var ms int64
	require.NoError(t, f.db.QueryRow("SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms))
	return ms
}

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

func (f *fixture) initialized(t *testing.T) publication.Record {
	t.Helper()
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Initialize(context.Background(), tx, f.tables, testSchema)
		return err
	}))
	return f.record(t)
}

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

type staged struct {
	cur      publication.Record
	manifest manifestv2.Manifest
	next     publication.Next
}

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

func (f *fixture) publishCompact(t *testing.T, g generation) generation {
	t.Helper()
	return f.publish(t, f.stage(t, g.rec, publication.KindCompact, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		h.InputGeneration = g.rec.Generation
		return manifestv2.NewCompact(&g.manifest, h, g.manifest.Keys(), []manifestv2.File{f.file(t, h.Epoch, manifestv2.TierBase)})
	}))
}

func (f *fixture) keysOf(g generation) []string {
	keys := append(g.manifest.Keys(), f.layout.ManifestKey(testSchema, g.rec.Epoch, g.rec.Generation, g.rec.Token))
	slices.Sort(keys)
	return keys
}

func (f *fixture) stagedKeys(s staged, parent generation) []string {
	keys := []string{s.next.ManifestKey}
	for _, key := range s.manifest.Keys() {
		if !slices.Contains(parent.manifest.Keys(), key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func testOptions(gc bool) Options {
	return Options{
		GC: gc, RetentionGrace: 300 * time.Millisecond, ReaderDeadline: 200 * time.Millisecond,
		ClockMargin: 50 * time.Millisecond, OrphanAge: time.Millisecond,
	}
}

func agedOptions(gc bool) Options {
	return Options{
		GC: gc, RetentionGrace: time.Minute, ReaderDeadline: 50 * time.Second,
		ClockMargin: 50 * time.Millisecond, OrphanAge: time.Millisecond,
	}
}

func (f *fixture) ageStamps(t *testing.T, d time.Duration) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = retired_at - $1 WHERE schema_id = $2 AND retired_at <> 0",
		f.tables.Generation), d.Milliseconds(), testSchema)
	require.NoError(t, err)
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	for key, o := range f.store.objects {
		o.lastModified -= d.Milliseconds()
		f.store.objects[key] = o
	}
}

func (f *fixture) ageRecovery(t *testing.T, d time.Duration) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET recovery_retired_at = recovery_retired_at - $1 WHERE schema_id = $2 AND recovery_retired_at IS NOT NULL AND recovery_retired_at <> 0",
		f.tables.Epoch), d.Milliseconds(), testSchema)
	require.NoError(t, err)
}

func (f *fixture) requireOrphansEligible(t *testing.T, rep SchemaReport, opts Options, keys []string) {
	t.Helper()
	listed, err := f.store.List(context.Background(), "")
	require.NoError(t, err)
	at := map[string]int64{}
	for _, o := range listed {
		at[o.Key] = o.LastModified
	}
	cutoff := publication.OrphanCutoff(rep.Horizon, opts.ClockMargin)
	for _, key := range keys {
		lm, ok := at[key]
		require.True(t, ok, "%s is listed", key)
		require.Less(t, lm, cutoff, "%s is below the orphan cutoff: absent the fence it is collected", key)
	}
}

var errDiagnosisLocked = errors.New("the read-only diagnosis took the schema lock")

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
		Storage: f.target(),
	}
}

func (f *fixture) pass(t *testing.T, opts Options) SchemaReport {
	t.Helper()
	report, err := f.reconciler(opts).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Schemas, 1)
	return report.Schemas[0]
}

func waitOutGrace() { time.Sleep(400 * time.Millisecond) }

func waitOutListingPrecision() { time.Sleep(publication.LastModifiedPrecision + 300*time.Millisecond) }

type fixedSchemas []int16

func (s fixedSchemas) SchemaIDs(context.Context) ([]int16, error) { return s, nil }

func kinds(findings []Finding) []FindingKind {
	out := make([]FindingKind, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.Kind)
	}
	return out
}
