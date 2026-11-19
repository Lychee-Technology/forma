package federatedv2

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The integration fixture: PostgreSQL storage and publication tables, a
// manifest store and parquet objects on local disk, and the federated engine
// over them as the factory wires it, with this package's Source as its
// publication reader. Generations are seeded the way a V2 writer commits
// them: parquet and manifest staged first, then Publish and the marks in one
// transaction.

const schemaID int16 = 7

// row is one logical row: its values, and its change_log entries.
type row struct {
	i    int
	name string
	age  int64
}

func (r row) id() uuid.UUID { return uuid.MustParse(fmt.Sprintf("00000000-0000-7000-8000-%012d", r.i)) }

type fixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	tables model.StorageTables
	pub    publication.Tables
	meta   *schemameta.MetadataCache
	duck   *sql.DB
	layout manifestv2.Layout
	store  *faultyStore
	// probes counts the existence probes the source ran.
	probes int
	mu     sync.Mutex
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	f := &fixture{
		ctx: ctx, pool: pool,
		tables: testdb.CreateDSQLStorageTables(t, ctx, pool, "fv2"),
		layout: manifestv2.Layout{Prefix: filepath.Join(t.TempDir(), "data")},
		store:  &faultyStore{Store: &manifestv2.FSStore{Dir: t.TempDir()}},
	}
	f.pub = publication.TablesFor(f.tables.EntityMain)
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, f.tables.EntityMain) {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{f.pub.Record, f.pub.Generation} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	f.meta = schemameta.NewMetadataCache()
	require.NoError(t, f.meta.RegisterSchema("v2_reader", schemaID, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
		"age":  {AttributeID: 2, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: "integer_01"}},
	}))
	duck, err := sql.Open("duckdb", "?lambda_syntax=DISABLE_SINGLE_ARROW")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, duck.Close()) })
	f.duck = duck
	return f
}

// faultyStore is the manifest store, counting its gets, and corrupting or
// losing objects on request.
type faultyStore struct {
	manifestv2.Store
	mu      sync.Mutex
	gets    int
	lost    map[string]bool
	corrupt map[string]bool
}

func (s *faultyStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets++
	lost, corrupt := s.lost[key], s.corrupt[key]
	s.mu.Unlock()
	if lost {
		return nil, fmt.Errorf("get object %s: %w", key, manifestv2.ErrObjectNotFound)
	}
	raw, err := s.Store.Get(ctx, key)
	if err == nil && corrupt {
		raw = append(raw[:len(raw):len(raw)], ' ')
	}
	return raw, err
}

func (s *faultyStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// source is the fixture's publication reader, pinned at epoch.
func (f *fixture) source(t *testing.T, epoch int64) *Source {
	t.Helper()
	cache, err := manifestv2.NewCache(f.store, 16)
	require.NoError(t, err)
	src, err := New(Config{
		EntityMain: f.tables.EntityMain, Manifests: cache, Epochs: map[int16]int64{schemaID: epoch},
		ObjectURI: func(key string) string { return key },
		Exists: func(_ context.Context, key string) (bool, error) {
			f.mu.Lock()
			f.probes++
			f.mu.Unlock()
			_, err := os.Stat(key)
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return err == nil, err
		},
	})
	require.NoError(t, err)
	return src
}

// engine is the federated engine over the fixture with reader as its
// publication source, and the repository as its degraded fallback.
func (f *fixture) engine(t *testing.T, reader federated.PublicationReader, duck federated.DuckDBQueryExecutor, breaker *federated.CircuitBreaker) *federated.DBFederatedQueryEngine {
	t.Helper()
	dbEngine, err := internal.WithDatabaseEngine(forma.EnginePostgreSQL)
	require.NoError(t, err)
	repo := internal.NewDBPersistentRecordRepository(f.pool, f.meta, dbEngine)
	if duck == nil {
		duck = federated.NewDuckDBClientQueryExecutor(&federated.DuckDBClient{DB: f.duck})
	}
	cfg := forma.DuckDBConfig{
		Enabled: true, S3Bucket: "b", HotLegTransport: forma.HotLegTransportPgx,
		Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid},
	}
	return federated.NewDBFederatedQueryEngine(repo, duck, breaker, cfg, f.meta,
		federated.WithPgxHotLeg(f.pool, repo), federated.WithPublicationReader(reader, 0))
}

// write inserts or updates r in entity_main and adds an unflushed change_log
// entry at changedAt.
func (f *fixture) write(t *testing.T, r row, changedAt int64) {
	t.Helper()
	quote := func(name string) string { return pgx.Identifier{name}.Sanitize() }
	_, err := f.pool.Exec(f.ctx, "INSERT INTO "+quote(f.tables.EntityMain)+
		" (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01, integer_01) VALUES ($1, $2, $3, $4, $5, $6)"+
		" ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET ltbase_updated_at = $4, text_01 = $5, integer_01 = $6",
		schemaID, r.id(), int64(1000+r.i), changedAt, r.name, r.age)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, "INSERT INTO "+quote(f.tables.ChangeLog)+" (schema_id, row_id, changed_at) VALUES ($1, $2, $3)",
		schemaID, r.id(), changedAt)
	require.NoError(t, err)
}

// initialize writes the schema's record as registration does.
func (f *fixture) initialize(t *testing.T, state publication.State) {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	init := publication.Initialize
	if state == publication.AwaitingInit {
		init = publication.InitializeAwaitingInit
	}
	_, err = init(f.ctx, publication.PgxTx(tx), f.pub, schemaID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
}

// record reads the schema's record outside any pass.
func (f *fixture) record(t *testing.T) publication.Record {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	snap, err := publication.Read(f.ctx, publication.PgxTx(tx), f.pub, schemaID)
	require.NoError(t, err)
	return snap.Record
}

// deltaFile exports rows at their versions as one delta object under the
// layout, and answers its manifest entry.
func (f *fixture) deltaFile(t *testing.T, epoch int64, rows map[row]int64) manifestv2.File {
	t.Helper()
	key, err := f.layout.NewParquetKey(schemaID, epoch, manifestv2.TierDelta)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(key), 0o755))
	values := make([]string, 0, len(rows))
	file := manifestv2.File{Tier: manifestv2.TierDelta, Key: key, RowCount: int64(len(rows))}
	for r, version := range rows {
		values = append(values, fmt.Sprintf("('%s'::UUID, %d::BIGINT, %d::BIGINT, 0::BIGINT, '%s', %d::INTEGER)", r.id(), 1000+r.i, version, r.name, r.age))
		if file.RowIDMin == uuid.Nil || r.id().String() < file.RowIDMin.String() {
			file.RowIDMin = r.id()
		}
		if r.id().String() > file.RowIDMax.String() {
			file.RowIDMax = r.id()
		}
	}
	_, err = f.duck.Exec(`COPY (SELECT * FROM (VALUES ` + strings.Join(values, ", ") +
		`) AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age)) TO '` + key + `' (FORMAT PARQUET)`)
	require.NoError(t, err)
	raw, err := os.ReadFile(key)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	file.SizeBytes, file.SHA256 = int64(len(raw)), hex.EncodeToString(sum[:])
	file.Columns = map[string]string{}
	described, err := f.duck.Query(`SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('` + key + `'))`)
	require.NoError(t, err)
	defer func() { require.NoError(t, described.Close()) }()
	for described.Next() {
		var name, typ string
		require.NoError(t, described.Scan(&name, &typ))
		file.Columns[name] = typ
	}
	require.NoError(t, described.Err())
	return file
}

// flush publishes a flush generation of the delta files after the current
// record, and marks each row's entry at its listed version in the same
// transaction, as cdc-flush commits it.
func (f *fixture) flush(t *testing.T, rows map[row]int64, deltas ...manifestv2.File) publication.Record {
	t.Helper()
	cur := f.record(t)
	lineage := cur.Lineage
	if cur.State != publication.Published {
		lineage = uuid.New()
	}
	h, err := manifestv2.Stage(cur, publication.KindFlush, lineage, uuid.New())
	require.NoError(t, err)
	var parent *manifestv2.Manifest
	if cur.State == publication.Published {
		ref, err := manifestv2.RecordRef(cur)
		require.NoError(t, err)
		parent, err = manifestv2.Fetch(f.ctx, f.store, ref)
		require.NoError(t, err)
	}
	m, err := manifestv2.NewFlush(parent, h, deltas)
	require.NoError(t, err)
	var stagedAt int64
	require.NoError(t, f.pool.QueryRow(f.ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&stagedAt))
	key, digest, err := manifestv2.PutManifest(f.ctx, f.store, f.layout, m)
	require.NoError(t, err)

	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	next, err := publication.Publish(f.ctx, publication.PgxTx(tx), f.pub, cur, publication.Next{
		Kind: publication.KindFlush, Token: h.Token, ManifestKey: key, ManifestSHA256: digest,
		Lineage: lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
	})
	require.NoError(t, err)
	for r, version := range rows {
		_, err := tx.Exec(f.ctx, "UPDATE "+pgx.Identifier{f.tables.ChangeLog}.Sanitize()+
			" SET flushed_at = $3 WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0 AND changed_at = $4",
			schemaID, r.id(), stagedAt, version)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(f.ctx))
	return next
}
