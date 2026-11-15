package federated

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The pgx hot leg's PostgreSQL reads on the aurora-dsql test target (#691):
// the snapshot's dirty set in both forms and its parquet-history answer, the
// hot-keys query and the page read, each through a federated pass under
// HotLegTransport = pgx, with the parquet leg on a local file. FORMA_DSQL_DSN
// names a cluster, and with FORMA_DSQL_TARGET=ministack MiniStack's strict
// proxy (docs/dsql-ministack.md). Without either they run on the integration
// PostgreSQL. The tables use DSQL's DDL either way.

// dsqlHotLegRow is one row of the fixture, in parquetOnlyMetadata's schema:
// name is text_01, age integer_01, tag the EAV attribute 205.
type dsqlHotLegRow struct {
	i         int
	name, tag string
	age       int64
	// parquet puts the row in the parquet file. A row that is also hot has
	// a stale copy there, with age -1 and the tag "stale".
	parquet bool
	// main puts the row in entity_main.
	main bool
	// entries are the row's change_log entries.
	entries []dsqlHotLegEntry
}

type dsqlHotLegEntry struct {
	flushedAt, changedAt int64
	deleted              bool
}

func (r dsqlHotLegRow) id() uuid.UUID { return parquetOnlyRowID(r.i) }

// dsqlHotLegRows covers every case a pass tells apart. now places the
// recently flushed row's mark after the flush-grace cutoff.
func dsqlHotLegRows(now int64) map[string]dsqlHotLegRow {
	flushed := []dsqlHotLegEntry{{flushedAt: 1, changedAt: 100}}
	return map[string]dsqlHotLegRow{
		// Flushed long ago: served from parquet.
		"cold dev": {i: 1, name: "c1", tag: "dev", age: 30, parquet: true, main: true, entries: flushed},
		"cold ops": {i: 2, name: "c2", tag: "ops", age: 31, parquet: true, main: true, entries: flushed},
		// Updated after its flush: its parquet copy is stale.
		"updated": {i: 3, name: "u3", tag: "dev", age: 33, parquet: true, main: true,
			entries: []dsqlHotLegEntry{{flushedAt: 1, changedAt: 100}, {changedAt: 200}}},
		// Deleted after its flush: in parquet, and in no answer.
		"deleted": {i: 4, name: "d4", tag: "dev", age: 34, parquet: true,
			entries: []dsqlHotLegEntry{{flushedAt: 1, changedAt: 100}, {changedAt: 300, deleted: true}}},
		// Written and never flushed.
		"new": {i: 5, name: "n5", tag: "dev", age: 32, main: true, entries: []dsqlHotLegEntry{{changedAt: 400}}},
		// Marked within the flush grace: the hot form's widening serves it hot.
		"recent": {i: 6, name: "r6", tag: "ops", age: 35, parquet: true, main: true,
			entries: []dsqlHotLegEntry{{flushedAt: now, changedAt: 500}}},
	}
}

type dsqlHotLegFixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	tables model.StorageTables
	rows   map[string]dsqlHotLegRow
	duck   *sql.DB
	path   string
	now    int64
}

func newDSQLHotLegFixture(t *testing.T) *dsqlHotLegFixture {
	t.Helper()
	ctx := context.Background()
	pool, _ := testdb.OpenDSQLPool(t, ctx)
	now := time.Now().UnixMilli()
	f := &dsqlHotLegFixture{
		ctx: ctx, pool: pool, now: now, rows: dsqlHotLegRows(now),
		tables: testdb.CreateDSQLStorageTables(t, ctx, pool, "hot_leg_dsql"),
	}
	for _, r := range f.rows {
		f.seedPostgres(t, r)
	}
	duck, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, duck.Close()) })
	f.duck = duck
	f.path = f.writeParquet(t)
	return f
}

func (f *dsqlHotLegFixture) seedPostgres(t *testing.T, r dsqlHotLegRow) {
	t.Helper()
	quote := func(name string) string { return pgx.Identifier{name}.Sanitize() }
	exec := func(stmt string, args ...any) {
		t.Helper()
		_, err := f.pool.Exec(f.ctx, stmt, args...)
		require.NoError(t, err, stmt)
	}
	for _, e := range r.entries {
		var deletedAt any
		if e.deleted {
			deletedAt = e.changedAt
		}
		exec("INSERT INTO "+quote(f.tables.ChangeLog)+" (schema_id, row_id, flushed_at, changed_at, deleted_at) VALUES ($1, $2, $3, $4, $5)",
			parquetOnlySchemaID, r.id(), e.flushedAt, e.changedAt, deletedAt)
	}
	if !r.main {
		return
	}
	exec("INSERT INTO "+quote(f.tables.EntityMain)+
		" (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01, integer_01) VALUES ($1, $2, $3, $3, $4, $5)",
		parquetOnlySchemaID, r.id(), int64(1000+r.i), r.name, r.age)
	exec("INSERT INTO "+quote(f.tables.EAVData)+" (schema_id, row_id, attr_id, value_text) VALUES ($1, $2, 205, $3)",
		parquetOnlySchemaID, r.id(), r.tag)
}

// writeParquet writes the parquet rows as one export object.
func (f *dsqlHotLegFixture) writeParquet(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "base.parquet")
	var values []string
	for _, r := range f.rows {
		if !r.parquet {
			continue
		}
		age, tag := fmt.Sprint(r.age), r.tag
		if f.isHot(r) {
			age, tag = "-1", "stale"
		}
		values = append(values, fmt.Sprintf("('%s'::UUID, %d::BIGINT, 100::BIGINT, 0::BIGINT, '%s', %s::INTEGER, '%s')",
			r.id(), 1000+r.i, r.name, age, tag))
	}
	_, err := f.duck.Exec(`COPY (SELECT * FROM (VALUES ` + strings.Join(values, ", ") +
		`) AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age, tag)) TO '` + path + `' (FORMAT PARQUET)`)
	require.NoError(t, err)
	return path
}

// isHot reports whether a pass's hot form serves r from PostgreSQL.
func (f *dsqlHotLegFixture) isHot(r dsqlHotLegRow) bool {
	for _, e := range r.entries {
		if e.flushedAt == 0 || e.flushedAt == f.now {
			return true
		}
	}
	return false
}

func (f *dsqlHotLegFixture) ids(names ...string) []uuid.UUID {
	ids := make([]uuid.UUID, len(names))
	for i, n := range names {
		ids[i] = f.rows[n].id()
	}
	return ids
}

// engine is a pgx engine over the fixture: the real snapshot pool and the
// aurora-dsql repository's hot-leg reads, as the factory wires them.
func (f *dsqlHotLegFixture) engine(t *testing.T) *DBFederatedQueryEngine {
	t.Helper()
	meta := parquetOnlyMetadata(t)
	engine, err := internal.WithDatabaseEngine(forma.EngineAuroraDSQL)
	require.NoError(t, err)
	repo := internal.NewDBPersistentRecordRepository(f.pool, meta, engine)
	cfg := pgxDuckConfig()
	require.Equal(t, forma.HotLegTransportPgx, cfg.HotLegTransport)
	// The grace puts "recent"'s mark after the pass's cutoff.
	cfg.FlushVisibilityGraceMs = time.Minute.Milliseconds()
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.duck}), nil,
		cfg, meta, WithParquetSource(&fakeParquetSource{paths: []string{f.path}}), WithPgxHotLeg(f.pool, repo))
}

// The snapshot reads: the hot form widens the strict set by the rows marked
// at or after the cutoff, and the flushed entries are parquet history.
func TestDSQLHotLegSnapshotReadsTheDirtySet(t *testing.T) {
	f := newDSQLHotLegFixture(t)
	fetcher := NewPostgresDirtyIDFetcher(f.pool)
	read := func(form DirtySetForm) ChangeLogSnapshot {
		t.Helper()
		snap, err := fetcher.ReadChangeLogSnapshot(f.ctx, ChangeLogSnapshotRequest{
			Tables: f.tables, SchemaID: parquetOnlySchemaID, Form: form, GraceCutoffMs: f.now - 1_000, ParquetHistory: true,
		})
		require.NoError(t, err)
		require.True(t, snap.HasParquetHistory, "the schema has flushed entries")
		return snap
	}
	require.ElementsMatch(t, f.ids("updated", "deleted", "new"), read(DirtySetHotExcluded).DirtyIDs)
	require.ElementsMatch(t, f.ids("updated", "deleted", "new", "recent"), read(DirtySetHot).DirtyIDs)
}

// A pass answers every live row once, the dirty ones as the snapshot holds
// them: the hot keys sort and filter in PostgreSQL's snapshot as the parquet
// leg does in DuckDB, and the page read returns the hot rows' values.
func TestDSQLHotLegPassMergesTheTiers(t *testing.T) {
	f := newDSQLHotLegFixture(t)
	e := f.engine(t)
	query := func(cond forma.Condition) *model.FederatedAttributeQuery {
		return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: parquetOnlySchemaID, Condition: cond}}
	}

	records, total, err := e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(nil), 10, 0, ageOrders(false), nil)
	require.NoError(t, err)
	require.Equal(t, f.ids("cold dev", "cold ops", "new", "updated", "recent"), recordIDs(records), "by age, the hot rows' ages")
	require.Equal(t, int64(5), total)
	for _, rec := range records {
		var want dsqlHotLegRow
		for _, r := range f.rows {
			if r.id() == rec.RowID {
				want = r
			}
		}
		require.Equal(t, int32(want.age), rec.Int32Items["integer_01"], "row %d", want.i)
		require.Equal(t, want.name, rec.TextItems["text_01"], "row %d", want.i)
	}

	// The EAV filter: "updated" matches hot and not through its stale copy,
	// "recent" matches neither.
	records, total, err = e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(&forma.KvCondition{Attr: "tag", Value: "equals:dev"}), 10, 0, ageOrders(false), nil)
	require.NoError(t, err)
	require.Equal(t, f.ids("cold dev", "new", "updated"), recordIDs(records))
	require.Equal(t, int64(3), total)

	// A page inside the merged order counts every match.
	records, total, err = e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(nil), 2, 2, ageOrders(true), nil)
	require.NoError(t, err)
	require.Equal(t, f.ids("new", "cold ops"), recordIDs(records))
	require.Equal(t, int64(5), total)
}
