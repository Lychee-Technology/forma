package federated

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// The pgx pass's engine-seam fixture: a real DuckDB over real parquet files
// for the parquet leg, and an in-memory snapshot for the PostgreSQL leg, so
// the pass runs its real statement, staging, merge and paging.

// pgxRow is one logical row of a fixture: its stored values, and where it
// lives. hot rows are served by the snapshot (entity_main, with a change_log
// entry in the dirty window); parquet rows by the parquet file; a row that is
// both was updated after its flush, so the parquet holds a stale version the
// anti-join must drop.
type pgxRow struct {
	i         int
	createdAt int64
	age       *int64
	hot       bool
	parquet   bool
	// deleted marks a row deleted after its flush: dirty, in parquet, and
	// in neither entity_main nor the answer.
	deleted bool
	// active, flag and key0 are the attributes only pgxSortMetadata
	// declares (pgx_pass_sort_keys_test.go): a bool_smallint bool, a
	// bool_text bool, and the EAV integer named forma_sort_key_0.
	active, flag *bool
	key0         *float64
}

func (r pgxRow) id() uuid.UUID { return parquetOnlyRowID(r.i) }

func ageOf(v int64) *int64 { return &v }

// pgxFixture holds the DuckDB database and the fake snapshot of one test.
type pgxFixture struct {
	db   *sql.DB
	dir  string
	rows []pgxRow
	leg  *fakeHotLeg
	// meta is the schema the engine reads; parquetOnlyMetadata when nil.
	meta *schemameta.MetadataCache
}

func newPgxFixture(t *testing.T, rows []pgxRow) *pgxFixture {
	t.Helper()
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	f := &pgxFixture{db: db, dir: t.TempDir(), rows: rows, leg: &fakeHotLeg{t: t, records: map[uuid.UUID]pgxRow{}}}
	var parquet []pgxRow
	for _, r := range rows {
		if r.parquet {
			parquet = append(parquet, r)
		}
		if r.hot || r.deleted {
			f.leg.dirty = append(f.leg.dirty, r.id())
		}
		if r.hot {
			f.leg.records[r.id()] = r
		}
	}
	if len(parquet) > 0 {
		f.writeParquet(t, "base.parquet", parquet)
	}
	return f
}

// writeParquet writes rows as one export object. A row the snapshot serves
// hot carries a stale age in parquet, so serving it from there shows.
func (f *pgxFixture) writeParquet(t *testing.T, name string, rows []pgxRow) string {
	t.Helper()
	path := filepath.Join(f.dir, name)
	values := make([]string, 0, len(rows))
	for _, r := range rows {
		age, active, flag, key0 := sqlValue(r.age), sqlValue(r.active), sqlValue(r.flag), sqlValue(r.key0)
		if r.hot {
			age, active, flag, key0 = "-1", sqlValue(negated(r.active)), sqlValue(negated(r.flag)), "-1"
		}
		values = append(values, fmt.Sprintf("('%s'::UUID, %d::BIGINT, 100::BIGINT, 0::BIGINT, 'row %d', %s::INTEGER, 'developer', %s::BOOLEAN, %s::BOOLEAN, %s::DOUBLE)",
			r.id(), r.createdAt, r.i, age, active, flag, key0))
	}
	_, err := f.db.Exec(`COPY (SELECT * FROM (VALUES ` + strings.Join(values, ", ") +
		`) AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age, tag, active, flag, forma_sort_key_0)) TO '` + path + `' (FORMAT PARQUET)`)
	require.NoError(t, err)
	return path
}

// engine builds a pgx engine over the fixture whose parquet source answers
// paths.
func (f *pgxFixture) engine(t *testing.T, paths []string, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	return f.engineOver(t, NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db}), paths, opts...)
}

// engineOver is engine with the DuckDB executor the caller hands it.
func (f *pgxFixture) engineOver(t *testing.T, duck DuckDBQueryExecutor, paths []string, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	cfg := pgxDuckConfig()
	meta := f.meta
	if meta == nil {
		meta = parquetOnlyMetadata(t)
	}
	all := append([]EngineOption{WithParquetSource(&fakeParquetSource{paths: paths})}, opts...)
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, duck, nil, cfg, meta, all...)
	e.hotLeg = f.leg
	return e
}

// sqlValue renders a fixture value as a SQL literal, NULL for nil.
func sqlValue[T any](v *T) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(*v)
}

// negated is a stale parquet copy's bool: the opposite of the snapshot's.
func negated(v *bool) *bool {
	if v == nil {
		return nil
	}
	n := !*v
	return &n
}

// visibleOrder is the fixture's answer in order: every row that is hot, or
// in parquet and neither dirty nor deleted, sorted by order.
func (f *pgxFixture) visibleOrder(t *testing.T, order sortkey.Order) []uuid.UUID {
	t.Helper()
	type keyed struct {
		id  uuid.UUID
		key []any
	}
	var visible []keyed
	for _, r := range f.rows {
		if r.deleted || !(r.hot || r.parquet) {
			continue
		}
		visible = append(visible, keyed{r.id(), pgxRowKey(t, r, order)})
	}
	for i := 1; i < len(visible); i++ {
		for j := i; j > 0 && order.Compare(visible[j].key, visible[j-1].key) < 0; j-- {
			visible[j], visible[j-1] = visible[j-1], visible[j]
		}
	}
	ids := make([]uuid.UUID, len(visible))
	for i, v := range visible {
		ids[i] = v.id
	}
	return ids
}

// pgxRowKey derives a row's key for order's columns, as the hot-keys query
// and the parquet leg both read it.
func pgxRowKey(t *testing.T, r pgxRow, order sortkey.Order) []any {
	t.Helper()
	key := make([]any, len(order.Columns))
	for i, col := range order.Columns {
		key[i] = pgxRowValue(t, r, col.Name)
	}
	return key
}

func pgxRowValue(t *testing.T, r pgxRow, name string) any {
	t.Helper()
	switch name {
	case sortkey.ColumnCreatedAt:
		return r.createdAt
	case sortkey.ColumnRowID:
		return r.id()
	case sortkey.ColumnDeletedTS:
		return int64(0)
	case "age":
		if r.age == nil {
			return nil
		}
		return *r.age
	case "active":
		return derefOrNil(r.active)
	case "flag":
		return derefOrNil(r.flag)
	case sqlgen.SortKeyColumn(0):
		return derefOrNil(r.key0)
	}
	t.Fatalf("the fixture has no sort column %q", name)
	return nil
}

// fakeHotLeg is an in-memory snapshot. It applies no condition: the hot-keys
// query's filtering is QueryHotKeys' to test (#855).
type fakeHotLeg struct {
	t       *testing.T
	dirty   []uuid.UUID
	records map[uuid.UUID]pgxRow
	history bool

	beginErr, changeLogErr, hotKeysErr error
	// afterChangeLog runs once the dirty set is read: the seam for a
	// writer between the snapshot and the scan.
	afterChangeLog func()

	begins, commits, rollbacks int
	// historyChecks counts the change_log reads that asked for
	// HasParquetHistory (#720).
	historyChecks int
	lastChangeLog ChangeLogSnapshotRequest
	lastHotKeys   *model.HotKeysQuery
	readRows      [][]uuid.UUID
}

func (f *fakeHotLeg) begin(context.Context) (hotLegSnapshot, error) {
	f.begins++
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return f, nil
}

func (f *fakeHotLeg) readChangeLog(_ context.Context, req ChangeLogSnapshotRequest) (ChangeLogSnapshot, error) {
	f.lastChangeLog = req
	if req.ParquetHistory {
		f.historyChecks++
	}
	if f.changeLogErr != nil {
		return ChangeLogSnapshot{}, f.changeLogErr
	}
	if f.afterChangeLog != nil {
		defer f.afterChangeLog()
	}
	return ChangeLogSnapshot{DirtyIDs: f.dirty, HasParquetHistory: req.ParquetHistory && f.history}, nil
}

func (f *fakeHotLeg) queryHotKeys(_ context.Context, req model.HotKeysQuery) ([]model.HotKey, error) {
	f.lastHotKeys = &req
	if f.hotKeysErr != nil {
		return nil, f.hotKeysErr
	}
	var keys []model.HotKey
	for id, r := range f.records {
		key := make([]any, len(req.SortColumns))
		for i, name := range req.SortColumns {
			key[i] = pgxRowValue(f.t, r, name)
		}
		keys = append(keys, model.HotKey{RowID: id, VerTS: 200, Key: key})
	}
	return keys, nil
}

func (f *fakeHotLeg) readHotRows(_ context.Context, _ model.StorageTables, schemaID int16, ids []uuid.UUID) ([]*model.PersistentRecord, error) {
	f.readRows = append(f.readRows, ids)
	out := make([]*model.PersistentRecord, len(ids))
	for i, id := range ids {
		r, ok := f.records[id]
		if !ok {
			return nil, fmt.Errorf("hot row %s is not in the snapshot", id)
		}
		rec := &model.PersistentRecord{SchemaID: schemaID, RowID: id, CreatedAt: r.createdAt}
		if r.age != nil {
			rec.Int32Items = map[string]int32{"integer_01": int32(*r.age)}
		}
		out[i] = rec
	}
	return out, nil
}

func (f *fakeHotLeg) commit(context.Context) error { f.commits++; return nil }
func (f *fakeHotLeg) rollback(context.Context)     { f.rollbacks++ }

// derefOrNil is a fixture value as a sort key: nil for an absent one.
func derefOrNil[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}

func recordIDs(records []*model.PersistentRecord) []uuid.UUID {
	ids := make([]uuid.UUID, len(records))
	for i, r := range records {
		ids[i] = r.RowID
	}
	return ids
}

// pgxQuery is a query of the fixture's schema in the default all-tier form.
func pgxQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: parquetOnlySchemaID}}
}

// pgxDuckConfig is hybridDuckConfig under the pgx transport.
func pgxDuckConfig() forma.DuckDBConfig {
	cfg := hybridDuckConfig()
	cfg.HotLegTransport = forma.HotLegTransportPgx
	return cfg
}
