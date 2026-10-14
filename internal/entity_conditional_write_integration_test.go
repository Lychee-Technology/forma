package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Conditional mutations (#649) on real PostgreSQL, over the cond649 schema
// (condSchemaCache): integer, date and datetime attributes in both storage
// kinds, which the shipped schemas do not have.

type condPG struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	tables model.StorageTables
	em     forma.EntityManager
	txr    *transactor
}

// newCondPG builds a manager and transactor over temporary tables. The pool
// is wide enough that every worker of the concurrency tests holds its own
// connection.
func newCondPG(t *testing.T) *condPG {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	testdb.Connect(t, ctx) // skips, or fails in CI, when PostgreSQL is absent
	cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
	require.NoError(t, err)
	cfg.MaxConns = 48
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	tables := createTempPersistentTables(t, ctx, pool)
	cache := condSchemaCache()
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema(condSchemaName, condSchemaID, cache))
	registry := &stubSchemaRegistry{schemaID: condSchemaID, schemaName: condSchemaName, cache: cache}

	config := createTestConfig()
	config.Database.TableNames = forma.TableNames{EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog}
	config.Transaction.DefaultTimeout = 30 * time.Second
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), NewDBPersistentRecordRepository(pool, metadata), nil, registry, config, nil)
	txr, err := NewTransactor(em)
	require.NoError(t, err)
	return &condPG{ctx: ctx, pool: pool, tables: tables, em: em, txr: txr.(*transactor)}
}

func (e *condPG) create(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName}, Data: data})
	require.NoError(t, err)
	return record.RowID
}

func (e *condPG) get(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: condSchemaName, RowID: &rowID})
	require.NoError(t, err)
	return record.Attributes
}

// matching answers the rows a hot-tier Query with cond returns.
func (e *condPG) matching(t *testing.T, cond forma.Condition) map[uuid.UUID]bool {
	t.Helper()
	result, err := e.em.Query(e.ctx, &forma.QueryRequest{SchemaName: condSchemaName, Condition: cond, Page: 1, ItemsPerPage: 100})
	require.NoError(t, err)
	rows := make(map[uuid.UUID]bool, len(result.Data))
	for _, record := range result.Data {
		rows[record.RowID] = true
	}
	return rows
}

// snapshot is the row's stored state in all three tables, by direct SQL.
func (e *condPG) snapshot(t *testing.T, rowID uuid.UUID) rowSnapshot {
	t.Helper()
	var s rowSnapshot
	queries := []struct {
		dest  *string
		table string
		sql   string
	}{
		{&s.main, e.tables.EntityMain, "SELECT COALESCE((SELECT row_to_json(m)::text FROM %s m WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2), '')"},
		{&s.eav, e.tables.EAVData, "SELECT COALESCE(json_agg(v ORDER BY attr_id, array_indices)::text, '[]') FROM %s v WHERE schema_id = $1 AND row_id = $2"},
		{&s.changeLog, e.tables.ChangeLog, "SELECT COALESCE(json_agg(c ORDER BY flushed_at, changed_at)::text, '[]') FROM %s c WHERE schema_id = $1 AND row_id = $2"},
	}
	for _, q := range queries {
		require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(q.sql, sanitizeIdentifier(q.table)), condSchemaID, rowID).Scan(q.dest))
	}
	return s
}

// hotStamp answers the row's ltbase_updated_at and its hot change_log
// entries' changed_at values.
func (e *condPG) hotStamp(t *testing.T, rowID uuid.UUID) (int64, []int64) {
	t.Helper()
	var updatedAt int64
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT ltbase_updated_at FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(e.tables.EntityMain)), condSchemaID, rowID).Scan(&updatedAt))
	var hot []int64
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT COALESCE(array_agg(changed_at), '{}') FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0",
		sanitizeIdentifier(e.tables.ChangeLog)), condSchemaID, rowID).Scan(&hot))
	return updatedAt, hot
}

func condUpdate(rowID uuid.UUID, precondition forma.Condition, updates map[string]any, ops ...forma.UpdateOp) *forma.EntityOperation {
	op := condOp(rowID)
	op.Updates, op.Preconditions, op.UpdateOps = updates, precondition, ops
	return op
}

func condDelete(rowID uuid.UUID, precondition forma.Condition) *forma.EntityOperation {
	op := condOp(rowID)
	op.Preconditions = precondition
	return op
}

func and(conds ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: conds}
}

func or(conds ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: conds}
}

// TestPreconditionHoldsExactlyWhenQueryReturnsTheRow is the truth table of
// invariant 4: a precondition holds on a row exactly when a hot-tier Query
// with the same condition returns that row. Each operator runs on a
// column-bound attribute (version, rank, due_main) and an EAV one (counter,
// name, due). The update writes only due_iso, which no condition tests, so
// the outcome of one case cannot move another, and the EAV-less row stays
// EAV-less.
func TestPreconditionHoldsExactlyWhenQueryReturnsTheRow(t *testing.T) {
	e := newCondPG(t)
	past, future := time.Now().Add(-48*time.Hour).UTC(), time.Now().Add(48*time.Hour).UTC()
	rows := []uuid.UUID{
		e.create(t, map[string]any{"version": int64(7), "rank": int64(2), "counter": int64(10), "name": "alpha", "due": past, "due_main": future}),
		e.create(t, map[string]any{"version": int64(8), "rank": int64(5), "counter": int64(3), "name": "beta", "due": future, "due_main": past}),
		e.create(t, map[string]any{"name": "gamma"}),
		e.create(t, map[string]any{"version": int64(7), "rank": int64(-1), "counter": int64(7)}),
		e.create(t, map[string]any{"version": int64(9), "rank": int64(0)}), // no EAV row at all
	}
	cases := []struct {
		name string
		cond forma.Condition
	}{
		{"equals main", kv("version", "equals:7")},
		{"equals eav", kv("counter", "equals:10")},
		{"equals eav text", kv("name", "equals:alpha")},
		{"not_equals main", kv("version", "not_equals:7")},
		{"not_equals eav", kv("counter", "not_equals:10")},
		{"gt main", kv("version", "gt:7")},
		{"gte main", kv("rank", "gte:2")},
		{"lt eav", kv("counter", "lt:7")},
		{"lte eav", kv("counter", "lte:7")},
		{"starts_with eav", kv("name", "starts_with:g")},
		{"in main", kv("version", "in:[7, 9]")},
		{"in eav", kv("counter", "in:[3, 10]")},
		{"not_in main", kv("rank", "not_in:[2]")},
		{"not_in eav", kv("counter", "not_in:[3]")},
		{"is_null main", kv("version", "is_null:true")},
		{"is_null eav", kv("counter", "is_null:true")},
		{"is_not_null main", kv("rank", "is_not_null:true")},
		{"is_not_null eav", kv("name", "is_not_null:true")},
		{"$now main", kv("due_main", "gte:$now")},
		{"$now eav", kv("due", "lt:$now")},
		{"and across kinds", and(kv("version", "equals:7"), kv("counter", "gt:5"))},
		{"or across kinds", or(kv("version", "is_null:true"), kv("counter", "in:[3]"))},
		{"nested", or(and(kv("version", "equals:7"), kv("name", "equals:alpha")), kv("rank", "lt:0"))},
		{"and with an empty and", and(and(), kv("counter", "equals:10"))},
		{"or with an empty or", or(or(), kv("counter", "equals:10"))},
	}
	// An empty and holds for every row. Alone or inside an or, it reaches
	// the EAV-less row with no column-bound or is_null leaf to force Query's
	// entity_main anchor.
	everyRow := []struct {
		name string
		cond forma.Condition
	}{
		{"empty and", and()},
		{"or with an empty and", or(and(), kv("counter", "equals:10"))},
	}
	requireGuardMatchesQuery := func(t *testing.T, i int, cond forma.Condition, want map[uuid.UUID]bool) {
		t.Helper()
		for _, rowID := range rows {
			written := time.Unix(int64(1_700_000_000+i), 0).UTC()
			_, err := e.em.Update(e.ctx, condUpdate(rowID, cond, map[string]any{"due_iso": written}))
			if want[rowID] {
				require.NoError(t, err, "Query returns row %s, so the precondition holds", rowID)
				assert.Equal(t, written, e.get(t, rowID)["due_iso"])
				continue
			}
			require.ErrorIs(t, err, forma.ErrPreconditionFailed, "Query does not return row %s", rowID)
			assert.NotErrorIs(t, err, forma.ErrNotFound)
		}
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := e.matching(t, tc.cond)
			require.NotEmpty(t, want, "the case must match some row")
			require.Less(t, len(want), len(rows), "the case must also miss some row")
			requireGuardMatchesQuery(t, i, tc.cond, want)
		})
	}
	for i, tc := range everyRow {
		t.Run(tc.name, func(t *testing.T) {
			want := e.matching(t, tc.cond)
			require.Len(t, want, len(rows), "the case must match every row")
			requireGuardMatchesQuery(t, len(cases)+i, tc.cond, want)
		})
	}
}

// TestFailedPreconditionWritesNothing compares every table's image of the
// row before and after a refused Update and Delete.
func TestFailedPreconditionWritesNothing(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(7), "counter": int64(1), "name": "kept"})
	before := e.snapshot(t, rowID)

	_, err := e.em.Update(e.ctx, condUpdate(rowID, kv("version", "equals:6"), map[string]any{"name": "lost"},
		forma.Increment("counter", 1), forma.ServerTime("due", 0)))
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published)
	assert.Equal(t, fmt.Sprintf("precondition does not hold for entity (row=%s)", rowID), message)
	assert.Equal(t, before, e.snapshot(t, rowID), "a refused update leaves every table as it was")

	err = e.em.Delete(e.ctx, condDelete(rowID, kv("counter", "gt:1")))
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	assert.Equal(t, before, e.snapshot(t, rowID), "a refused delete leaves every table as it was")

	require.NoError(t, e.em.Delete(e.ctx, condDelete(rowID, and(kv("version", "equals:7"), kv("counter", "equals:1")))))
	assert.Empty(t, e.snapshot(t, rowID).main, "a delete whose precondition holds deletes")
}

// TestUpdatesReplacingARowArrayWritesNothing is the #674 review reproduction
// on PostgreSQL. items holds two objects, and the update sends an object for
// items beside server_time on items.seenAt. On this row the object replaces
// the list rather than merging into it, so Updates writes the operator's
// attribute too: the update is refused, and no table changes. The operator
// alone is refused as inside the array.
func TestUpdatesReplacingARowArrayWritesNothing(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"items": []any{
		map[string]any{"qty": int64(1), "seenAt": time.UnixMilli(1000).UTC()},
		map[string]any{"qty": int64(2), "seenAt": time.UnixMilli(2000).UTC()},
	}})
	before := e.snapshot(t, rowID)
	cases := map[string]struct {
		updates map[string]any
		want    string
	}{
		"beside a replacing object": {map[string]any{"items": map[string]any{"qty": int64(3)}},
			"attribute 'items.seenAt' appears in both updates and update operators: the row holds a value at 'items' that is not an object"},
		"alone": {nil, "attribute 'items.seenAt' is inside the array 'items'"},
	}
	for name, tc := range cases {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, tc.updates, forma.ServerTime("items.seenAt", 0)))
		require.ErrorIs(t, err, forma.ErrInvalidInput, name)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, name)
		assert.Contains(t, message, tc.want, name)
		assert.Equal(t, before, e.snapshot(t, rowID), "%s: every table as it was", name)
	}
}

// TestMissingRowIsNotFoundWhateverThePrecondition pins the rule that tells
// the two refusals apart: no row (never created, or deleted) is ErrNotFound
// even when the precondition could never hold.
func TestMissingRowIsNotFoundWhateverThePrecondition(t *testing.T) {
	e := newCondPG(t)
	deleted := e.create(t, map[string]any{"version": int64(7)})
	require.NoError(t, e.em.Delete(e.ctx, condDelete(deleted, nil)))

	for name, rowID := range map[string]uuid.UUID{"never created": uuid.New(), "deleted": deleted} {
		for _, precondition := range []forma.Condition{kv("version", "equals:7"), kv("counter", "is_null:true")} {
			_, err := e.em.Update(e.ctx, condUpdate(rowID, precondition, nil, forma.Increment("version", 1)))
			require.ErrorIs(t, err, forma.ErrNotFound, "update, %s", name)
			assert.NotErrorIs(t, err, forma.ErrPreconditionFailed)

			err = e.em.Delete(e.ctx, condDelete(rowID, precondition))
			require.ErrorIs(t, err, forma.ErrNotFound, "delete, %s", name)
			assert.NotErrorIs(t, err, forma.ErrPreconditionFailed)
		}
	}
}

// TestInvalidPreconditionIsRefusedBeforeAnyStatement pins that the
// renderer's refusals surface before the write sends SQL, so they leave a Tx
// usable.
func TestInvalidPreconditionIsRefusedBeforeAnyStatement(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(7), "counter": int64(1)})
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condUpdate(rowID, kv("counter", "in:[]"), map[string]any{"name": "x"}))
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		err = tx.Delete(ctx, condDelete(rowID, kv("version", "contains:7")))
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		_, err = tx.Update(ctx, condUpdate(rowID, kv("version", "equals:7"), map[string]any{"name": "after"}))
		return err
	})
	require.NoError(t, err, "the refusals did not poison the Tx")
	assert.Equal(t, "after", e.get(t, rowID)["name"])
}

// TestOperatorOnASystemColumnAliasIsRefusedBeforeAnyStatement is the #674
// review reproduction on PostgreSQL. createdEpoch and touchedAt are
// read-only aliases of ltbase_created_at and ltbase_updated_at: the writer
// drops an update's value for them, so an operator there succeeded and
// stored nothing. Intake refuses both kinds: outside a Tx no table changes,
// and inside one the refusals leave the Tx usable.
func TestOperatorOnASystemColumnAliasIsRefusedBeforeAnyStatement(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"name": "before"})
	created := e.get(t, rowID)["createdEpoch"]
	require.NotNil(t, created)
	before := e.snapshot(t, rowID)
	ops := []forma.UpdateOp{forma.Increment("createdEpoch", 1000), forma.ServerTime("touchedAt", time.Hour)}
	for _, op := range ops {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, nil, op))
		require.ErrorIs(t, err, forma.ErrInvalidInput, op.Kind)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, op.Kind)
		assert.Contains(t, message, fmt.Sprintf("attribute '%s', which is bound to system column", op.Attribute))
		assert.Equal(t, before, e.snapshot(t, rowID), "%s: every table as it was", op.Kind)
	}

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		for _, op := range ops {
			_, err := tx.Update(ctx, condUpdate(rowID, nil, nil, op))
			require.ErrorIs(t, err, forma.ErrInvalidInput, op.Kind)
		}
		_, err := tx.Update(ctx, condUpdate(rowID, nil, map[string]any{"name": "after"}))
		return err
	})
	require.NoError(t, err, "the refusals did not poison the Tx")
	got := e.get(t, rowID)
	assert.Equal(t, "after", got["name"])
	assert.Equal(t, created, got["createdEpoch"], "the creation stamp is the store's, not the operator's")
}
