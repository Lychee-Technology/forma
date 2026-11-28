package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of Unset in BatchUpdate on unique keys and on the
// rows a batch modifies (#940).

// unsetKeysRow answers the unsetkeys document of row n: every key value its
// own, so the rows of one batch hold disjoint entries.
func unsetKeysRow(n int) map[string]any {
	return map[string]any{"note": "n", "slug": fmt.Sprintf("s%d", n), "handle": fmt.Sprintf("h%d", n),
		"tenant": fmt.Sprintf("t%d", n), "email": fmt.Sprintf("e%d", n)}
}

// unsetKeysRetake answers the document of another entity taking the value
// the unset of attr releases on row n.
func unsetKeysRetake(attr string, n int) map[string]any {
	doc := unsetKeysRow(n)
	retake := map[string]any{"note": "b", attr: doc[attr]}
	if attr == "tenant" || attr == "email" {
		retake["tenant"], retake["email"] = doc["tenant"], doc["email"]
	}
	return retake
}

// unsetKeysBatch creates one row per unsetKeyCases entry and answers the
// rows with the batch clearing each row's case component.
func (e *txEnv) unsetKeysBatch(t *testing.T, atomic bool) ([]uuid.UUID, *forma.BatchOperation) {
	t.Helper()
	rows := make([]uuid.UUID, len(unsetKeyCases))
	batch := &forma.BatchOperation{Atomic: atomic}
	for i, tc := range unsetKeyCases {
		rows[i] = e.mustCreate(t, unsetKeysSchemaName, unsetKeysRow(i))
		require.ElementsMatch(t, []int16{1, 2, 3}, e.heldKeys(t, rows[i]))
		batch.Operations = append(batch.Operations, *unsetKeysOp(rows[i], nil, forma.Unset(tc.attr)))
	}
	return rows, batch
}

// TestBatchUpdateUnsetReleasesTheUniqueKeyEntries clears, in one batch, a
// different key component on each row: each row releases that key's entry
// in the batch and keeps the others, and another entity can take the value.
func TestBatchUpdateUnsetReleasesTheUniqueKeyEntries(t *testing.T) {
	for _, atomic := range []bool{true, false} {
		t.Run(batchModeName(atomic), func(t *testing.T) {
			env := setupUnsetKeysPG(t)
			rows, batch := env.unsetKeysBatch(t, atomic)

			result, err := env.manager.BatchUpdate(env.ctx, batch)
			require.NoError(t, err)
			require.Empty(t, result.Failed)
			for i, tc := range unsetKeyCases {
				assert.NotContains(t, env.heldKeys(t, rows[i]), tc.key, "%s: the cleared component releases the entry", tc.name)
				assert.Len(t, env.heldKeys(t, rows[i]), 2, "%s: the other keys keep theirs", tc.name)
			}
			env.assertKeyTableMatchesRows(t)

			for i, tc := range unsetKeyCases {
				taker := env.mustCreate(t, unsetKeysSchemaName, unsetKeysRetake(tc.attr, i))
				assert.Contains(t, env.heldKeys(t, taker), tc.key, "%s: another entity takes the value", tc.name)
			}
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// TestRolledBackAtomicBatchUnsetKeepsTheReservations ends the same clears
// with an operation on a missing row: the batch rolls back under its locks,
// every row and entry is as stored, and the values stay reserved.
func TestRolledBackAtomicBatchUnsetKeepsTheReservations(t *testing.T) {
	env := setupUnsetKeysPG(t)
	rows, batch := env.unsetKeysBatch(t, true)
	before := make([]rowSnapshot, len(rows))
	for i, rowID := range rows {
		before[i] = env.snapshot(t, unsetKeysSchemaName, rowID)
	}
	entries := env.keyEntries(t)
	batch.Operations = append(batch.Operations, *unsetKeysOp(uuid.New(), nil, forma.Unset("memo")))

	_, err := env.manager.BatchUpdate(env.ctx, batch)
	require.ErrorIs(t, err, forma.ErrNotFound)

	for i, rowID := range rows {
		assert.Equal(t, before[i], env.snapshot(t, unsetKeysSchemaName, rowID), "no partial state")
	}
	assert.Equal(t, entries, env.keyEntries(t), "the reservations are kept")
	for i, tc := range unsetKeyCases {
		_, err := env.manager.Create(env.ctx, txCreateOp(unsetKeysSchemaName, unsetKeysRetake(tc.attr, i)))
		require.ErrorIs(t, err, forma.ErrUniqueConflict, tc.name)
	}
	env.assertKeyTableMatchesRows(t)
}

// rowTagTracer adds up, while armed, the rows each INSERT, UPDATE and
// DELETE its pool sends reports modified, by the table it writes.
type rowTagTracer struct {
	armed atomic.Bool
	mu    sync.Mutex
	rows  map[string]int64
	sql   map[*pgx.Conn]string
}

func (r *rowTagTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if r.armed.Load() {
		r.mu.Lock()
		r.sql[conn] = data.SQL
		r.mu.Unlock()
	}
	return ctx
}

func (r *rowTagTracer) TraceQueryEnd(_ context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if !r.armed.Load() || data.Err != nil {
		return
	}
	tag := data.CommandTag
	if !tag.Insert() && !tag.Update() && !tag.Delete() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[r.tableOf(r.sql[conn])] += tag.RowsAffected()
}

// tableOf answers the table sql writes, as its first statement keyword's
// target.
func (r *rowTagTracer) tableOf(sql string) string {
	fields := strings.Fields(sql)
	for i, field := range fields {
		upper := strings.ToUpper(field)
		if (upper == "INTO" || upper == "UPDATE" || upper == "FROM") && i+1 < len(fields) {
			return strings.Trim(fields[i+1], `"`)
		}
	}
	return "?"
}

// TestAtomicBatchUpdateUnsetPhysicalRows records the physical rows an
// atomic batch modifies when its clears delete list element rows. The
// atomic batch path is not metered yet (#692 has not landed): the batch's
// only budget is the logical one, MaxBatchSize, and its rows are not
// counted against any limit. The count is pinned so #692's meter can be
// held against it. Each row's write sends one entity_main UPDATE, one
// change_log upsert, and one DELETE of every eav_data row of the schema's
// attributes followed by one INSERT of those the row keeps: a cleared
// list's element rows are deleted and not reinserted.
func TestAtomicBatchUpdateUnsetPhysicalRows(t *testing.T) {
	tracer := &rowTagTracer{rows: map[string]int64{}, sql: map[*pgx.Conn]string{}}
	env := setupUnsetPGTuned(t, unsetSchemaDir, unsetSchemaName, 140, func(cfg *pgxpool.Config) { cfg.ConnConfig.Tracer = tracer })
	doc := unsetBaseDoc()
	doc["tags"] = []any{"a", "b", "c"}
	rows := []uuid.UUID{env.mustCreate(t, unsetSchemaName, doc), env.mustCreate(t, unsetSchemaName, doc)}
	tags, aliases := env.attrID(t, unsetSchemaName, "tags"), env.attrID(t, unsetSchemaName, "profile.aliases")
	for _, rowID := range rows {
		require.Equal(t, 3, env.eavRowCount(t, unsetSchemaName, rowID, tags))
		require.Equal(t, 2, env.eavRowCount(t, unsetSchemaName, rowID, aliases))
	}
	eavBefore := env.schemaEAVRows(t)

	tracer.armed.Store(true)
	_, err := env.manager.BatchUpdate(env.ctx, unsetBatch(true,
		unsetOp(rows[0], nil, forma.Unset("tags")),
		unsetOp(rows[1], nil, forma.Unset("tags"), forma.Unset("profile.aliases"))))
	tracer.armed.Store(false)
	require.NoError(t, err)

	eavAfter := env.schemaEAVRows(t)
	// Each row holds 13 eav_data rows: name, memo, note, short, counter,
	// profile.nick, owner.id, owner.name, 3 tags and 2 aliases.
	require.Equal(t, 26, eavBefore)
	assert.Equal(t, 3+3+2, eavBefore-eavAfter, "the clears delete 3 + 3 tag and 2 alias element rows")
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	assert.Equal(t, int64(2), tracer.rows[env.tables.EntityMain], "one entity_main row per operation")
	assert.Equal(t, int64(2), tracer.rows[env.tables.ChangeLog], "one change_log row per operation")
	assert.Equal(t, int64(eavBefore+eavAfter), tracer.rows[env.tables.EAVData],
		"every eav_data row deleted, and those kept reinserted")
	var total int64
	for _, n := range tracer.rows {
		total += n
	}
	t.Logf("physical rows modified by the atomic batch: %v (eav_data %d before, %d after)", tracer.rows, eavBefore, eavAfter)
	assert.Equal(t, int64(4+eavBefore+eavAfter), total, "no other table is written: unsetdoc has no unique key")
	assert.Equal(t, int64(48), total, "the observed count: 2 entity_main + 2 change_log + 26 eav_data deleted + 18 reinserted")
}

// schemaEAVRows counts unsetdoc's eav_data rows.
func (e *txEnv) schemaEAVRows(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, e.postgresPool.QueryRow(e.ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE schema_id = $1",
		sanitizeIdentifier(e.tables.EAVData)), e.schemaID(t, unsetSchemaName)).Scan(&n))
	return n
}
