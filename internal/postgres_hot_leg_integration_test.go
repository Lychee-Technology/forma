package internal

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// Real-PostgreSQL tests of the pgx hot leg's reads (pgx-hot-leg-plan.md
// step 4): the hot-keys query filters as the PostgreSQL route does, reads
// only the dirty window, derives each key as the export will, and the page
// read answers what Get answers, all in one snapshot.

// logChange appends a change_log entry for rowID.
func (f *dslFixture) logChange(t *testing.T, rowID uuid.UUID, flushedAt, changedAt int64) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_id, row_id, flushed_at, changed_at) VALUES ($1, $2, $3, $4)",
		sanitizeIdentifier(f.tables.ChangeLog)), dslSchemaID, rowID, flushedAt, changedAt)
	require.NoError(t, err)
}

// inSnapshot runs read in a REPEATABLE READ READ ONLY transaction, the
// snapshot a pgx pass reads in.
func (f *dslFixture) inSnapshot(t *testing.T, read func(tx pgx.Tx)) {
	t.Helper()
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(f.ctx)) }()
	read(tx)
	require.NoError(t, tx.Commit(f.ctx))
}

func (f *dslFixture) queryHotKeys(t *testing.T, tx pgx.Tx, cond forma.Condition, cutoff int64, columns ...string) []model.HotKey {
	t.Helper()
	keys, err := f.repo.QueryHotKeys(f.ctx, tx, model.HotKeysQuery{
		Tables: f.tables, SchemaID: dslSchemaID, Condition: cond, GraceCutoffMs: cutoff, SortColumns: columns,
	})
	require.NoError(t, err, "%+v", cond)
	return keys
}

// hotKeyIDs answers the row IDs of the hot keys cond selects at cutoff,
// sorted.
func (f *dslFixture) hotKeyIDs(t *testing.T, cond forma.Condition, cutoff int64) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	f.inSnapshot(t, func(tx pgx.Tx) {
		for _, k := range f.queryHotKeys(t, tx, cond, cutoff) {
			ids = append(ids, k.RowID)
		}
	})
	return sortedIDs(ids...)
}

// The condition-DSL truth table, every case of it, selects the same rows
// through the hot-keys query as through the PostgreSQL route, with every row
// in the dirty window (dslFixture.hotKeys).
func TestHotKeysQuerySelectsThePostgresRouteRows(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		t.Run(fmt.Sprintf("numeric_eav=%v", numeric), func(t *testing.T) {
			f := newDSLFixture(t, numeric)
			f.hotKeys = true
			runDSLTruthTable(t, f)
		})
	}
	t.Run("like operands", func(t *testing.T) {
		f := newDSLFixtureWith(t, true, "operand_conformance", operandSchemaCache())
		f.hotKeys = true
		runLikeOperandConformance(t, f)
	})
}

// Text ranges compare bytes on both, whatever the database's collation:
// the hot-keys query renders the PostgreSQL route's COLLATE "C" (#728), so
// gt:B matches "a" as DuckDB's binary order does. Numeric and temporal
// ranges select the same rows on both too.
func TestHotKeysQueryRangesMatchThePostgresRoute(t *testing.T) {
	f := newDSLFixture(t, true)
	f.hotKeys = true
	texts := []string{"a", "B", "b", "Z", "é", "", "a b"}
	for _, attr := range []string{"code", "note"} {
		stored := map[uuid.UUID]string{}
		for _, v := range texts {
			var id uuid.UUID
			if attr == "code" {
				id = f.seed(t, map[string]any{"text_01": v})
			} else {
				id = f.seed(t, map[string]any{}, eavText(attr, v))
			}
			stored[id] = v
		}
		for _, operand := range []string{"B", "a", "Z", "é", "b"} {
			for op, holds := range map[string]func(int) bool{
				"gt": func(c int) bool { return c > 0 }, "gte": func(c int) bool { return c >= 0 },
				"lt": func(c int) bool { return c < 0 }, "lte": func(c int) bool { return c <= 0 },
			} {
				var want []uuid.UUID
				for id, v := range stored {
					if holds(strings.Compare(v, operand)) {
						want = append(want, id)
					}
				}
				require.Equal(t, sortedIDs(want...), f.match(t, kv(attr, op+":"+operand)), "%s %s:%s compares bytes", attr, op, operand)
			}
		}
	}

	for _, n := range []int{1, 2, 3} {
		f.seed(t, map[string]any{"integer_01": n, "bigint_01": int64(1704164645000 + n)},
			eavNum("weight", float64(n)+0.5), eavInt("big", int64(n)), eavNum("seen", float64(1704164645000+n)))
	}
	for _, c := range []struct{ attr, value string }{
		{"rank", "gt:1"}, {"rank", "lte:2"}, {"weight", "gte:2.5"}, {"weight", "lt:2"},
		{"big", "gt:2"}, {"due", "lt:2024-01-02T03:04:05.002Z"}, {"seen", "gte:1704164645002"},
	} {
		require.NotEmpty(t, f.match(t, kv(c.attr, c.value)), "%s %s", c.attr, c.value)
	}
}

// The hot keys are the live entity_main rows with an entry in the dirty
// window, flushed_at = 0 OR flushed_at >= cutoff, and ver_ts is the newest
// entry's changed_at there.
func TestHotKeysQueryReadsTheDirtyWindow(t *testing.T) {
	f := newDSLFixture(t, true)
	unflushed := f.seed(t, map[string]any{"text_01": "x"})
	f.logChange(t, unflushed, 5, 3)
	f.logChange(t, unflushed, 0, 10)
	marked := f.seed(t, map[string]any{"text_01": "x"})
	f.logChange(t, marked, 100, 7)
	clean := f.seed(t, map[string]any{"text_01": "y"})
	f.logChange(t, clean, 50, 4)
	f.seed(t, map[string]any{"text_01": "x"}) // never logged
	deleted := uuid.New()                     // a tombstone: no entity_main row
	f.logChange(t, deleted, 0, 12)

	require.Equal(t, sortedIDs(unflushed, marked), f.hotKeyIDs(t, nil, 100), "a mark at the cutoff is in the window")
	require.Equal(t, sortedIDs(unflushed), f.hotKeyIDs(t, nil, 101))
	require.Equal(t, sortedIDs(unflushed), f.hotKeyIDs(t, nil, math.MaxInt64), "the strict set")
	require.Equal(t, sortedIDs(unflushed, marked, clean), f.hotKeyIDs(t, nil, 0))
	require.Equal(t, sortedIDs(unflushed, marked), f.hotKeyIDs(t, kv("code", "equals:x"), 0))
	require.Equal(t, sortedIDs(clean), f.hotKeyIDs(t, kv("code", "equals:y"), 0))

	f.inSnapshot(t, func(tx pgx.Tx) {
		verTS := map[uuid.UUID]int64{}
		for _, k := range f.queryHotKeys(t, tx, nil, 0, "ver_ts") {
			verTS[k.RowID] = k.VerTS
			require.Equal(t, []any{k.VerTS}, k.Key)
		}
		require.Equal(t, map[uuid.UUID]int64{unflushed: 10, marked: 7, clean: 4}, verTS)
		verTS = map[uuid.UUID]int64{}
		for _, k := range f.queryHotKeys(t, tx, nil, 60, "ver_ts") {
			verTS[k.RowID] = k.VerTS
		}
		require.Equal(t, map[uuid.UUID]int64{unflushed: 10, marked: 7}, verTS)
	})
}

// A row whose newest entry in the window is a tombstone is not a hot key,
// even where entity_main still holds it, as the attach path's dedup and
// visibility filter drop it (#856). A tie on changed_at goes to the delete.
// A tombstone older than the row's newest entry, or outside the window,
// leaves the row hot.
func TestHotKeysQueryDropsARowWhoseNewestEntryIsATombstone(t *testing.T) {
	f := newDSLFixture(t, true)
	logTombstone := func(rowID uuid.UUID, flushedAt, changedAt int64) {
		t.Helper()
		_, err := f.pool.Exec(f.ctx, fmt.Sprintf(
			"INSERT INTO %s (schema_id, row_id, flushed_at, changed_at, deleted_at) VALUES ($1, $2, $3, $4, $4)",
			sanitizeIdentifier(f.tables.ChangeLog)), dslSchemaID, rowID, flushedAt, changedAt)
		require.NoError(t, err)
	}
	deletedLast := f.seed(t, map[string]any{"text_01": "x"})
	f.logChange(t, deletedLast, 5, 3)
	logTombstone(deletedLast, 0, 10)
	tied := f.seed(t, map[string]any{"text_01": "x"})
	f.logChange(t, tied, 0, 10)
	logTombstone(tied, 50, 10)
	recreated := f.seed(t, map[string]any{"text_01": "x"})
	logTombstone(recreated, 40, 5)
	f.logChange(t, recreated, 0, 9)
	flushedTombstone := f.seed(t, map[string]any{"text_01": "x"})
	f.logChange(t, flushedTombstone, 0, 7)
	logTombstone(flushedTombstone, 20, 8)

	require.Equal(t, sortedIDs(recreated, flushedTombstone), f.hotKeyIDs(t, nil, 30),
		"the tombstone marked below the cutoff is outside the window")
	require.Equal(t, sortedIDs(recreated), f.hotKeyIDs(t, nil, 0))
}

// Each sort key derives from the stored value as the export will hold it:
// system columns, bound columns through their encodings, EAV-only attributes
// through the #785 double decode and the export's MAX over their rows.
func TestHotKeysQueryDerivesSortKeys(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		t.Run(fmt.Sprintf("numeric_eav=%v", numeric), func(t *testing.T) {
			f := newDSLFixture(t, numeric)
			owner := "6F1C3C3E-1111-4111-8111-111111111111"
			full := f.seed(t, map[string]any{
				"text_01": "a", "integer_01": 7, "smallint_01": 2, "text_02": "1",
				"text_03": "2024-01-02T03:04:05.000Z", "uuid_01": owner,
			}, eavText("note", "m"), dslEAVRow{attr: "note", index: "1", text: "q"}, dslEAVRow{attr: "note", index: "2", text: "B"},
				eavNum("weight", 1.5), dslEAVRow{attr: "weight", index: "1", numeric: 2.5},
				eavInt("big", 9007199254740993), eavNum("soft", 0.75), eavNum("day", 1704153600000), eavText("ref", owner))
			empty := f.seed(t, map[string]any{})
			f.logChange(t, full, 0, 20)
			f.logChange(t, empty, 0, 21)

			columns := []string{"code", "rank", "flag", "flag_text", "due_iso", "owner", "NOTE", "weight", "big", "soft", "day", "ref", "Created_At", "deleted_ts", "ver_ts", "row_id"}
			keys := map[uuid.UUID][]any{}
			f.inSnapshot(t, func(tx pgx.Tx) {
				for _, k := range f.queryHotKeys(t, tx, nil, 0, columns...) {
					keys[k.RowID] = k.Key
				}
			})
			require.Equal(t, []any{
				"a", int64(7), true, true, int64(1704164645000), strings.ToLower(owner),
				"q", 2.5, int64(9007199254740992), true, int64(1704153600000), owner,
				int64(1), int64(0), int64(20), full,
			}, keys[full], "NOTE reaches note; the bigint reads its double image; an EAV uuid keeps its stored text")
			require.Equal(t, []any{
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				int64(1), int64(0), int64(21), empty,
			}, keys[empty])

			order := sortkey.Order{}
			for _, name := range columns {
				typ := sortkey.BigInt
				if system, ok := sortkey.SystemColumnName(name); ok && system == sortkey.ColumnRowID {
					typ = sortkey.UUID
				} else if !ok {
					meta, err := sortkey.ColumnAttribute(name, f.cache)
					require.NoError(t, err)
					typ, err = sortkey.AttributeType(meta)
					require.NoError(t, err)
				}
				order.Columns = append(order.Columns, sortkey.Column{Name: name, Type: typ})
			}
			for id, key := range keys {
				require.NoError(t, order.CheckKey(key), "%s: each key has its column's export type", id)
			}
		})
	}
}

func TestHotKeysQueryRefusesColumnsWithoutAKey(t *testing.T) {
	f := newDSLFixture(t, true)
	f.inSnapshot(t, func(tx pgx.Tx) {
		for column, want := range map[string]string{
			"labels":  "is a list",
			"missing": "neither a system column",
		} {
			_, err := f.repo.QueryHotKeys(f.ctx, tx, model.HotKeysQuery{
				Tables: f.tables, SchemaID: dslSchemaID, SortColumns: []string{column},
			})
			require.ErrorContains(t, err, want, column)
		}
	})
}

// The page read answers what Get answers for each row, in the order asked,
// from the snapshot the hot keys came from: a write committed after the
// snapshot began is invisible to both reads.
func TestReadHotRowsReadsThePassSnapshot(t *testing.T) {
	f := newDSLFixture(t, true)
	a := f.seed(t, map[string]any{"text_01": "a", "integer_01": 1}, eavText("note", "x"), eavNum("weight", 1.5))
	b := f.seed(t, map[string]any{"text_01": "b"})
	f.logChange(t, a, 0, 1)
	f.logChange(t, b, 0, 1)
	want := map[uuid.UUID]*model.PersistentRecord{}
	for _, id := range []uuid.UUID{a, b} {
		record, err := f.repo.GetPersistentRecord(f.ctx, f.tables, dslSchemaID, id)
		require.NoError(t, err)
		want[id] = record
	}

	f.inSnapshot(t, func(tx pgx.Tx) {
		require.Len(t, f.queryHotKeys(t, tx, nil, 0), 2)

		_, err := f.pool.Exec(f.ctx, fmt.Sprintf("UPDATE %s SET text_01 = 'changed' WHERE ltbase_row_id = $1", sanitizeIdentifier(f.tables.EntityMain)), a)
		require.NoError(t, err)
		late := f.seed(t, map[string]any{"text_01": "late"})
		f.logChange(t, late, 0, 2)

		require.Len(t, f.queryHotKeys(t, tx, nil, 0), 2, "a row created after the snapshot is not a hot key")
		records, err := f.repo.ReadHotRows(f.ctx, tx, f.tables, dslSchemaID, []uuid.UUID{b, a})
		require.NoError(t, err)
		require.Equal(t, []*model.PersistentRecord{want[b], want[a]}, records, "Get's reading, in the order asked, at the snapshot")

		_, err = f.repo.ReadHotRows(f.ctx, tx, f.tables, dslSchemaID, []uuid.UUID{a, late})
		require.ErrorContains(t, err, "is not in entity_main in the pass's snapshot")

		records, err = f.repo.ReadHotRows(f.ctx, tx, f.tables, dslSchemaID, nil)
		require.NoError(t, err)
		require.Empty(t, records)
	})
}
