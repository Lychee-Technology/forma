package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Golden SQL of a claim's group head (#653) on the hybrid test schema: the
// predicate for one-key, two-key and two-order groups, and the candidate
// select it joins. Query's own SQL is pinned as well, from files rendered
// before the head renderer existed, so the renderer cannot change it.

var claimHeadTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

func headOrder(column string, order forma.SortOrder) model.AttributeOrder {
	return model.AttributeOrder{ColumnName: column, SortOrder: order, StorageLocation: forma.AttributeStorageLocationMain}
}

func TestClaimHeadPredicateGoldenSQL(t *testing.T) {
	live := &forma.KvCondition{Attr: "score", Value: "lt:5"}
	same := func(c string) string {
		return `(e."` + c + `" = m."` + c + `" OR (e."` + c + `" IS NULL AND m."` + c + `" IS NULL))`
	}
	ascBefore := func(c, then string) string {
		return `(e."` + c + `" < m."` + c + `" OR (e."` + c + `" IS NOT NULL AND m."` + c + `" IS NULL) OR (` + same(c) + ` AND ` + then + `))`
	}
	descBefore := func(c, then string) string {
		return `(e."` + c + `" > m."` + c + `" OR (e."` + c + `" IS NOT NULL AND m."` + c + `" IS NULL) OR (` + same(c) + ` AND ` + then + `))`
	}
	const rowID = `e.ltbase_row_id < m.ltbase_row_id`
	exists := `NOT EXISTS (SELECT 1 FROM "entity_main" e WHERE e.ltbase_schema_id = m.ltbase_schema_id AND `
	cases := []struct {
		name string
		head *model.GroupHead
		want string
	}{
		{"one key", &model.GroupHead{KeyColumns: []string{"integer_01"},
			Orders: []model.AttributeOrder{headOrder("bigint_01", forma.SortOrderAsc)}, Live: live},
			`(m."double_01" < $4) AND ` + exists + same("integer_01") + ` AND ` +
				ascBefore("bigint_01", rowID) + ` AND (e."double_01" < $4))`},
		{"two keys", &model.GroupHead{KeyColumns: []string{"integer_01", "uuid_01"},
			Orders: []model.AttributeOrder{headOrder("bigint_01", forma.SortOrderAsc)}, Live: live},
			`(m."double_01" < $4) AND ` + exists + same("integer_01") + ` AND ` + same("uuid_01") + ` AND ` +
				ascBefore("bigint_01", rowID) + ` AND (e."double_01" < $4))`},
		{"two orders, the second descending", &model.GroupHead{KeyColumns: []string{"integer_01"},
			Orders: []model.AttributeOrder{headOrder("bigint_01", forma.SortOrderAsc), headOrder("text_02", forma.SortOrderDesc)}, Live: live},
			`(m."double_01" < $4) AND ` + exists + same("integer_01") + ` AND ` +
				ascBefore("bigint_01", descBefore("text_02", rowID)) + ` AND (e."double_01" < $4))`},
	}
	h := newHybridTestHelper(true)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clause, args, next, err := h.repo.headPredicate(claimHeadTables, 1, tc.head, 3)
			require.NoError(t, err)
			assert.Equal(t, tc.want, clause)
			assert.Equal(t, []any{int64(5)}, args, "Live's values are bound once for m and e")
			assert.Equal(t, 4, next)
		})
	}
}

// statementCapture is a repository pool that records the first query it is
// sent and fails it, so a test can read the SQL a read path renders.
type statementCapture struct {
	sql  string
	args []any
}

var errStatementCaptured = errors.New("statement captured")

func (c *statementCapture) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.sql, c.args = sql, args
	return nil, errStatementCaptured
}

func (c *statementCapture) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("the test expects a Query")
}

func (c *statementCapture) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errStatementCaptured
}

// capturedQuerySQL renders query through QueryPersistentRecords and
// answers the SQL, whitespace collapsed, and its args.
func capturedQuerySQL(t *testing.T, query *model.PersistentRecordQuery) (string, []any) {
	t.Helper()
	capture := &statementCapture{}
	repo := NewDBPersistentRecordRepository(capture, newHybridTestHelper(true).repo.metadataCache)
	_, err := repo.QueryPersistentRecords(context.Background(), query)
	require.ErrorIs(t, err, errStatementCaptured)
	return strings.Join(strings.Fields(capture.sql), " "), capture.args
}

func goldenSQL(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "claim_head", name+".sql"))
	require.NoError(t, err)
	return strings.TrimSpace(string(data))
}

func headQuery(cond forma.Condition, head *model.GroupHead) *model.PersistentRecordQuery {
	return &model.PersistentRecordQuery{Tables: claimHeadTables, SchemaID: 1, Condition: cond, Limit: 6, Head: head,
		AttributeOrders: []model.AttributeOrder{{AttrID: 10, ValueType: forma.ValueTypeDateTime, SortOrder: forma.SortOrderAsc,
			StorageLocation: forma.AttributeStorageLocationMain, ColumnName: "bigint_01", AttrName: "created"}}}
}

// Query's SQL is what it was before group heads: the files were rendered by
// the commit before them.
func TestQuerySQLIsUnchangedByTheHeadRenderer(t *testing.T) {
	mainAnchor := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:3"}, &forma.KvCondition{Attr: "name", Value: "equals:x"}}}
	sql, _ := capturedQuerySQL(t, headQuery(mainAnchor, nil))
	assert.Equal(t, goldenSQL(t, "query_main_anchor"), sql)
	sql, _ = capturedQuerySQL(t, headQuery(&forma.KvCondition{Attr: "name", Value: "equals:x"}, nil))
	assert.Equal(t, goldenSQL(t, "query_eav_anchor"), sql)
}

// A claim's candidate select is Query's SQL with the head predicate joined
// to the condition, in the anchor and in its change_log branch, and anchors
// on entity_main even when the condition alone tests only EAV attributes.
func TestClaimCandidateSelectJoinsTheHeadToQuerySQL(t *testing.T) {
	head := &model.GroupHead{KeyColumns: []string{"integer_01"},
		Orders: []model.AttributeOrder{headOrder("bigint_01", forma.SortOrderAsc)},
		Live:   &forma.KvCondition{Attr: "score", Value: "lt:5"}}
	query := goldenSQL(t, "query_eav_anchor")
	sql, args := capturedQuerySQL(t, headQuery(&forma.KvCondition{Attr: "name", Value: "equals:x"}, head))

	predicate, _, _, err := newHybridTestHelper(true).repo.headPredicate(claimHeadTables, 1, head, 3)
	require.NoError(t, err)
	condition, _, _, err := newHybridTestHelper(true).repo.renderHybrid(claimHeadTables, 1, &forma.KvCondition{Attr: "name", Value: "equals:x"}, 1, "m")
	require.NoError(t, err)
	joined := "((" + condition + ") AND " + predicate + ")"
	assert.Equal(t, 2, strings.Count(sql, joined), "the anchor and its change_log branch both test the head")
	assert.Contains(t, sql, `FROM "entity_main" m WHERE m.ltbase_schema_id = $1 AND `+joined)
	assert.NotContains(t, sql, "SELECT DISTINCT t.row_id", "the query anchors on entity_main")
	assert.NotEqual(t, query, sql)
	assert.Equal(t, []any{int16(1), int16(7), "x", int64(5), 6, 0}, args)
}
