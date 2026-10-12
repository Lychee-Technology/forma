package declaredindex

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var catalogColumns = []string{"relname", "indisvalid", "plain", "columns", "build_pid"}

// catalogRow is one pg_index row as catalogQuery returns it.
type catalogRow struct {
	name    string
	valid   bool
	plain   bool
	columns []string
	pid     int32
}

func row(ix Index, valid bool) catalogRow {
	return catalogRow{name: ix.Name, valid: valid, plain: true, columns: ix.Columns}
}

// baseline is what every entity_main carries before any declaration: the
// primary key and one of init-db's per-column indexes.
func baseline() []catalogRow {
	return []catalogRow{
		{name: "entity_main_pkey", valid: true, columns: []string{"ltbase_schema_id", "ltbase_row_id"}},
		{name: "entity_main_text_01_idx", valid: true, plain: true, columns: []string{"ltbase_schema_id", "ltbase_row_id", "text_01"}},
	}
}

func expectCatalog(mock pgxmock.PgxConnIface, rows ...catalogRow) {
	result := pgxmock.NewRows(catalogColumns)
	for _, r := range rows {
		result.AddRow(r.name, r.valid, r.plain, r.columns, r.pid)
	}
	mock.ExpectQuery(regexp.QuoteMeta("FROM pg_index i")).WithArgs(`"entity_main"`).WillReturnRows(result)
}

func newMock(t *testing.T) pgxmock.PgxConnIface {
	t.Helper()
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet(), "every expected statement ran, and no other")
		_ = mock.Close(context.Background())
	})
	return mock
}

func planOutbox(t *testing.T) []Index {
	t.Helper()
	planned, err := Plan("entity_main", []SchemaDeclarations{outbox()})
	require.NoError(t, err)
	return planned
}

func TestEnsure_BuildsMissingIndexesConcurrently(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	expectCatalog(mock, baseline()...)
	for _, ix := range planned {
		mock.ExpectExec(regexp.QuoteMeta(ix.CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	}
	after := baseline()
	for _, ix := range planned {
		after = append(after, row(ix, true))
	}
	expectCatalog(mock, after...)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.NoError(t, err)
	assert.Equal(t, planned, report.Created)
	assert.Empty(t, report.Present)
	assert.Empty(t, report.Unreferenced)
}

// A re-run with unchanged declarations reads the catalog once and issues no
// DDL: the mock fails on any statement not expected here.
func TestEnsure_RerunIssuesNoDDL(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	rows := baseline()
	for _, ix := range planned {
		rows = append(rows, row(ix, true))
	}
	expectCatalog(mock, rows...)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.NoError(t, err)
	assert.Equal(t, planned, report.Present)
	assert.Empty(t, report.Created)
}

// An index an interrupted build left invalid is neither rebuilt nor dropped:
// Ensure names it and the remediation, and fails.
func TestEnsure_InvalidIndexFailsWithRemediation(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	rows := append(baseline(), row(planned[0], false), row(planned[1], true), row(planned[2], true))
	expectCatalog(mock, rows...)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "declared index "+planned[0].Name+" on entity_main is invalid")
	assert.Contains(t, msg, "pg_index.indisvalid = false")
	assert.Contains(t, msg, `DROP INDEX CONCURRENTLY "`+planned[0].Name+`" and rerun`)
	assert.Equal(t, planned[1:], report.Present, "the valid ones are still reported")
}

func TestEnsure_IndexStillBeingBuiltNamesTheBackend(t *testing.T) {
	planned := planOutbox(t)[:1]
	mock := newMock(t)

	building := row(planned[0], false)
	building.pid = 4242
	expectCatalog(mock, append(baseline(), building)...)

	_, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), planned[0].Name+" on entity_main is still being built by backend pid 4242")
	assert.NotContains(t, err.Error(), "DROP INDEX", "an in-progress build must not be advised away")
}

func TestEnsure_MismatchedKeyFails(t *testing.T) {
	planned := planOutbox(t)[:1]
	mock := newMock(t)

	wrongKey := row(planned[0], true)
	wrongKey.columns = []string{"ltbase_schema_id", "text_09", "ltbase_row_id"}
	partial := row(planned[0], true)
	partial.plain = false

	expectCatalog(mock, append(baseline(), wrongKey)...)
	_, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has key (ltbase_schema_id, text_09, ltbase_row_id)")
	assert.Contains(t, err.Error(), "outbox_message.")
	assert.Contains(t, err.Error(), "init-db never replaces an index")

	expectCatalog(mock, append(baseline(), partial)...)
	_, err = Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partial, unique or over an expression")
}

// A declaration removed from every schema leaves its index in place: a valid
// one is reported unreferenced, an invalid one still fails the run. Neither is
// dropped, and indexes outside the declared namespace are not reported.
func TestEnsure_UnreferencedIndexesAreReportedNeverDropped(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	orphan := Index{Name: Name("entity_main", []string{"ltbase_schema_id", "uuid_02", "ltbase_row_id"}),
		Columns: []string{"ltbase_schema_id", "uuid_02", "ltbase_row_id"}}
	rows := append(baseline(), row(orphan, true))
	for _, ix := range planned {
		rows = append(rows, row(ix, true))
	}
	expectCatalog(mock, rows...)
	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.NoError(t, err)
	require.Len(t, report.Unreferenced, 1)
	assert.Equal(t, orphan.Name, report.Unreferenced[0].Name)
	assert.Equal(t, orphan.Columns, report.Unreferenced[0].Columns)

	expectCatalog(mock, append(baseline(), row(orphan, false))...)
	_, err = Ensure(context.Background(), mock, "entity_main", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), orphan.Name+" on entity_main is invalid")
}

// CREATE INDEX ... IF NOT EXISTS skips silently when another relation in the
// schema holds the name; Ensure notices the index is still not on the table.
func TestEnsure_NameHeldByAnotherRelationFails(t *testing.T) {
	planned := planOutbox(t)[:1]
	mock := newMock(t)

	expectCatalog(mock, baseline()...)
	mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	expectCatalog(mock, baseline()...)

	_, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "another relation in the same schema already holds the name")
}

// A failed build stops the run, naming the index and the remediation; the
// Report still lists what was built before it, and nothing after it is tried.
func TestEnsure_BuildFailureNamesIndexAndRemediation(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	expectCatalog(mock, baseline()...)
	mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	cause := errors.New("canceling statement due to user request")
	mock.ExpectExec(regexp.QuoteMeta(planned[1].CreateSQL())).WillReturnError(cause)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "build declared index "+planned[1].Name+" on entity_main for outbox_message.")
	assert.Contains(t, err.Error(), `DROP INDEX CONCURRENTLY "`+planned[1].Name+`"`)
	assert.Equal(t, planned[:1], report.Created)
	assert.Empty(t, report.Present)
}
