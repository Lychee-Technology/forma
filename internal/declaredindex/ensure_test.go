package declaredindex

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var catalogColumns = []string{"relname", "indisvalid", "columns", "deviations", "build_pid"}

// catalogRow is one pg_index row as catalogQuery returns it.
type catalogRow struct {
	name       string
	valid      bool
	columns    []string
	deviations []string
	pid        int32
}

func row(ix Index, valid bool) catalogRow {
	return catalogRow{name: ix.Name, valid: valid, columns: ix.Columns}
}

// baseline is what every entity_main carries before any declaration: the
// primary key and one of init-db's per-column indexes.
func baseline() []catalogRow {
	return []catalogRow{
		{name: "entity_main_pkey", valid: true, columns: []string{"ltbase_schema_id", "ltbase_row_id"}, deviations: []string{"unique"}},
		{name: "entity_main_text_01_idx", valid: true, columns: []string{"ltbase_schema_id", "ltbase_row_id", "text_01"}},
	}
}

// expectSchema expects Ensure's lookup of the PostgreSQL schema table
// resolves to.
func expectSchema(mock pgxmock.PgxConnIface, table, schema string) {
	mock.ExpectQuery(regexp.QuoteMeta("JOIN pg_namespace n")).WithArgs(sqlutil.SanitizeIdentifier(table)).
		WillReturnRows(pgxmock.NewRows([]string{"nspname"}).AddRow(schema))
}

func expectCatalog(mock pgxmock.PgxConnIface, rows ...catalogRow) {
	expectCatalogOn(mock, "entity_main", rows...)
}

func expectCatalogOn(mock pgxmock.PgxConnIface, table string, rows ...catalogRow) {
	result := pgxmock.NewRows(catalogColumns)
	for _, r := range rows {
		result.AddRow(r.name, r.valid, r.columns, r.deviations, r.pid)
	}
	mock.ExpectQuery(regexp.QuoteMeta("FROM pg_index i")).WithArgs(sqlutil.SanitizeIdentifier(table)).WillReturnRows(result)
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

	expectSchema(mock, "entity_main", "public")
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
	expectSchema(mock, "entity_main", "public")
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
	expectSchema(mock, "entity_main", "public")
	expectCatalog(mock, rows...)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "declared index "+planned[0].Name+" on entity_main is invalid")
	assert.Contains(t, msg, "pg_index.indisvalid = false")
	assert.Contains(t, msg, `DROP INDEX CONCURRENTLY "public"."`+planned[0].Name+`" and rerun`)
	assert.Equal(t, planned[1:], report.Present, "the valid ones are still reported")
}

func TestEnsure_IndexStillBeingBuiltNamesTheBackend(t *testing.T) {
	planned := planOutbox(t)[:1]
	mock := newMock(t)

	building := row(planned[0], false)
	building.pid = 4242
	expectSchema(mock, "entity_main", "public")
	expectCatalog(mock, append(baseline(), building)...)

	_, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), planned[0].Name+" on entity_main is still being built by backend pid 4242")
	assert.NotContains(t, err.Error(), "DROP INDEX", "an in-progress build must not be advised away")
}

// An index under the planned name that is not exactly what CreateSQL builds
// fails the run, whether its key differs or only its shape does: a BRIN index
// over the planned key would otherwise pass as present while serving none of
// the btree access paths the declaration promises.
func TestEnsure_MismatchedIndexFails(t *testing.T) {
	planned := planOutbox(t)[:1]
	key := "(" + strings.Join(planned[0].Columns, ", ") + ")"
	cases := []struct {
		name       string
		columns    []string
		deviations []string
		want       string
	}{
		{"different key", []string{"ltbase_schema_id", "text_09", "ltbase_row_id"}, nil,
			"has key (ltbase_schema_id, text_09, ltbase_row_id), but outbox_message."},
		{"same key, another access method", planned[0].Columns, []string{"brin access method"},
			"has key " + key + " and is not a plain btree (brin access method), but outbox_message."},
		{"key moved under INCLUDE", planned[0].Columns[:2], []string{"INCLUDE columns"},
			"has key (" + strings.Join(planned[0].Columns[:2], ", ") + ") and is not a plain btree (INCLUDE columns)"},
		{"several deviations", planned[0].Columns, []string{"unique", "partial", "non-default collation"},
			"and is not a plain btree (unique, partial, non-default collation)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMock(t)
			existing := row(planned[0], true)
			existing.columns, existing.deviations = tc.columns, tc.deviations
			expectSchema(mock, "entity_main", "public")
			expectCatalog(mock, append(baseline(), existing)...)

			report, err := Ensure(context.Background(), mock, "entity_main", planned)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "as a plain btree index; init-db never replaces an index")
			assert.Contains(t, err.Error(), `DROP INDEX CONCURRENTLY "public"."`+planned[0].Name+`"`)
			assert.Empty(t, report.Present, "a mismatched index is never reported present")
		})
	}
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
	expectSchema(mock, "entity_main", "public")
	expectCatalog(mock, rows...)
	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.NoError(t, err)
	require.Len(t, report.Unreferenced, 1)
	assert.Equal(t, orphan.Name, report.Unreferenced[0].Name)
	assert.Equal(t, orphan.Columns, report.Unreferenced[0].Columns)

	expectSchema(mock, "entity_main", "public")
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

	expectSchema(mock, "entity_main", "public")
	expectCatalog(mock, baseline()...)
	mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	expectCatalog(mock, baseline()...)

	_, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "another relation in schema public already holds the name")
}

// A failed build stops the run, naming the index and the remediation; the
// Report still lists what was built before it, and nothing after it is tried.
func TestEnsure_BuildFailureNamesIndexAndRemediation(t *testing.T) {
	planned := planOutbox(t)
	mock := newMock(t)

	expectSchema(mock, "entity_main", "public")
	expectCatalog(mock, baseline()...)
	mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	cause := errors.New("canceling statement due to user request")
	mock.ExpectExec(regexp.QuoteMeta(planned[1].CreateSQL())).WillReturnError(cause)

	report, err := Ensure(context.Background(), mock, "entity_main", planned)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "build declared index "+planned[1].Name+" on entity_main for outbox_message.")
	assert.Contains(t, err.Error(), `DROP INDEX CONCURRENTLY "public"."`+planned[1].Name+`"`)
	assert.Equal(t, planned[:1], report.Created)
	assert.Empty(t, report.Present)
}

// Every remedy names the index qualified by the schema its table resolved to,
// because PostgreSQL creates an index in its table's schema and an unqualified
// DROP INDEX resolves through the session's search_path, which need not
// include it. The schema comes from the catalog, not from the configured name:
// an unqualified table may resolve anywhere on init-db's search_path.
func TestEnsure_RemediesQualifyTheIndexWithItsTableSchema(t *testing.T) {
	targets := []struct{ table, schema string }{
		{"tenant.entity_main", "tenant"},
		{"entity_main", "app"},
	}
	for _, tc := range targets {
		planned, err := Plan(tc.table, []SchemaDeclarations{outbox()})
		require.NoError(t, err)
		drop := func(ix Index) string {
			return `DROP INDEX CONCURRENTLY "` + tc.schema + `"."` + ix.Name + `" and rerun`
		}
		t.Run(tc.table+"/invalid and mismatched", func(t *testing.T) {
			mock := newMock(t)
			mismatched := row(planned[1], true)
			mismatched.deviations = []string{"brin access method"}
			expectSchema(mock, tc.table, tc.schema)
			expectCatalogOn(mock, tc.table, row(planned[0], false), mismatched, row(planned[2], true))

			_, err := Ensure(context.Background(), mock, tc.table, planned)
			require.Error(t, err)
			assert.Contains(t, err.Error(), planned[0].Name+" on "+tc.table+" is invalid")
			assert.Contains(t, err.Error(), drop(planned[0]))
			assert.Contains(t, err.Error(), "is not a plain btree (brin access method)")
			assert.Contains(t, err.Error(), drop(planned[1]))
			assert.NotContains(t, err.Error(), `CONCURRENTLY "`+planned[0].Name+`"`)
		})
		t.Run(tc.table+"/failed build", func(t *testing.T) {
			mock := newMock(t)
			expectSchema(mock, tc.table, tc.schema)
			expectCatalogOn(mock, tc.table)
			mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnError(errors.New("canceling statement due to user request"))

			report, err := Ensure(context.Background(), mock, tc.table, planned)
			require.Error(t, err)
			assert.Contains(t, err.Error(), drop(planned[0]))
			assert.Equal(t, tc.schema, report.Schema)
		})
		t.Run(tc.table+"/name taken", func(t *testing.T) {
			mock := newMock(t)
			expectSchema(mock, tc.table, tc.schema)
			expectCatalogOn(mock, tc.table)
			mock.ExpectExec(regexp.QuoteMeta(planned[0].CreateSQL())).WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
			expectCatalogOn(mock, tc.table)

			_, err := Ensure(context.Background(), mock, tc.table, planned[:1])
			require.Error(t, err)
			assert.Contains(t, err.Error(), "another relation in schema "+tc.schema+" already holds the name")
		})
	}
}
