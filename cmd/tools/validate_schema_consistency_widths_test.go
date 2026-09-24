package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

var (
	widthRowA = uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	widthRowB = uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
)

// widthSchemaDir writes a contact schema with an EAV-only integer, qty (id 2),
// and an EAV-only bigint, big (id 3).
func widthSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "contact",
		`{"type":"object","properties":{"id":{"type":"string"},"qty":{"type":"integer"},"big":{"type":"integer"}}}`,
		`{"id":{"attributeID":1,"valueType":"text"},"qty":{"attributeID":2,"valueType":"integer"},"big":{"attributeID":3,"valueType":"bigint"}}`)
	return dir
}

// newWidthCensusMock wires validator.run's five fixed queries with empty
// results, then the integer-width census (#501) returning census.
func newWidthCensusMock(t *testing.T, census *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock := newSchemaConsistencyMock(t, attrCensusRows())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, e\.row_id, e\.array_indices, e\.value_numeric::text`).
		WithArgs(int16(100), int16(2), "-2147483648", "2147483647",
			int16(100), int16(3), "-9223372036854775808", "9223372036854775807").
		WillReturnRows(census)
	return mock
}

func widthCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows(strings.Fields("schema_id attr_id row_id array_indices value_numeric pending last_flushed_at"))
}

func newWidthValidator(t *testing.T, mock pgxmock.PgxPoolIface, out *strings.Builder, widths widthAuditOptions) schemaConsistencyValidator {
	t.Helper()
	if widths.changeLogTable == "" {
		widths.changeLogTable = "change_log_dev"
	}
	widths.entityMainTable = "entity_main_dev"
	return schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   widthSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		widths:      widths,
		out:         out,
	}
}

// TestValidateSchemaConsistencyFailsStaleWidthExport pins #501's detection:
// with a cutover, an out-of-width integer row last exported before it has a
// parquet copy cast at declared width, so the federated route serves NULL or
// a rounded value. That is a failure naming the row. A pending row is served
// hot and a row flushed after the cutover was re-exported at storage width,
// so neither is reported.
func TestValidateSchemaConsistencyFailsStaleWidthExport(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowB, "", "1.5", true, int64(1000)).
		AddRow(int16(100), int16(2), uuid.New(), "", "7.25", false, int64(6000)))

	var out strings.Builder
	validator := newWidthValidator(t, mock, &out, widthAuditOptions{cutoverMillis: 5000})
	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one stale-export failure, got %v", err)
	}
	want := "EAV integer values whose parquet copy predates the #384 storage-width export in eav_data_dev: " +
		"schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" + widthRowA.String() +
		" value=4294967296 last_flushed_at=1000"
	if got := out.String(); !strings.Contains(got, want) || strings.Contains(got, "informational") {
		t.Fatalf("output missing %q or carries notices, got %q", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyReportsWidthCandidatesWithoutCutover: with no
// cutover, the validator cannot tell a pre-#384 export from a later one, so an
// exported out-of-width row is informational and the pre-flight stays green.
func TestValidateSchemaConsistencyReportsWidthCandidatesWithoutCutover(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)))

	var out strings.Builder
	if err := newWidthValidator(t, mock, &out, widthAuditOptions{}).run(context.Background()); err != nil {
		t.Fatalf("a candidate must not fail the pre-flight: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "1 informational finding(s)") || !strings.Contains(got, "pass -width-export-cutover to confirm") {
		t.Fatalf("expected an informational candidate, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsBigIntOutOfContract: a bigint value past
// int64 diverges on every DuckDB leg, never-exported rows included, and a
// re-flush cannot repair it, so it fails even without a cutover and is never
// requeued.
func TestValidateSchemaConsistencyFailsBigIntOutOfContract(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(3), widthRowA, "", "9223372036854775808", true, int64(0)))

	var out strings.Builder
	err := newWidthValidator(t, mock, &out, widthAuditOptions{requeue: true}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one bigint failure, got %v", err)
	}
	want := "bigint EAV values outside int64 or non-integral in eav_data_dev: schema=contact schema_id=100 attr_id=3 attribute=big declared=bigint"
	if got := out.String(); !strings.Contains(got, want) || strings.Contains(got, "requeued") {
		t.Fatalf("output missing %q or requeued a bigint row, got %q", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// expectWidthRequeue wires one RequeueForFlush transaction for rowID.
func expectWidthRequeue(mock pgxmock.PgxPoolIface, rowID uuid.UUID) {
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`UPDATE "entity_main_dev" SET ltbase_updated_at = GREATEST`).
		WithArgs(pgxmock.AnyArg(), int16(100), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at", "ltbase_deleted_at"}).AddRow(int64(9000), (*int64)(nil)))
	mock.ExpectExec(`INSERT INTO "change_log_dev"`).
		WithArgs(int16(100), rowID, int64(0), int64(9000), nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
}

// TestValidateSchemaConsistencyRequeuesStaleWidthExports pins the repair:
// each row with a stale export is requeued once, however many of its
// elements are out of width. Requeued rows report as informational, so a
// repair run exits green. A row whose entity_main row is gone cannot be
// requeued and stays a failure without stopping the sweep.
func TestValidateSchemaConsistencyRequeuesStaleWidthExports(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowA, "1", "2.5", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowB, "", "-4294967296", false, int64(1000)))
	expectWidthRequeue(mock, widthRowA)
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`UPDATE "entity_main_dev"`).WithArgs(pgxmock.AnyArg(), int16(100), widthRowB).
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectRollback()

	var out strings.Builder
	err := newWidthValidator(t, mock, &out, widthAuditOptions{cutoverMillis: 5000, requeue: true}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected the missing row as the only failure, got %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"requeued 1 row(s) for re-flush",
		"EAV rows with no entity_main row, which cannot be requeued, in eav_data_dev: schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" + widthRowB.String(),
		"EAV rows requeued for re-flush under the #384 storage-width export in eav_data_dev: schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" + widthRowA.String() + " array_indices=1 value=2.5",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

func TestWidthAuditFlagsRejectBadInput(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-width-export-cutover", "2026-08-29"}, "invalid -width-export-cutover"},
		{[]string{"-change-log-table", "", "-requeue-stale-width-exports"}, "-requeue-stale-width-exports needs -change-log-table"},
		{[]string{"-entity-main-table", "", "-requeue-stale-width-exports"}, "-requeue-stale-width-exports needs -entity-main-table"},
	} {
		args := append([]string{"-schema-dir", t.TempDir(), "-schema-registry-table", "schema_registry_dev"}, tc.args...)
		err := runValidateSchemaConsistencyOut(context.Background(), args, &strings.Builder{})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: expected %q, got %v", tc.args, tc.want, err)
		}
	}
}

// A deployment without CDC sets CHANGE_LOG_TABLE= (the Make target forwards
// it as ""); the empty value must disable the census's change_log lookup, not
// fall back to the default table.
func TestWidthAuditFlagsHonorEmptyChangeLogTableEnv(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  bool
		env  string
		want string
	}{
		{"unset takes default", false, "", "change_log_dev"},
		{"explicit empty disables", true, "", ""},
		{"explicit name", true, "change_log_prod", "change_log_prod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CHANGE_LOG_TABLE", tc.env)
			if !tc.set {
				if err := os.Unsetenv("CHANGE_LOG_TABLE"); err != nil {
					t.Fatalf("failed to unset CHANGE_LOG_TABLE: %v", err)
				}
			}
			flags := flag.NewFlagSet("t", flag.ContinueOnError)
			opts, err := registerWidthAuditFlags(flags).options()
			if err != nil {
				t.Fatalf("options: %v", err)
			}
			if opts.changeLogTable != tc.want {
				t.Fatalf("expected change_log table %q, got %q", tc.want, opts.changeLogTable)
			}
		})
	}
}

func TestParseWidthExportCutover(t *testing.T) {
	got, err := parseWidthExportCutover("1970-01-01T00:00:05Z")
	if err != nil || got != 5000 {
		t.Fatalf("expected 5000ms, got %d, %v", got, err)
	}
	if got, err := parseWidthExportCutover(""); err != nil || got != 0 {
		t.Fatalf("empty cutover must mean unknown, got %d, %v", got, err)
	}
}
