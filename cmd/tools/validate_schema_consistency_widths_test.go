package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
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
			int16(100), int16(3), "-9007199254740992", "9007199254740992").
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

// eavIntegerRefused is the category every out-of-width EAV smallint/integer
// is reported under since #729, before its table name.
const eavIntegerRefused = "smallint/integer EAV values outside the declared range or non-integral, which the OLTP read refuses"

// TestValidateSchemaConsistencyFailsEveryEAVIntegerOutOfContract pins #729's
// classification: the OLTP read refuses an out-of-width EAV integer, so every
// such row is a failure whatever its flush state. With a cutover, a row last
// exported before it is told apart, since its parquet copy was cast at
// declared width and the federated route serves NULL or a rounded value for
// it until the rewrite's flush. A pending row and one flushed after the
// cutover carry no stale copy.
func TestValidateSchemaConsistencyFailsEveryEAVIntegerOutOfContract(t *testing.T) {
	rowC := uuid.New()
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowB, "", "1.5", true, int64(1000)).
		AddRow(int16(100), int16(2), rowC, "", "7.25", false, int64(6000)))

	var out strings.Builder
	validator := newWidthValidator(t, mock, &out, widthAuditOptions{cutoverMillis: 5000})
	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "3 issue(s)") {
		t.Fatalf("expected three failures, got %v", err)
	}
	got := out.String()
	for _, want := range []string{
		eavIntegerRefused + ", whose parquet copy also predates the #384 storage-width export, in eav_data_dev: " +
			"schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" + widthRowA.String() +
			" value=4294967296 last_flushed_at=1000",
		eavIntegerRefused + ", in eav_data_dev: schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" +
			widthRowB.String() + " value=1.5 last_flushed_at=1000",
		eavIntegerRefused + ", in eav_data_dev: schema=contact schema_id=100 attr_id=2 attribute=qty declared=integer row_id=" +
			rowC.String() + " value=7.25 last_flushed_at=6000",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if strings.Contains(got, "informational") {
		t.Fatalf("an out-of-width EAV integer is never informational, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsEAVIntegerWithoutCutover: before #729 an
// exported row with no cutover was an informational candidate and the
// pre-flight stayed green. The Postgres value is now the defect, which the
// cutover does not change, so it fails.
func TestValidateSchemaConsistencyFailsEAVIntegerWithoutCutover(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)))

	var out strings.Builder
	err := newWidthValidator(t, mock, &out, widthAuditOptions{}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one failure, got %v", err)
	}
	if got := out.String(); !strings.Contains(got, eavIntegerRefused+", in eav_data_dev: ") || strings.Contains(got, "informational") {
		t.Fatalf("expected a failure without a stale-copy note, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsBigIntOutOfContract: a bigint value past
// 2^53 was rounded on the write (#590) and one past int64 diverges on every
// DuckDB leg, never-exported rows included; a re-flush cannot repair either,
// so the class fails even without a cutover and is never requeued.
func TestValidateSchemaConsistencyFailsBigIntOutOfContract(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(3), widthRowA, "", "9007199254740994", true, int64(0)))

	var out strings.Builder
	err := newWidthValidator(t, mock, &out, widthAuditOptions{requeue: true}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one bigint failure, got %v", err)
	}
	want := "bigint EAV values outside ±2^53 (the float64-exact range) or non-integral in eav_data_dev: schema=contact schema_id=100 attr_id=3 attribute=big declared=bigint"
	if got := out.String(); !strings.Contains(got, want) || strings.Contains(got, "requeued for re-flush") {
		t.Fatalf("output missing %q or requeued a bigint row, got %q", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyRequeueFlagRequeuesNothing: the retired
// -requeue-stale-width-exports is still accepted, so scripts that pass it
// run, but it writes nothing and says why. A re-flush re-exports a value the
// OLTP read refuses (#729), so it would report a row repaired that is not;
// every row stays a failure. The mock fails any write it was not told to
// expect.
func TestValidateSchemaConsistencyRequeueFlagRequeuesNothing(t *testing.T) {
	mock := newWidthCensusMock(t, widthCensusRows().
		AddRow(int16(100), int16(2), widthRowA, "", "4294967296", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowA, "1", "2.5", false, int64(1000)).
		AddRow(int16(100), int16(2), widthRowB, "", "-4294967296", false, int64(1000)))

	var out strings.Builder
	err := newWidthValidator(t, mock, &out, widthAuditOptions{cutoverMillis: 5000, requeue: true}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "3 issue(s)") {
		t.Fatalf("expected every row as a failure, got %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "-requeue-stale-width-exports requeued nothing") || strings.Contains(got, "requeued for re-flush") {
		t.Fatalf("expected the retired flag's notice and no requeued row, got %q", got)
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
