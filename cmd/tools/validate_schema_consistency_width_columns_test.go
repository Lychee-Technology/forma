package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// boundBigintSchemaDir writes a contact schema with an EAV-only bigint, big
// (id 2), a bigint bound to double_01, ratio (id 3), and an integer bound to
// bigint_01, qty (id 4).
func boundBigintSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "contact",
		`{"type":"object","properties":{"id":{"type":"string"},"big":{"type":"integer"},"ratio":{"type":"integer"},"qty":{"type":"integer"}}}`,
		`{"id":{"attributeID":1,"valueType":"text"},"big":{"attributeID":2,"valueType":"bigint"},`+
			`"ratio":{"attributeID":3,"valueType":"bigint","column_binding":{"col_name":"double_01"}},`+
			`"qty":{"attributeID":4,"valueType":"integer","column_binding":{"col_name":"bigint_01"}}}`)
	return dir
}

func columnCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows(strings.Fields("ltbase_schema_id attr_id ltbase_row_id image exact pending last_flushed_at"))
}

func ptr[T any](v T) *T { return &v }

// boundBigintEAVCensusArgs are the eav_data census targets of
// boundBigintSchemaDir: big, and the eav_data rows ratio and qty may have
// kept from before their promotion, each at its declared width (#729).
var boundBigintEAVCensusArgs = []any{
	int16(100), int16(2), "-9007199254740992", "9007199254740992",
	int16(100), int16(3), "-9007199254740992", "9007199254740992",
	int16(100), int16(4), "-2147483648", "2147483647",
}

// newBoundBigintMock wires validator.run's five fixed queries and an empty
// eav_data census over big and the retained rows of ratio and qty, then the
// entity_main census over ratio (#618) and qty (#708) returning census.
func newBoundBigintMock(t *testing.T, census *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	return newBoundBigintMockWithEAV(t, widthCensusRows(), census)
}

func newBoundBigintMockWithEAV(t *testing.T, eav, census *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock := newSchemaConsistencyMock(t, attrCensusRows())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, e\.row_id, e\.array_indices, e\.value_numeric::text`).
		WithArgs(boundBigintEAVCensusArgs...).
		WillReturnRows(eav)
	mock.ExpectQuery(`SELECT m\.ltbase_schema_id, w\.attr_id, m\.ltbase_row_id, v\.image,[\s\S]*FROM "entity_main_dev" AS m`).
		WithArgs(int16(100), int16(3), "double_01", int64(-9007199254740992), int64(9007199254740992),
			int16(100), int16(4), "bigint_01", int64(-2147483648), int64(2147483647)).
		WillReturnRows(census)
	return mock
}

func newBoundBigintValidator(t *testing.T, mock pgxmock.PgxPoolIface, out *strings.Builder, widths widthAuditOptions) schemaConsistencyValidator {
	t.Helper()
	validator := newWidthValidator(t, mock, out, widths)
	validator.schemaDir = boundBigintSchemaDir(t)
	return validator
}

// TestValidateSchemaConsistencyFailsBoundBigIntOutOfContract pins #618: a
// bigint bound to a double_* column is stored as its float64 image, so a
// value past ±2^53 or one that names no int64 is the same failure the EAV
// bigint class is. The line names the schema, the row, the attribute and
// the column. A re-flush cannot repair it, so it is never requeued.
func TestValidateSchemaConsistencyFailsBoundBigIntOutOfContract(t *testing.T) {
	mock := newBoundBigintMock(t, columnCensusRows().
		AddRow(int16(100), int16(3), widthRowA, ptr(float64(9007199254740994)), (*int64)(nil), false, int64(0)).
		AddRow(int16(100), int16(3), widthRowB, ptr(1000.5), (*int64)(nil), false, int64(1700)))

	var out strings.Builder
	err := newBoundBigintValidator(t, mock, &out, widthAuditOptions{requeue: true}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "2 issue(s)") {
		t.Fatalf("expected two bound-bigint failures, got %v", err)
	}
	const category = "bigint values in double columns outside ±2^53 (the float64-exact range) or non-integral in entity_main_dev: " +
		"schema=contact schema_id=100 attr_id=3 attribute=ratio declared=bigint row_id="
	got := out.String()
	for _, want := range []string{
		category + widthRowA.String() + " column=double_01 value=9007199254740994\n",
		category + widthRowB.String() + " column=double_01 value=1000.5 last_flushed_at=1700\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if strings.Contains(got, "requeued for re-flush") || strings.Contains(got, "informational") {
		t.Fatalf("a bound bigint finding must fail and never requeue, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsBoundIntegerOutOfDeclaredType pins
// #708: an integer bound to a bigint_* column holding a value past int32 is
// refused by the OLTP read, so it is a failure. The line names the schema,
// the row, the attribute and the column. A re-flush reads the column the
// same way, so it is never requeued, whatever the cutover.
func TestValidateSchemaConsistencyFailsBoundIntegerOutOfDeclaredType(t *testing.T) {
	mock := newBoundBigintMock(t, columnCensusRows().
		AddRow(int16(100), int16(4), widthRowA, (*float64)(nil), ptr(int64(4294967296)), false, int64(1700)))

	var out strings.Builder
	err := newBoundBigintValidator(t, mock, &out, widthAuditOptions{requeue: true, cutoverMillis: 1 << 40}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one bound-integer failure, got %v", err)
	}
	const want = "smallint/integer values in wider bound columns outside the declared range or non-integral, which the OLTP read refuses, " +
		"in entity_main_dev: schema=contact schema_id=100 attr_id=4 attribute=qty declared=integer row_id="
	got := out.String()
	if !strings.Contains(got, want+widthRowA.String()+" column=bigint_01 value=4294967296 last_flushed_at=1700\n") {
		t.Fatalf("output missing the bound-integer line, got %q", got)
	}
	if strings.Contains(got, "requeued for re-flush") || strings.Contains(got, "informational") {
		t.Fatalf("a bound integer finding must fail and never requeue, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyPassesBoundBigIntInContract: the census reads
// the column and finds nothing to report, so the pre-flight is green.
func TestValidateSchemaConsistencyPassesBoundBigIntInContract(t *testing.T) {
	mock := newBoundBigintMock(t, columnCensusRows())

	var out strings.Builder
	if err := newBoundBigintValidator(t, mock, &out, widthAuditOptions{}).run(context.Background()); err != nil {
		t.Fatalf("a clean census must pass: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "schema consistency checks passed for 1 schema(s)") {
		t.Fatalf("expected a passing report, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyBoundBigIntNeedsEntityMainTable: with
// -entity-main-table emptied the column cannot be read. The run must fail
// naming the attribute, not pass over rows it never scanned.
func TestValidateSchemaConsistencyBoundBigIntNeedsEntityMainTable(t *testing.T) {
	mock := newSchemaConsistencyMock(t, attrCensusRows())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, e\.row_id, e\.array_indices, e\.value_numeric::text`).
		WithArgs(boundBigintEAVCensusArgs...).
		WillReturnRows(widthCensusRows())

	var out strings.Builder
	validator := newBoundBigintValidator(t, mock, &out, widthAuditOptions{})
	validator.widths.entityMainTable = ""
	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "integer-width census: column-bound attributes: no entity main table is configured") ||
		!strings.Contains(err.Error(), "schema=contact attribute=ratio column=double_01") {
		t.Fatalf("expected a census error naming the bound attribute, got %v", err)
	}
	if got := out.String(); strings.Contains(got, "passed") {
		t.Fatalf("the run must not report a pass, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsRetainedEAVInteger: an eav_data row a
// column-bound integer kept from before its promotion is decoded under the
// declared type, so the OLTP read refuses it when the type cannot hold it
// (#729). The census reports it as an eav_data value, naming no column.
func TestValidateSchemaConsistencyFailsRetainedEAVInteger(t *testing.T) {
	mock := newBoundBigintMockWithEAV(t,
		widthCensusRows().AddRow(int16(100), int16(4), widthRowA, "", "1001.5", false, int64(0)),
		columnCensusRows())

	var out strings.Builder
	err := newBoundBigintValidator(t, mock, &out, widthAuditOptions{}).run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one retained-row failure, got %v", err)
	}
	want := eavIntegerRefused + ", in eav_data_dev: schema=contact schema_id=100 attr_id=4 attribute=qty declared=integer row_id=" +
		widthRowA.String() + " value=1001.5"
	if got := out.String(); !strings.Contains(got, want) || strings.Contains(got, "column=") {
		t.Fatalf("output missing %q or names a column, got %q", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}
