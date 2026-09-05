package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
)

func TestRunValidateSchemaConsistencyHelpFlag(t *testing.T) {
	if err := runValidateSchemaConsistency(context.Background(), []string{"-h"}); err != nil {
		t.Fatalf("expected no error with -h flag, got %v", err)
	}
}

func TestRunValidateSchemaConsistencyRequiresSchemaRegistryPair(t *testing.T) {
	err := runValidateSchemaConsistency(context.Background(), []string{"-schema-dir", "/tmp/schemas"})
	if err == nil || !strings.Contains(err.Error(), "--schema-registry-table is required") {
		t.Fatalf("expected schema registry pair validation error, got %v", err)
	}
}

func TestValueTypeUsesNumericColumn(t *testing.T) {
	tests := []struct {
		name      string
		valueType forma.ValueType
		want      bool
	}{
		{name: "numeric", valueType: forma.ValueTypeNumeric, want: true},
		{name: "bool", valueType: forma.ValueTypeBool, want: true},
		{name: "text", valueType: forma.ValueTypeText, want: false},
		{name: "uuid", valueType: forma.ValueTypeUUID, want: false},
		{name: "unknown defaults to text", valueType: forma.ValueType("custom_future_type"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valueTypeUsesNumericColumn(tt.valueType); got != tt.want {
				t.Fatalf("valueTypeUsesNumericColumn(%q)=%v want %v", tt.valueType, got, tt.want)
			}
		})
	}
}

// TestAttributeUsesNumericColumn_ListClassifiedByItemsType: list attrs store
// one element per row typed by items_type, so the storage-column check must
// classify them by the element type — numeric-item lists live in
// value_numeric, default (text) items in value_text (#204).
func TestAttributeUsesNumericColumn_ListClassifiedByItemsType(t *testing.T) {
	tests := []struct {
		name string
		meta forma.AttributeMetadata
		want bool
	}{
		{name: "list default items -> text backed",
			meta: forma.AttributeMetadata{ValueType: forma.ValueTypeList}, want: false},
		{name: "list integer items -> numeric backed",
			meta: forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger}, want: true},
		{name: "list bool items -> numeric backed",
			meta: forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBool}, want: true},
		{name: "scalar text unchanged",
			meta: forma.AttributeMetadata{ValueType: forma.ValueTypeText}, want: false},
		{name: "scalar numeric unchanged",
			meta: forma.AttributeMetadata{ValueType: forma.ValueTypeNumeric}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := attributeUsesNumericColumn(tt.meta); got != tt.want {
				t.Fatalf("attributeUsesNumericColumn(%+v)=%v want %v", tt.meta, got, tt.want)
			}
		})
	}
}

func TestValidateSchemaConsistencyReportsSuccess(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	defer mock.Close()

	schemaDir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, schemaDir, "contact", `{"type":"object","properties":{"id":{"type":"string"}}}`, `{"id":{"attributeID":1,"valueType":"text"}}`)

	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("contact", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = ''`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   schemaDir,
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	if err := validator.run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "schema consistency checks passed") {
		t.Fatalf("expected success output, got %q", out.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

func TestValidateSchemaConsistencyReportsIssues(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	defer mock.Close()

	schemaDir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, schemaDir, "contact", `{"type":"object","properties":{"id":{"type":"string"},"score":{"type":"number"}}}`, `{"id":{"attributeID":1,"valueType":"text"},"score":{"attributeID":2,"valueType":"numeric"}}`)

	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("contact", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).AddRow(int16(100), int16(99), int64(2)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).AddRow(int16(100), int16(2), int64(3)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = ''`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   schemaDir,
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err = validator.run(context.Background())
	if err == nil {
		t.Fatal("expected validation failure")
	}
	if !strings.Contains(err.Error(), "2 issue(s)") {
		t.Fatalf("expected issue count in error, got %v", err)
	}
	if !strings.Contains(out.String(), "unknown attribute IDs in eav_data_dev") {
		t.Fatalf("expected unknown attribute issue output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "numeric/date/bool attributes stored in value_text") {
		t.Fatalf("expected storage mismatch output, got %q", out.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

func TestValidateSchemaConsistencyBuildPoolFailure(t *testing.T) {
	oldBuildPool := buildToolPostgresPoolFn
	oldValidate := runValidateSchemaConsistencyOutFn
	defer func() {
		buildToolPostgresPoolFn = oldBuildPool
		runValidateSchemaConsistencyOutFn = oldValidate
	}()

	buildToolPostgresPoolFn = func(ctx context.Context, cfg forma.DatabaseConfig) (toolDBPool, error) {
		return nil, errors.New("boom")
	}
	runValidateSchemaConsistencyOutFn = func(ctx context.Context, args []string, out io.Writer) error {
		return runValidateSchemaConsistencyOut(ctx, args, out)
	}

	err := runValidateSchemaConsistency(context.Background(), []string{
		"-db-host", "localhost",
		"-db-port", "5432",
		"-db-user", "postgres",
		"-db-name", "forma",
		"-db-ssl-mode", "disable",
		"-schema-registry-table", "schema_registry_dev",
		"-schema-dir", t.TempDir(),
		"-eav-table", "eav_data_dev",
	})
	if err == nil || !strings.Contains(err.Error(), "create connection pool") {
		t.Fatalf("expected pool creation error, got %v", err)
	}
}

func writeSchemaConsistencyArtifacts(t *testing.T, dir, schemaName, schemaJSON, attrsJSON string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, schemaName+".json"), []byte(schemaJSON), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, schemaName+"_attributes.json"), []byte(attrsJSON), 0o644); err != nil {
		t.Fatalf("write attrs: %v", err)
	}
}

// newSchemaConsistencyMock wires the five queries validator.run issues, in
// order: the schema registry, the attr_id census, the two storage-column
// censuses, then the scalar-row census under list attributes (#372). Only the
// attr_id census varies across the #341 classification tests, so the rest is
// fixed here.
func newSchemaConsistencyMock(t *testing.T, attrCensus *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)

	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("contact", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).
		WillReturnRows(attrCensus)
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = ''`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))
	return mock
}

// retiredLedgerSchemaDir writes a contact schema whose ledger has one active
// attribute (id 1) and one retired entry, legacy (id 9) — the #342 shape.
func retiredLedgerSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "contact",
		`{"type":"object","properties":{"id":{"type":"string"}}}`,
		`{"id":{"attributeID":1,"valueType":"text"},"legacy":{"attributeID":9,"valueType":"text","retired":true}}`)
	return dir
}

func attrCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"})
}

// TestValidateSchemaConsistencyClassifiesRetiredAttrRows pins the #341
// contract: EAV rows whose attr_id belongs to a retired ledger entry are the
// #294 tolerate-and-preserve steady state, so they report as informational and
// leave the pre-flight green.
func TestValidateSchemaConsistencyClassifiesRetiredAttrRows(t *testing.T) {
	mock := newSchemaConsistencyMock(t, attrCensusRows().AddRow(int16(100), int16(9), int64(12)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   retiredLedgerSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	if err := validator.run(context.Background()); err != nil {
		t.Fatalf("preserved rows must not fail the run: %v", err)
	}
	got := out.String()
	if strings.Contains(got, "unknown attribute IDs") {
		t.Fatalf("preserved rows must not be reported as unknown attribute IDs, got %q", got)
	}
	for _, want := range []string{
		"informational (not a failure):",
		"preserved EAV rows for retired attributes in eav_data_dev",
		"schema=contact", "attr_id=9", "attribute=legacy", "rows=12",
		"schema consistency checks passed for 1 schema(s), 1 informational finding(s)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsUnledgeredAttrID pins that classification
// is per attributeID, not a per-schema amnesty: a schema that owns a retired
// entry still fails for a different, unledgered id.
func TestValidateSchemaConsistencyFailsUnledgeredAttrID(t *testing.T) {
	mock := newSchemaConsistencyMock(t, attrCensusRows().AddRow(int16(100), int16(99), int64(2)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   retiredLedgerSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one failure for the unledgered attr_id, got %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "unknown attribute IDs in eav_data_dev: schema_id=100 attr_id=99 rows=2") {
		t.Fatalf("expected the orphan finding, got %q", got)
	}
	if strings.Contains(got, "informational") {
		t.Fatalf("an unledgered attr_id must not be softened to informational, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyFailsUnknownSchemaID pins that the ledger lookup
// is scoped per schema: attr_id 9 is retired on schema 100, and that must not
// excuse rows carrying schema_id 200, which the registry does not know at all.
func TestValidateSchemaConsistencyFailsUnknownSchemaID(t *testing.T) {
	mock := newSchemaConsistencyMock(t, attrCensusRows().AddRow(int16(200), int16(9), int64(4)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   retiredLedgerSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected a failure for the unknown schema_id, got %v", err)
	}
	if got := out.String(); !strings.Contains(got, "schema_id=200 attr_id=9 rows=4") ||
		strings.Contains(got, "informational") {
		t.Fatalf("unknown schema_id must stay a failure, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyReportsBothBlocks pins that a run carrying both
// kinds prints both, counts only the failure, and still exits non-zero.
func TestValidateSchemaConsistencyReportsBothBlocks(t *testing.T) {
	mock := newSchemaConsistencyMock(t, attrCensusRows().
		AddRow(int16(100), int16(9), int64(12)).
		AddRow(int16(100), int16(99), int64(2)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   retiredLedgerSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("the error count must exclude informational findings, got %v", err)
	}
	got := out.String()
	if strings.Contains(got, "schema consistency checks passed") {
		t.Fatalf("a failing run must not print the success line, got %q", got)
	}
	for _, want := range []string{
		"unknown attribute IDs in eav_data_dev: schema_id=100 attr_id=99 rows=2",
		"informational (not a failure):",
		"attribute=legacy",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}
