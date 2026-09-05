package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// newListScalarCensusMock wires validator.run's five queries with every census
// empty except the scalar-row census under list attributes (#372).
func newListScalarCensusMock(t *testing.T, listCensus *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)

	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("contact", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = '' AND \(e\.value_text IS NOT NULL OR e\.value_numeric IS NOT NULL\)`).
		WillReturnRows(listCensus)
	return mock
}

// listAttrSchemaDir writes a contact schema with one scalar attribute (id 1)
// and one list attribute, tags (id 2).
func listAttrSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "contact",
		`{"type":"object","properties":{"id":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}}}}`,
		`{"id":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"list","items_type":"text"}}`)
	return dir
}

// TestValidateSchemaConsistencyFailsScalarRowsUnderListAttr pins #372: a
// value-carrying row with array_indices ” under an attribute whose metadata
// now declares a list is a pre-flight failure, named by schema and attribute,
// because the parquet export and the DuckDB pivot read it as a one-element
// list while the OLTP path still returns the scalar.
func TestValidateSchemaConsistencyFailsScalarRowsUnderListAttr(t *testing.T) {
	mock := newListScalarCensusMock(t, pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).
		AddRow(int16(100), int16(2), int64(7)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   listAttrSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one failure for the scalar rows, got %v", err)
	}
	got := out.String()
	want := "scalar rows stored under list attributes in eav_data_dev: schema=contact schema_id=100 attr_id=2 attribute=tags rows=7"
	if !strings.Contains(got, want) {
		t.Fatalf("output missing %q, got %q", want, got)
	}
	if strings.Contains(got, "informational") {
		t.Fatalf("scalar rows under a list attribute must not be softened to informational, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyIgnoresScalarRowsOutsideListAttrs pins that the
// census is classified against list metadata only: array_indices ” is the
// normal shape for every scalar attribute, and rows under an unknown
// schema_id or attr_id are the attr_id census's finding, not this one.
func TestValidateSchemaConsistencyIgnoresScalarRowsOutsideListAttrs(t *testing.T) {
	mock := newListScalarCensusMock(t, pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).
		AddRow(int16(100), int16(1), int64(40)).
		AddRow(int16(100), int16(99), int64(3)).
		AddRow(int16(200), int16(2), int64(3)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   listAttrSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	if err := validator.run(context.Background()); err != nil {
		t.Fatalf("scalar rows under scalar or unknown attributes must not fail this check: %v", err)
	}
	if !strings.Contains(out.String(), "schema consistency checks passed for 1 schema(s)") {
		t.Fatalf("expected success output, got %q", out.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}
