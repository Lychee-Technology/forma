package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

const scalarShapeCensusQuery = `SELECT e\.schema_id, e\.attr_id, LENGTH\(e\.array_indices\) - LENGTH\(REPLACE\(e\.array_indices, ',', ''\)\) \+ 1 AS depth, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices <> ''`

const scalarShapeExampleQuery = `SELECT e\.row_id::text, e\.array_indices FROM "eav_data_dev" AS e WHERE e\.schema_id = \$1 AND e\.attr_id = \$2`

func scalarShapeCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{"schema_id", "attr_id", "depth", "record_count"})
}

// expectEmptyScalarShapeCensus expects the census of indexed records under
// scalar attributes (#696) to find no indexed record at all.
func expectEmptyScalarShapeCensus(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(scalarShapeCensusQuery).WillReturnRows(scalarShapeCensusRows())
}

// scalarShapeSchemaDir writes two schemas. contact declares every attribute
// in an object or an array of objects, so the registry derives its array
// ancestry: addresses.city has one array ancestor. visit's place comes from
// another file's $ref, which leaves its ancestry undeclared.
func scalarShapeSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "contact",
		`{"type":"object","properties":{"note":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}},`+
			`"addresses":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"list","items_type":"text"},`+
			`"addresses.city":{"attributeID":3,"valueType":"text"}}`)
	writeSchemaConsistencyArtifacts(t, dir, "visit",
		`{"type":"object","properties":{"stop":{"$ref":"elsewhere.json#/$defs/stop"}}}`,
		`{"stop.place.name":{"attributeID":1,"valueType":"text"}}`)
	return dir
}

func newScalarShapeCensusMock(t *testing.T, census *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)
	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("contact", int16(100)).AddRow("visit", int16(101)))
	for _, query := range []string{
		`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`,
		`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`,
		`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`,
		`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = ''`,
	} {
		mock.ExpectQuery(query).WillReturnRows(empty())
	}
	mock.ExpectQuery(scalarShapeCensusQuery).WillReturnRows(census)
	return mock
}

func expectScalarShapeExample(mock pgxmock.PgxPoolIface, schemaID, attrID int16, depth int, rowID, indices string) {
	mock.ExpectQuery(scalarShapeExampleQuery).WithArgs(schemaID, attrID, depth).
		WillReturnRows(pgxmock.NewRows([]string{"row_id", "array_indices"}).AddRow(rowID, indices))
}

// The census judges each attribute's indexed records by the metadata: a
// scalar's record carries one index per array of objects around it. note
// has none, so its indexed records are array elements stored under a scalar,
// a failure; so are addresses.city's records with two indices. Its records
// with one index are members of the array of objects, and a list's elements
// are not judged. Where the registry declares no ancestry, records within
// the bound the name allows are reported as informational, and records past
// it as failures. Each finding names one record, never a value.
func TestValidateSchemaConsistencyReportsIndexedRecordsUnderScalarAttrs(t *testing.T) {
	mock := newScalarShapeCensusMock(t, scalarShapeCensusRows().
		AddRow(int16(100), int16(1), 1, int64(4)).
		AddRow(int16(100), int16(2), 1, int64(9)).
		AddRow(int16(100), int16(3), 1, int64(5)).
		AddRow(int16(100), int16(3), 2, int64(2)).
		AddRow(int16(101), int16(1), 2, int64(6)).
		AddRow(int16(101), int16(1), 3, int64(1)).
		AddRow(int16(100), int16(99), 1, int64(3)))
	expectScalarShapeExample(mock, 100, 1, 1, "row-a", "0")
	expectScalarShapeExample(mock, 100, 3, 2, "row-b", "1,0")
	expectScalarShapeExample(mock, 101, 1, 2, "row-c", "0,1")
	expectScalarShapeExample(mock, 101, 1, 3, "row-d", "0,0,2")

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool: mock, schemaDir: scalarShapeSchemaDir(t), schemaTable: "schema_registry_dev", eavTable: "eav_data_dev", out: &out,
	}
	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "3 issue(s)") {
		t.Fatalf("expected three failures, got %v\n%s", err, out.String())
	}
	got := out.String()
	failures, notices, _ := strings.Cut(got, "informational (not a failure):")
	for _, want := range []string{
		"- array elements stored under scalar attributes in eav_data_dev: schema=contact schema_id=100 attr_id=1 attribute=note type=text indices=1 array_ancestors=0 rows=4 example=row_id=row-a array_indices=\"0\"",
		"- array elements stored under scalar attributes in eav_data_dev: schema=contact schema_id=100 attr_id=3 attribute=addresses.city type=text indices=2 array_ancestors=1 rows=2 example=row_id=row-b array_indices=\"1,0\"",
		"- array elements stored under scalar attributes in eav_data_dev: schema=visit schema_id=101 attr_id=1 attribute=stop.place.name type=text indices=3 array_ancestors=at most 2 (not declared) rows=1 example=row_id=row-d array_indices=\"0,0,2\"",
	} {
		if !strings.Contains(failures, want) {
			t.Fatalf("failures missing %q, got %q", want, got)
		}
	}
	want := "- indexed scalar records whose array ancestry the registry does not declare in eav_data_dev: schema=visit schema_id=101 attr_id=1 attribute=stop.place.name type=text indices=2 array_ancestors=unknown, at most 2 rows=6 example=row_id=row-c array_indices=\"0,1\""
	if !strings.Contains(notices, want) {
		t.Fatalf("informational block missing %q, got %q", want, got)
	}
	for _, unexpected := range []string{"attr_id=2 ", "indices=1 array_ancestors=1", "attr_id=99"} {
		if strings.Contains(got, unexpected) {
			t.Fatalf("output must not report %q, got %q", unexpected, got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}
