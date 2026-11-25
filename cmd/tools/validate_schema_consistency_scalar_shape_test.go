package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// indexedScalarCensusQuery matches the #696 census query.
const indexedScalarCensusQuery = `SELECT e\.schema_id, e\.attr_id, length\(e\.array_indices\) - length\(replace\(e\.array_indices, ',', ''\)\) \+ 1 AS depth`

func indexedScalarCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{"schema_id", "attr_id", "depth", "record_count", "example"})
}

// expectIndexedScalarCensus expects the #696 census and answers no record.
func expectIndexedScalarCensus(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(indexedScalarCensusQuery).WillReturnRows(indexedScalarCensusRows())
}

// censusLeadSchema declares, for schema "lead" (id 100): note, a scalar;
// phones, a primitive array under an element-typed (pre-#204) ledger entry;
// interests, an array of objects; office, an object or one array of
// objects; tags, a list.
const censusLeadSchema = `{"type":"object","properties":{
	"note":{"type":"string"},
	"phones":{"type":["array","null"],"items":{"type":"string"}},
	"interests":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"}}}},
	"office":{"anyOf":[{"type":"object","properties":{"city":{"type":"string"}}},
		{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
	"tags":{"type":"array","items":{"type":"string"}}}}`

const censusLeadAttributes = `{"note":{"attributeID":1,"valueType":"text"},"phones":{"attributeID":2,"valueType":"text"},
	"interests.id":{"attributeID":3,"valueType":"text"},"office.city":{"attributeID":4,"valueType":"text"},
	"tags":{"attributeID":5,"valueType":"list","items_type":"text"}}`

func runIndexedScalarCensus(t *testing.T, schemaJSON string, census *pgxmock.Rows) (string, error) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "lead", schemaJSON, censusLeadAttributes)

	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("lead", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).WillReturnRows(empty())
	mock.ExpectQuery(`WHERE e\.value_text IS NOT NULL`).WillReturnRows(empty())
	mock.ExpectQuery(`WHERE e\.value_numeric IS NOT NULL`).WillReturnRows(empty())
	mock.ExpectQuery(`WHERE e\.array_indices = '' AND`).WillReturnRows(empty())
	mock.ExpectQuery(indexedScalarCensusQuery).WillReturnRows(census)
	mock.MatchExpectationsInOrder(true)

	var out strings.Builder
	validator := schemaConsistencyValidator{pool: mock, schemaDir: dir, schemaTable: "schema_registry_dev", eavTable: "eav_data_dev", out: &out}
	err = validator.run(context.Background())
	return out.String(), err
}

// TestValidateSchemaConsistencyCensusesIndexedScalarRecords pins the #696
// census: indexed records the schema's structure admits are not reported,
// whatever their count of indices, and the ones it admits no arrangement
// for fail the pre-flight, named by one record and never a value.
func TestValidateSchemaConsistencyCensusesIndexedScalarRecords(t *testing.T) {
	const example = "00000000-0000-4000-8000-00000000000b"
	census := indexedScalarCensusRows().
		AddRow(int16(100), int16(1), 1, int64(2), example+" 0").     // note inside an array: anomaly
		AddRow(int16(100), int16(2), 1, int64(9), example+" 3").     // a declared primitive array
		AddRow(int16(100), int16(2), 2, int64(1), example+" 0,1").   // nested in the declared array: anomaly
		AddRow(int16(100), int16(3), 1, int64(4), example+" 1").     // a member of the array of objects
		AddRow(int16(100), int16(3), 2, int64(1), example+" 0,0").   // an extra array around it: anomaly
		AddRow(int16(100), int16(4), 1, int64(3), example+" 0").     // office as one array of objects
		AddRow(int16(100), int16(4), 2, int64(1), example+" 0,0").   // past it: anomaly
		AddRow(int16(100), int16(5), 3, int64(1), example+" 0,0,0"). // a list: #372's census
		AddRow(int16(100), int16(9), 1, int64(1), example+" 0")      // unknown attribute: the attr_id census's
	out, err := runIndexedScalarCensus(t, censusLeadSchema, census)
	if err == nil || !strings.Contains(err.Error(), "4 issue(s)") {
		t.Fatalf("expected four failures, got %v\n%s", err, out)
	}
	category := "array elements stored where the schema declares no such arrays, under scalar attributes in eav_data_dev: "
	for _, want := range []string{
		`schema=lead schema_id=100 attr_id=1 attribute=note type=text indices=1 rows=2 example=row_id=` + example + ` array_indices="0"`,
		`attr_id=2 attribute=phones type=text indices=2 rows=1 example=row_id=` + example + ` array_indices="0,1"`,
		`attr_id=3 attribute=interests.id type=text indices=2 rows=1`,
		`attr_id=4 attribute=office.city type=text indices=2 rows=1`,
	} {
		if !strings.Contains(out, category) || !strings.Contains(out, want) {
			t.Fatalf("output missing %q, got\n%s", want, out)
		}
	}
	for _, clean := range []string{"indices=1 rows=9", "indices=1 rows=4", "indices=1 rows=3", "attr_id=5", "attr_id=9"} {
		if strings.Contains(out, clean) {
			t.Fatalf("output must not report %q, got\n%s", clean, out)
		}
	}
}

// TestValidateSchemaConsistencyCensusesAScalarOnlyBranch pins #696 review
// R1-F1 in the census: a text attribute whose second branch admits only
// false admits no array, so its indexed record is an anomaly, as the update
// that stored it is refused.
func TestValidateSchemaConsistencyCensusesAScalarOnlyBranch(t *testing.T) {
	const example = "00000000-0000-4000-8000-00000000000b"
	schema := `{"type":"object","properties":{"note":{"anyOf":[{"type":"string"},{"enum":[false]}]},
		"phones":{"anyOf":[{"type":"string"},{"type":"array","items":{"type":"string"}}]}}}`
	census := indexedScalarCensusRows().
		AddRow(int16(100), int16(1), 1, int64(1), example+" 0").
		AddRow(int16(100), int16(2), 1, int64(1), example+" 0")
	out, err := runIndexedScalarCensus(t, schema, census)
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("expected one failure, got %v\n%s", err, out)
	}
	if !strings.Contains(out, "array elements stored where the schema declares no such arrays") ||
		!strings.Contains(out, `attr_id=1 attribute=note type=text indices=1 rows=1`) || strings.Contains(out, "attr_id=2") {
		t.Fatalf("expected note alone reported, got\n%s", out)
	}
}

// TestValidateSchemaConsistencyReportsUnjudgedIndexedRecordsAsUnverified
// pins that a group the census cannot judge is informational, not clean
// and not a failure: here a pointer crosses a nested $id into a oneOf, so
// the schema keeps a partial container skeleton, which does not follow its
// references. A $ref into a oneOf alone is followed (TestShapeFollows...).
func TestValidateSchemaConsistencyReportsUnjudgedIndexedRecordsAsUnverified(t *testing.T) {
	schema := `{"type":"object","$defs":{"inner":{"$id":"inner.json","properties":{
		"x":{"oneOf":[{"type":"string"},{"type":"integer"}]}}}},"properties":{
		"note":{"$ref":"#/$defs/inner/properties/x/oneOf/0"},"phones":{"type":"string"}}}`
	census := indexedScalarCensusRows().
		AddRow(int16(100), int16(1), 1, int64(2), "00000000-0000-4000-8000-00000000000b 0").
		AddRow(int16(100), int16(2), 1, int64(1), "00000000-0000-4000-8000-00000000000b 0")
	out, err := runIndexedScalarCensus(t, schema, census)
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("a partial skeleton's refusal is still proof: %v\n%s", err, out)
	}
	want := "indexed records under scalar attributes the census could not judge in eav_data_dev: schema=lead schema_id=100 attr_id=1 attribute=note type=text indices=1 rows=2"
	if !strings.Contains(out, "informational (not a failure):") || !strings.Contains(out, want) ||
		!strings.Contains(out, "unverified=the schema keeps a partial container skeleton") {
		t.Fatalf("output missing the unverified group, got\n%s", out)
	}
	if !strings.Contains(out, "array elements stored where the schema declares no such arrays") || !strings.Contains(out, "attr_id=2 attribute=phones") {
		t.Fatalf("output missing the refused group, got\n%s", out)
	}
}
