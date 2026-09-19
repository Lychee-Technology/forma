package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// newEAVDateImageCensusMock wires validator.run's six queries with every
// census empty except the date-image census (#582, #587 review).
func newEAVDateImageCensusMock(t *testing.T, dateCensus *pgxmock.Rows) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)

	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("visit", int16(100)))
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_text IS NOT NULL`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.array_indices = ''`).
		WillReturnRows(empty())
	mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e WHERE e\.value_numeric IS NOT NULL AND \(e\.value_numeric <> trunc\(e\.value_numeric\) OR abs\(e\.value_numeric\) > 9007199254740992\)`).
		WillReturnRows(dateCensus)
	return mock
}

// eavDateSchemaDir writes a visit schema with an unbound datetime (id 1), a
// bigint-bound datetime (id 2), an unbound bigint (id 3) and an unbound list
// of dates (id 4): only ids 1 and 4 store epoch millis the read path decodes
// from eav_data.value_numeric.
func eavDateSchemaDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "visit",
		`{"type":"object","properties":{"seenAt":{"type":"string","format":"date-time"},"loggedAt":{"type":"string","format":"date-time"},"counter":{"type":"integer"},"days":{"type":"array","items":{"type":"string","format":"date"}}}}`,
		`{"seenAt":{"attributeID":1,"valueType":"datetime"},"loggedAt":{"attributeID":2,"valueType":"datetime","column_binding":{"col_name":"bigint_01","encoding":"unix_ms"}},"counter":{"attributeID":3,"valueType":"bigint"},"days":{"attributeID":4,"valueType":"list","items_type":"date"}}`)
	return dir
}

// TestValidateSchemaConsistencyFailsDateImagesTheReadPathRefuses pins the
// pre-upgrade census for #582: an unbound date/datetime whose
// eav_data.value_numeric is not a whole number within 2^53 is a pre-flight
// failure named by schema and attribute, because the upgraded read path
// refuses the row and every update of it with it.
func TestValidateSchemaConsistencyFailsDateImagesTheReadPathRefuses(t *testing.T) {
	mock := newEAVDateImageCensusMock(t, pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).
		AddRow(int16(100), int16(1), int64(2)).
		AddRow(int16(100), int16(4), int64(1)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   eavDateSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "2 issue(s)") {
		t.Fatalf("expected two failures for the date images, got %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"date/datetime images the read path refuses in eav_data_dev: schema=visit schema_id=100 attr_id=1 attribute=seenAt rows=2 (value_numeric must be a whole number with |value| <= 9007199254740992)",
		"date/datetime images the read path refuses in eav_data_dev: schema=visit schema_id=100 attr_id=4 attribute=days rows=1 (value_numeric must be a whole number with |value| <= 9007199254740992)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q, got %q", want, got)
		}
	}
	if strings.Contains(got, "informational") {
		t.Fatalf("date images the read path refuses must not be softened to informational, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}

// TestValidateSchemaConsistencyIgnoresLargeImagesOutsideEAVDateAttrs pins
// that the census is classified against unbound date metadata only: a
// bigint attribute may hold such an image (#205 owns that ceiling), a bound
// datetime has no eav_data rows to judge, and an unknown schema_id or
// attr_id is the attr_id census's finding.
func TestValidateSchemaConsistencyIgnoresLargeImagesOutsideEAVDateAttrs(t *testing.T) {
	mock := newEAVDateImageCensusMock(t, pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}).
		AddRow(int16(100), int16(2), int64(5)).
		AddRow(int16(100), int16(3), int64(40)).
		AddRow(int16(100), int16(99), int64(3)).
		AddRow(int16(200), int16(1), int64(3)))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   eavDateSchemaDir(t),
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		out:         &out,
	}

	if err := validator.run(context.Background()); err != nil {
		t.Fatalf("large images outside unbound date attributes must not fail this check: %v", err)
	}
	if !strings.Contains(out.String(), "schema consistency checks passed for 1 schema(s)") {
		t.Fatalf("expected success output, got %q", out.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet db expectations: %v", err)
	}
}
