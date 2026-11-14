package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
)

// TestValidateSchemaConsistencyReportsNonCanonicalRFC3339Images (#842): the
// validator runs the storage-domain census over each date bound with
// rfc3339 and fails on every image that is not canonical, naming its row,
// its stored image and its repair: one the read refuses, and one the read
// parses that the two routes can answer differently on. A canonical image
// passes.
func TestValidateSchemaConsistencyReportsNonCanonicalRFC3339Images(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)
	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("visit", int16(100)))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).WillReturnRows(empty())
	}
	canonical, offset, noDay := uuid.New(), uuid.New(), uuid.New()
	mock.ExpectQuery(`SELECT m\.ltbase_schema_id, w\.attr_id, m\.ltbase_row_id, v\.image`).
		WithArgs(int16(100), int16(1), "text_02", "datetime").
		WillReturnRows(pgxmock.NewRows(strings.Fields("ltbase_schema_id attr_id ltbase_row_id image pending last_flushed_at")).
			AddRow(int16(100), int16(1), canonical, "2024-01-02T03:04:05.250Z", false, int64(0)).
			AddRow(int16(100), int16(1), offset, "2024-01-02T11:04:05+08:00", false, int64(7)).
			AddRow(int16(100), int16(1), noDay, "2024-02-30T03:04:05.250Z", true, int64(0)))

	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "visit",
		`{"type":"object","properties":{"seenAt":{"type":"string","format":"date-time"}}}`,
		`{"seenAt":{"attributeID":1,"valueType":"datetime","column_binding":{"col_name":"text_02","encoding":"rfc3339"}}}`)
	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   dir,
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		widths:      widthAuditOptions{entityMainTable: "entity_main_dev"},
		out:         &out,
	}
	err = validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "2 issue(s)") {
		t.Fatalf("run = %v, want two failures\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		fmt.Sprintf(`- rfc3339 images that are not canonical, on which the PostgreSQL and DuckDB routes can disagree, in entity_main_dev: `+
			`schema=visit schema_id=100 attr_id=1 attribute=seenAt declared=datetime row_id=%s column=text_02 value="2024-01-02T11:04:05+08:00" last_flushed_at=7 `+
			`repair="spelling only" (not the canonical rfc3339 image of its instant, 2024-01-02T03:04:05.000Z; a rewrite through the API changes only the spelling)`, offset),
		fmt.Sprintf(`- rfc3339 images the OLTP read refuses in entity_main_dev: schema=visit schema_id=100 attr_id=1 attribute=seenAt declared=datetime row_id=%s column=text_02 value="2024-02-30T03:04:05.250Z"`, noDay),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("report lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, canonical.String()) {
		t.Fatalf("a canonical image was reported:\n%s", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestValidateSchemaConsistencyReportsBoolTextImages (#857): a bool bound
// with bool_text joins the column census, and each image the census keeps
// fails the run, naming its row, its column and the encoding's contract.
// The census query keeps only images other than "1", "0" and NULL; the
// integration test runs it over a real entity_main.
func TestValidateSchemaConsistencyReportsBoolTextImages(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("create mock pool: %v", err)
	}
	t.Cleanup(mock.Close)
	empty := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}) }
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("visit", int16(100)))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT e\.schema_id, e\.attr_id, COUNT\(\*\) AS record_count FROM "eav_data_dev" AS e`).WillReturnRows(empty())
	}
	word := uuid.New()
	mock.ExpectQuery(`SELECT m\.ltbase_schema_id, w\.attr_id, m\.ltbase_row_id, v\.image`).
		WithArgs(int16(100), int16(1), "text_01", "bool").
		WillReturnRows(pgxmock.NewRows(strings.Fields("ltbase_schema_id attr_id ltbase_row_id image pending last_flushed_at")).
			AddRow(int16(100), int16(1), word, "true", true, int64(7)))

	dir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, dir, "visit",
		`{"type":"object","properties":{"active":{"type":"boolean"}}}`,
		`{"active":{"attributeID":1,"valueType":"bool","column_binding":{"col_name":"text_01","encoding":"bool_text"}}}`)
	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool:        mock,
		schemaDir:   dir,
		schemaTable: "schema_registry_dev",
		eavTable:    "eav_data_dev",
		widths:      widthAuditOptions{entityMainTable: "entity_main_dev"},
		out:         &out,
	}
	err = validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 issue(s)") {
		t.Fatalf("run = %v, want one failure\n%s", err, out.String())
	}
	want := fmt.Sprintf(`- bool_text images the OLTP read refuses in entity_main_dev: schema=visit schema_id=100 attr_id=1 attribute=active declared=bool row_id=%s column=text_01 value="true" last_flushed_at=7 (bool_text holds "1", "0" or NULL)`, word)
	if got := out.String(); !strings.Contains(got, want) {
		t.Fatalf("report lacks %q:\n%s", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
