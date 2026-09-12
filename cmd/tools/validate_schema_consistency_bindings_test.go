package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// #459: registration guards only future schemas; a deployment already
// carrying text→uuid_02 has the live 500/silent-drop shape today. The tool
// lists every such binding across the deployed set as a failure, and still
// runs the EAV checks (it loads through the deferred-binding loader).
func TestValidateSchemaConsistencyReportsBindingMismatches(t *testing.T) {
	schemaDir := t.TempDir()
	writeSchemaConsistencyArtifacts(t, schemaDir, "contact",
		`{"type":"object","properties":{"leadId":{"type":"string"},"rank":{"type":"integer"}}}`,
		`{"leadId":{"attributeID":1,"valueType":"text","column_binding":{"col_name":"uuid_02"}},
		  "rank":{"attributeID":2,"valueType":"bool","column_binding":{"col_name":"smallint_01"}},
		  "name":{"attributeID":3,"valueType":"text","column_binding":{"col_name":"text_01"}}}`)
	mock := newSchemaConsistencyMock(t, pgxmock.NewRows([]string{"schema_id", "attr_id", "record_count"}))

	var out strings.Builder
	validator := schemaConsistencyValidator{
		pool: mock, schemaDir: schemaDir, schemaTable: "schema_registry_dev", eavTable: "eav_data_dev", out: &out,
	}
	err := validator.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed with 2 issue(s)") {
		t.Fatalf("expected two binding failures, got err=%v out=%q", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"- valueType/column-encoding binding mismatches: schema=contact attribute leadId (valueType text) cannot round-trip through main column uuid_02",
		"- valueType/column-encoding binding mismatches: schema=contact attribute rank (valueType bool) cannot round-trip through main column smallint_01",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in output:\n%s", want, got)
		}
	}
	if strings.Contains(got, "attribute name (") {
		t.Fatalf("compatible binding must not be reported:\n%s", got)
	}
	if n := strings.Count(got, "- valueType/column-encoding binding mismatches:"); n != 2 {
		t.Fatalf("expected exactly two binding mismatch lines, got %d:\n%s", n, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("EAV checks must still run: %v", err)
	}
}
