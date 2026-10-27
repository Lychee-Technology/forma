package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

// TestRegisterSchemasRecordsGenerationOne: a schema init-db registers gets
// generation 1 of the declaration it was registered with, in the same
// transaction, so it never runs without a baseline. One without an attributes
// file is registered with none, and says so.
func TestRegisterSchemasRecordsGenerationOne(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"lead.json":            `{"type":"object","properties":{"score":{"type":"integer"}}}`,
		"lead_attributes.json": `{"score":{"attributeID":1,"valueType":"integer","column_binding":{"col_name":"integer_01"}}}`,
		"visit.json":           `{}`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev" \(schema_name, schema_id\)`).
		WithArgs("lead", int16(100)).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`UPDATE "schema_registry_dev" SET active_generation = \$2 WHERE schema_id = \$1 AND active_generation IS NULL`).
		WithArgs(int16(100), int64(1)).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev_generation"`).
		WithArgs(int16(100), int64(1), pgxmock.AnyArg(), "init-db", "").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev_generation_attr"`).
		WithArgs(int16(100), int64(1), int16(1), "score", "integer", "", "integer_01", "default", false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev" \(schema_name, schema_id\)`).
		WithArgs("visit", int16(101)).WillReturnResult(pgxmock.NewResult("INSERT", 1))

	tx, err := mock.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var regErr error
	stdout, _ := captureStdStreams(t, func() {
		regErr = registerSchemas(context.Background(), tx, "schema_registry_dev", dir)
	})
	if regErr != nil {
		t.Fatalf("registerSchemas: %v", regErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("generation 1 must be recorded for lead only: %v", err)
	}
	for _, want := range []string{
		"Recorded generation 1 of schema lead",
		"Schema visit has no attributes file, so no generation is recorded; run baseline-schema",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("init-db output %q lacks %q", stdout, want)
		}
	}
}

// TestRegisterSchemasRefusesAnInvalidAttributesFile: generation 1 records a
// declaration the registry would admit, so init-db fails on one it would not
// rather than registering it.
func TestRegisterSchemasRefusesAnInvalidAttributesFile(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"lead.json":            `{}`,
		"lead_attributes.json": `{"a":{"attributeID":1,"valueType":"text"},"b":{"attributeID":1,"valueType":"text"}}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev" \(schema_name, schema_id\)`).
		WithArgs("lead", int16(100)).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	tx, err := mock.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var regErr error
	captureStdStreams(t, func() { regErr = registerSchemas(context.Background(), tx, "schema_registry_dev", dir) })
	if regErr == nil || !strings.Contains(regErr.Error(), "record generation 1 of schema lead") {
		t.Fatalf("registerSchemas = %v, want the attribute id collision refused", regErr)
	}
}

func TestLedgerDDLAnnouncements(t *testing.T) {
	ddl := ledgerDDL(testInitDBOptions())
	want := []string{
		"Ensured active generation column: schema_registry.active_generation\n",
		"Created schema generation table: schema_registry_generation\n",
		"Created schema generation attribute table: schema_registry_generation_attr\n",
	}
	if len(ddl) != len(want) {
		t.Fatalf("ledgerDDL returned %d statements, want %d", len(ddl), len(want))
	}
	for i := range want {
		if ddl[i].announce != want[i] {
			t.Errorf("announcement %d = %q, want %q", i, ddl[i].announce, want[i])
		}
	}
}
