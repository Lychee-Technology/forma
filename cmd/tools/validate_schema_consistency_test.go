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
