package main

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/bootstrap"
)

// TestSampleConfigFromEnvRejectsUnrecognisedBoolean pins #642 on the sample
// importer: VALIDATE_UPDATES_STRICT=yes used to read as false and import with
// updates report-only. sampleConfigFromEnv does no I/O and runSampleImport
// calls it before connecting, so the run now stops before touching the
// database.
func TestSampleConfigFromEnvRejectsUnrecognisedBoolean(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "yes")

	config, err := sampleConfigFromEnv("./schemas")
	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) || envErr.Key != "VALIDATE_UPDATES_STRICT" {
		t.Fatalf("expected an *bootstrap.EnvError on VALIDATE_UPDATES_STRICT, got %v", err)
	}
	if config != nil {
		t.Fatalf("a refused environment must return no config, got %+v", config)
	}
}

// TestSampleConfigFromEnv pins what the extraction from runSampleImport must
// keep: the sample tables, the schema directory and the strict-update overlay.
func TestSampleConfigFromEnv(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "TRUE")

	config, err := sampleConfigFromEnv("./schemas")
	if err != nil {
		t.Fatalf("sampleConfigFromEnv: %v", err)
	}
	if !config.Entity.ValidateUpdatesStrict {
		t.Fatal("VALIDATE_UPDATES_STRICT=TRUE must enable strict updates")
	}
	if config.Entity.SchemaDirectory != "./schemas" {
		t.Fatalf("expected schema directory ./schemas, got %q", config.Entity.SchemaDirectory)
	}
	tables := config.Database.TableNames
	if tables.EAVData != "eav_data_sample" || tables.EntityMain != "entity_main_sample" ||
		tables.SchemaRegistry != "schema_registry_sample" || tables.ChangeLog != "change_log_sample" {
		t.Fatalf("expected the sample tables, got %+v", tables)
	}
}
