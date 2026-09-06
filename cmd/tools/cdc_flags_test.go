package main

import (
	"strings"
	"testing"
)

// TestParseCDCFlushFlagsMapsArgsToConfig pins the cdc-flush flag surface: the
// 148-line runCDCFlush had zero coverage before #319 split it, so this is the
// first thing that would notice a flag being renamed or wired to the wrong
// config field.
func TestParseCDCFlushFlagsMapsArgsToConfig(t *testing.T) {
	opts, err := parseCDCFlushFlags([]string{
		"-s3-bucket=my-bucket",
		"-s3-prefix=my-delta",
		"-change-log-table=cl",
		"-entity-main-table=em",
		"-eav-table=eav",
		"-min-records=5",
		"-max-age-ms=1000",
		"-batch-size=7",
		"-dry-run",
	})
	if err != nil {
		t.Fatalf("parse cdc-flush flags: %v", err)
	}
	if opts == nil {
		t.Fatal("expected options, got nil")
	}
	if opts.cfg.S3Bucket != "my-bucket" {
		t.Errorf("S3Bucket = %q, want %q", opts.cfg.S3Bucket, "my-bucket")
	}
	if opts.cfg.S3Prefix != "my-delta" {
		t.Errorf("S3Prefix = %q, want %q", opts.cfg.S3Prefix, "my-delta")
	}
	if opts.cfg.ChangeLogTable != "cl" {
		t.Errorf("ChangeLogTable = %q, want %q", opts.cfg.ChangeLogTable, "cl")
	}
	if opts.cfg.EntityMainTable != "em" {
		t.Errorf("EntityMainTable = %q, want %q", opts.cfg.EntityMainTable, "em")
	}
	if opts.cfg.EAVDataTable != "eav" {
		t.Errorf("EAVDataTable = %q, want %q", opts.cfg.EAVDataTable, "eav")
	}
	if opts.cfg.MinRecords != 5 {
		t.Errorf("MinRecords = %d, want 5", opts.cfg.MinRecords)
	}
	if opts.cfg.MaxAgeMs != 1000 {
		t.Errorf("MaxAgeMs = %d, want 1000", opts.cfg.MaxAgeMs)
	}
	if opts.cfg.BatchSize != 7 {
		t.Errorf("BatchSize = %d, want 7", opts.cfg.BatchSize)
	}
	if !opts.dryRun {
		t.Error("dryRun = false, want true")
	}
}

// TestParseCDCFlushFlagsRejectsMissingBucket keeps the s3Flags.validate(true)
// gate wired: dropping the validate call must not silently produce a config
// that fails much later inside RunOnce.
func TestParseCDCFlushFlagsRejectsMissingBucket(t *testing.T) {
	_, err := parseCDCFlushFlags(nil)
	if err == nil {
		t.Fatal("expected an error when -s3-bucket is missing")
	}
	if !strings.Contains(err.Error(), "--s3-bucket is required") {
		t.Errorf("error message = %q, want to contain %q", err.Error(), "--s3-bucket is required")
	}
}

// TestParseCDCFlushFlagsUsingDefaults pins the flag-layer defaults, but only
// partially: buildCDCConfig ends in cdc.CDCConfig.WithDefaults(), which
// backfills any field left <= 0 or "" (internal/cdc/config.go).
//
// Genuinely pinned here — WithDefaults never touches them, so deleting the
// flag default turns this test red: S3Prefix, ManifestTemplate, PGHost,
// PGPort, PGDB.
//
// Shadowed by WithDefaults, which supplies the identical value: MinRecords,
// MaxAgeMs, BatchSize, PGSSLMode. Deleting those flag defaults leaves this
// test green (measured), so their assertions cannot detect a missing flag
// default — they only catch a change in the agreed value. Keep them for that,
// but do not read them as coverage of the flag layer.
func TestParseCDCFlushFlagsUsingDefaults(t *testing.T) {
	opts, err := parseCDCFlushFlags([]string{"-s3-bucket=b"})
	if err != nil {
		t.Fatalf("parse cdc-flush flags: %v", err)
	}
	if opts == nil {
		t.Fatal("expected options, got nil")
	}
	if opts.cfg.MinRecords != 20000 {
		t.Errorf("MinRecords = %d, want 20000", opts.cfg.MinRecords)
	}
	if opts.cfg.MaxAgeMs != 3600000 {
		t.Errorf("MaxAgeMs = %d, want 3600000", opts.cfg.MaxAgeMs)
	}
	if opts.cfg.BatchSize != 10000 {
		t.Errorf("BatchSize = %d, want 10000", opts.cfg.BatchSize)
	}
	if opts.cfg.S3Prefix != "delta" {
		t.Errorf("S3Prefix = %q, want %q", opts.cfg.S3Prefix, "delta")
	}
	if opts.cfg.ManifestTemplate != "manifest/{{.SchemaID}}.json" {
		t.Errorf("ManifestTemplate = %q, want %q", opts.cfg.ManifestTemplate, "manifest/{{.SchemaID}}.json")
	}
	if opts.cfg.PGHost != "localhost" {
		t.Errorf("PGHost = %q, want %q", opts.cfg.PGHost, "localhost")
	}
	if opts.cfg.PGPort != 5432 {
		t.Errorf("PGPort = %d, want 5432", opts.cfg.PGPort)
	}
	if opts.cfg.PGDB != "forma" {
		t.Errorf("PGDB = %q, want %q", opts.cfg.PGDB, "forma")
	}
	if opts.cfg.PGSSLMode != "require" {
		t.Errorf("PGSSLMode = %q, want %q", opts.cfg.PGSSLMode, "require")
	}
}

// TestParseCDCFlushFlagsHelp ensures -help exits cleanly with nil error
// and nil options, preserving the CLI's quiet exit.
func TestParseCDCFlushFlagsHelp(t *testing.T) {
	opts, err := parseCDCFlushFlags([]string{"-help"})
	if opts != nil {
		t.Errorf("opts = %v, want nil", opts)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

// TestParseCDCFlushFlagsRejectsSchemaRegistryTableWithoutDir ensures
// schemaRegistry.validate(false) is wired and rejects incomplete config.
func TestParseCDCFlushFlagsRejectsSchemaRegistryTableWithoutDir(t *testing.T) {
	_, err := parseCDCFlushFlags([]string{"-s3-bucket=b", "-schema-registry-table=t"})
	if err == nil {
		t.Fatal("expected an error when -schema-registry-table is given without -schema-registry-dir")
	}
}

// TestParseCDCInitFlagsMapsArgsToConfig pins the cdc-init flag surface for the
// same reason as its cdc-flush sibling: runCDCInit was 155 code lines with no
// test (#319).
func TestParseCDCInitFlagsMapsArgsToConfig(t *testing.T) {
	opts, err := parseCDCInitFlags([]string{
		"-s3-bucket=init-bucket",
		"-s3-prefix=my-base",
		"-schema-registry-table=registry",
		"-schema-dir=/tmp/schemas",
		"-entity-main-table=em",
		"-eav-table=eav",
		"-batch-size=11",
		"-target-file-size-mb=64",
		"-max-batch-size=99",
		"-schema-id=3",
		"-dry-run",
		"-replace-delta",
		"-delta-prefix=my-delta",
	})
	if err != nil {
		t.Fatalf("parse cdc-init flags: %v", err)
	}
	if opts == nil {
		t.Fatal("expected options, got nil")
	}
	if opts.cfg.S3Bucket != "init-bucket" {
		t.Errorf("S3Bucket = %q, want %q", opts.cfg.S3Bucket, "init-bucket")
	}
	if opts.cfg.S3Prefix != "my-base" {
		t.Errorf("S3Prefix = %q, want %q", opts.cfg.S3Prefix, "my-base")
	}
	if opts.cfg.BatchSize != 11 {
		t.Errorf("BatchSize = %d, want 11", opts.cfg.BatchSize)
	}
	if opts.cfg.TargetFileSizeMB != 64 {
		t.Errorf("TargetFileSizeMB = %d, want 64", opts.cfg.TargetFileSizeMB)
	}
	if opts.cfg.MaxBatchSize != 99 {
		t.Errorf("MaxBatchSize = %d, want 99", opts.cfg.MaxBatchSize)
	}
	if opts.registry.table != "registry" {
		t.Errorf("registry table = %q, want %q", opts.registry.table, "registry")
	}
	if opts.registry.dir != "/tmp/schemas" {
		t.Errorf("registry dir = %q, want %q", opts.registry.dir, "/tmp/schemas")
	}
	if opts.schemaIDFilter != 3 {
		t.Errorf("schemaIDFilter = %d, want 3", opts.schemaIDFilter)
	}
	if !opts.dryRun {
		t.Error("dryRun = false, want true")
	}
	if !opts.replaceDelta {
		t.Error("replaceDelta = false, want true")
	}
	if opts.deltaPrefix != "my-delta" {
		t.Errorf("deltaPrefix = %q, want %q", opts.deltaPrefix, "my-delta")
	}
	// -estimated-row-bytes was not passed, so the auto-estimate path stays on.
	// This flag is derived, not read back from cfg, and dropping the derivation
	// during the split would be invisible without this assertion.
	if !opts.autoEstimateRowBytes {
		t.Error("autoEstimateRowBytes = false, want true when -estimated-row-bytes is absent")
	}
}

// TestParseCDCInitFlagsRequiresSchemaRegistry keeps schemaRegistry.validate(true)
// wired — cdc-init requires the registry pair where cdc-flush does not.
func TestParseCDCInitFlagsRequiresSchemaRegistry(t *testing.T) {
	if _, err := parseCDCInitFlags([]string{"-s3-bucket=b"}); err == nil {
		t.Fatal("expected an error when -schema-registry-table/-schema-dir are missing")
	}
}

// TestParseCDCInitFlagsUsingDefaults pins the flag-layer defaults, but only
// partially, for the same reason as its cdc-flush sibling: buildCDCInitConfig
// ends in cdc.CDCConfig.WithDefaults(), which backfills any field left <= 0
// or "" (internal/cdc/config.go).
//
// Genuinely pinned here — WithDefaults never touches them, so deleting the
// flag default turns this test red: TargetFileSizeMB, S3Prefix,
// ManifestTemplate, PGHost, PGPort, PGDB.
//
// Shadowed by WithDefaults, which supplies the identical value: MaxBatchSize,
// EntityMainTable, EAVDataTable, PGSSLMode. Deleting those flag defaults
// leaves this test green (measured), so their assertions cannot detect a
// missing flag default — they only catch a change in the agreed value. Keep
// them for that, but do not read them as coverage of the flag layer.
func TestParseCDCInitFlagsUsingDefaults(t *testing.T) {
	opts, err := parseCDCInitFlags([]string{
		"-s3-bucket=b",
		"-schema-registry-table=t",
		"-schema-dir=/d",
	})
	if err != nil {
		t.Fatalf("parse cdc-init flags: %v", err)
	}
	if opts == nil {
		t.Fatal("expected options, got nil")
	}
	if opts.cfg.TargetFileSizeMB != 256 {
		t.Errorf("TargetFileSizeMB = %d, want 256", opts.cfg.TargetFileSizeMB)
	}
	if opts.cfg.MaxBatchSize != 10000000 {
		t.Errorf("MaxBatchSize = %d, want 10000000", opts.cfg.MaxBatchSize)
	}
	if opts.cfg.S3Prefix != "base" {
		t.Errorf("S3Prefix = %q, want %q", opts.cfg.S3Prefix, "base")
	}
	// The delta-tier flags (#371) are flag-layer only; WithDefaults never
	// sees them. The default delta prefix matches cdc-flush --s3-prefix, and
	// the purge is opt-in.
	if opts.replaceDelta {
		t.Error("replaceDelta = true, want false by default")
	}
	if opts.deltaPrefix != "delta" {
		t.Errorf("deltaPrefix = %q, want %q", opts.deltaPrefix, "delta")
	}
	if opts.cfg.ManifestTemplate != "manifest/{{.SchemaID}}.json" {
		t.Errorf("ManifestTemplate = %q, want %q", opts.cfg.ManifestTemplate, "manifest/{{.SchemaID}}.json")
	}
	if opts.cfg.EntityMainTable != "entity_main" {
		t.Errorf("EntityMainTable = %q, want %q", opts.cfg.EntityMainTable, "entity_main")
	}
	if opts.cfg.EAVDataTable != "eav_data" {
		t.Errorf("EAVDataTable = %q, want %q", opts.cfg.EAVDataTable, "eav_data")
	}
	if opts.cfg.PGHost != "localhost" {
		t.Errorf("PGHost = %q, want %q", opts.cfg.PGHost, "localhost")
	}
	if opts.cfg.PGPort != 5432 {
		t.Errorf("PGPort = %d, want 5432", opts.cfg.PGPort)
	}
	if opts.cfg.PGDB != "forma" {
		t.Errorf("PGDB = %q, want %q", opts.cfg.PGDB, "forma")
	}
	if opts.cfg.PGSSLMode != "require" {
		t.Errorf("PGSSLMode = %q, want %q", opts.cfg.PGSSLMode, "require")
	}
}

// TestParseCDCInitFlagsHelp ensures -help exits cleanly with nil error
// and nil options, preserving the CLI's quiet exit.
func TestParseCDCInitFlagsHelp(t *testing.T) {
	opts, err := parseCDCInitFlags([]string{"-help"})
	if opts != nil {
		t.Errorf("opts = %v, want nil", opts)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

// TestParseCDCInitFlagsRejectsMissingBucket keeps the s3Flags.validate(true)
// gate wired: dropping the validate call must not silently produce a config
// that fails much later inside RunInit.
func TestParseCDCInitFlagsRejectsMissingBucket(t *testing.T) {
	_, err := parseCDCInitFlags([]string{"-schema-registry-table=t", "-schema-dir=/d"})
	if err == nil {
		t.Fatal("expected an error when -s3-bucket is missing")
	}
}

// TestParseCDCInitFlagsEstimatedRowBytes ensures that when -estimated-row-bytes
// is passed, both cfg.EstimatedRowBytes is set to that value and autoEstimateRowBytes
// is false (not true). This guards against mutations that hardcode autoEstimateRowBytes
// to true or cross-wire EstimatedRowBytes to a different flag.
func TestParseCDCInitFlagsEstimatedRowBytes(t *testing.T) {
	opts, err := parseCDCInitFlags([]string{
		"-s3-bucket=b",
		"-schema-registry-table=t",
		"-schema-dir=/d",
		"-estimated-row-bytes=4096",
	})
	if err != nil {
		t.Fatalf("parse cdc-init flags: %v", err)
	}
	if opts == nil {
		t.Fatal("expected options, got nil")
	}
	if opts.cfg.EstimatedRowBytes != 4096 {
		t.Errorf("EstimatedRowBytes = %d, want 4096", opts.cfg.EstimatedRowBytes)
	}
	if opts.autoEstimateRowBytes {
		t.Error("autoEstimateRowBytes = true, want false when -estimated-row-bytes is passed")
	}
}

// A --manifest-template that collapses every schema onto one object would
// append schema A's delta entries into schema B's manifest. cdc-init already
// probes the template (#518); flush must refuse it before opening Postgres
// or S3 (#520). An empty template keeps meaning "manifest tracking off".
func TestParseCDCFlushFlagsRejectsCollapsedManifestTemplate(t *testing.T) {
	for _, tmpl := range []string{"manifest/all.json", "manifest/{{.SchemaId}}.json"} {
		_, err := parseCDCFlushFlags([]string{"-s3-bucket=b", "-manifest-template=" + tmpl})
		if err == nil {
			t.Fatalf("template %q must be rejected", tmpl)
		}
		if !strings.Contains(err.Error(), "manifest-template") {
			t.Errorf("template %q: error %q must name the flag", tmpl, err.Error())
		}
	}
	if _, err := parseCDCFlushFlags([]string{"-s3-bucket=b", "-manifest-template="}); err != nil {
		t.Fatalf("empty template disables manifests and must parse: %v", err)
	}
}
