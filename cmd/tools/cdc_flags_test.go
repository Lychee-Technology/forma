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

// TestParseCDCFlushFlagsUsingDefaults ensures flag defaults survive the flag
// layer and are not silently overwritten by WithDefaults.
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
