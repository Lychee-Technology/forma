package main

import (
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
	if _, err := parseCDCFlushFlags(nil); err == nil {
		t.Fatal("expected an error when -s3-bucket is missing")
	}
}
