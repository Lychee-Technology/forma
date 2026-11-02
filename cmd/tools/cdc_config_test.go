package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
)

// sharedCDCArgs passes every flag of the block cdc-flush and cdc-init share
// (#445), each with a value that is distinct from every other and from both
// the flag default and the WithDefaults backfill. So a shared field that is
// dropped, transposed with a same-typed neighbour or left to a default turns
// TestParseCDCFlagsMapSharedBlock red, including the fields whose defaults
// the TestParseCDC*UsingDefaults tests cannot see (the WithDefaults shadow).
var sharedCDCArgs = []string{
	"-pg-host", "pg.example",
	"-pg-port", "6543",
	"-pg-user", "cdc-user",
	"-pg-password", "cdc-test-pw",
	"-pg-db", "cdc-db",
	"-pg-ssl-mode", "verify-full",
	"-duckdb-path", "/tmp/cdc.duckdb",
	"-duck-threads", "7",
	"-duck-mem-limit", "3GB",
	"-query-timeout", "42s",
	"-parquet-compression", "snappy",
	"-parquet-compression-level", "9",
	"-s3-bucket", "cdc-bucket",
	"-s3-prefix", "cdc-prefix",
	"-s3-endpoint", "http://minio.example",
	"-s3-region", "eu-west-3",
	"-s3-use-ssl=false",
	"-s3-use-path",
	"-manifest-prefix", "cdc-manifests",
	"-manifest-template", "cdc/{{.SchemaID}}.json",
	"-entity-main-table", "cdc_main",
	"-eav-table", "cdc_eav",
	"-batch-size", "123",
	"-estimated-row-bytes", "456",
}

// wantSharedCDCConfig is the shared block sharedCDCArgs must produce.
func wantSharedCDCConfig() cdc.CDCConfig {
	return cdc.CDCConfig{
		PGHost:                  "pg.example",
		PGPort:                  6543,
		PGUser:                  "cdc-user",
		PGPassword:              "cdc-test-pw",
		PGDB:                    "cdc-db",
		PGSSLMode:               "verify-full",
		DuckDBPath:              "/tmp/cdc.duckdb",
		DuckThreads:             7,
		DuckMemLimit:            "3GB",
		QueryTimeout:            42 * time.Second,
		ParquetCompression:      "snappy",
		ParquetCompressionLevel: 9,
		S3Bucket:                "cdc-bucket",
		S3Prefix:                "cdc-prefix",
		S3Endpoint:              "http://minio.example",
		S3Region:                "eu-west-3",
		S3UseSSL:                false,
		S3UsePath:               true,
		ManifestPrefix:          "cdc-manifests",
		ManifestTemplate:        "cdc/{{.SchemaID}}.json",
		EntityMainTable:         "cdc_main",
		EAVDataTable:            "cdc_eav",
		BatchSize:               123,
		EstimatedRowBytes:       456,
	}
}

// TestParseCDCFlagsMapSharedBlock pins the whole config each tool builds from
// the shared block plus its own fields (#445). It compares the full struct, so
// it also catches a field one tool sets that it did not set before the shared
// builder: cdc-init still leaves PGUseIAM, ChangeLogTable, MinRecords,
// MaxAgeMs and MaxBatchBytes to WithDefaults, and cdc-flush leaves
// TargetFileSizeMB unset and MaxBatchSize to WithDefaults.
func TestParseCDCFlagsMapSharedBlock(t *testing.T) {
	t.Run("cdc-flush", func(t *testing.T) {
		opts, err := parseCDCFlushFlags(append(append([]string{}, sharedCDCArgs...),
			"-change-log-table", "cdc_log",
			"-min-records", "11",
			"-max-age-ms", "22",
			"-max-batch-bytes", "33",
			"-pg-use-iam",
		))
		if err != nil {
			t.Fatalf("parse cdc-flush flags: %v", err)
		}
		want := wantSharedCDCConfig()
		want.ChangeLogTable = "cdc_log"
		want.MinRecords = 11
		want.MaxAgeMs = 22
		want.MaxBatchBytes = 33
		want.PGUseIAM = true
		want.Engine = forma.EngineAuroraDSQL
		want.MaxBatchSize = cdc.DefaultMaxBatchSize
		if !reflect.DeepEqual(opts.cfg, want) {
			t.Errorf("cdc-flush config:\n got %+v\nwant %+v", opts.cfg, want)
		}
	})

	t.Run("cdc-init", func(t *testing.T) {
		opts, err := parseCDCInitFlags(append(append([]string{}, sharedCDCArgs...),
			"-schema-registry-table", "registry",
			"-schema-dir", "/tmp/schemas",
			"-target-file-size-mb", "44",
			"-max-batch-size", "55",
		))
		if err != nil {
			t.Fatalf("parse cdc-init flags: %v", err)
		}
		want := wantSharedCDCConfig()
		want.TargetFileSizeMB = 44
		want.MaxBatchSize = 55
		want.Engine = forma.EnginePostgreSQL
		want.ChangeLogTable = "change_log"
		want.MinRecords = cdc.DefaultMinRecords
		want.MaxAgeMs = cdc.DefaultMaxAgeMs
		want.MaxBatchBytes = cdc.DefaultMaxBatchBytes
		if !reflect.DeepEqual(opts.cfg, want) {
			t.Errorf("cdc-init config:\n got %+v\nwant %+v", opts.cfg, want)
		}
	})
}
