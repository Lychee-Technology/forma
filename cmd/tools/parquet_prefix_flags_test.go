package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// globPrefixes are parquet prefixes the writers would store as literal S3 keys
// but DuckDB's read_parquet would expand (#527).
var globPrefixes = []string{"data/*", "data/**", "data?", "data[1]", "*"}

// TestWriterToolsRejectGlobPrefix pins #527 on the CLI side: every tool that
// writes, lists or reads parquet under a prefix flag refuses a glob
// metacharacter at flag parse, before any connection is opened, with the
// same forma.ConfigError rule the server applies to duckdb.s3DataPrefix, so
// a writer never produces keys the reader would glob-expand.
func TestWriterToolsRejectGlobPrefix(t *testing.T) {
	parsers := []struct {
		name  string
		flag  string
		parse func(prefix string) error
	}{
		{"cdc-flush", "s3-prefix", func(p string) error {
			_, err := parseCDCFlushFlags([]string{"--s3-bucket", "bkt", "--s3-prefix", p})
			return err
		}},
		{"cdc-init", "s3-prefix", func(p string) error {
			_, err := parseCDCInitFlags([]string{"--s3-bucket", "bkt", "--schema-registry-table", "t", "--schema-dir", "d", "--s3-prefix", p})
			return err
		}},
		{"cdc-init", "delta-prefix", func(p string) error {
			_, err := parseCDCInitFlags([]string{"--s3-bucket", "bkt", "--schema-registry-table", "t", "--schema-dir", "d", "--delta-prefix", p})
			return err
		}},
		{"compactor", "data-prefix", func(p string) error {
			_, err := parseCompactorFlags([]string{"--schema-id", "7", "--s3-bucket", "bkt", "--data-prefix", p})
			return err
		}},
		{"manifest-reconcile", "data-prefix", func(p string) error {
			_, err := parseReconcileFlags([]string{"--s3-bucket", "bkt", "--schema-registry-table", "t", "--data-prefix", p})
			return err
		}},
		{"baseline-schema", "data-prefix", func(p string) error {
			flags := baselineParquetFlags{s3: s3Flags{bucket: "bkt"}, manifestTemplate: "manifest/{{.SchemaID}}.json", dataPrefix: p}
			return flags.validate()
		}},
	}
	for _, tc := range parsers {
		t.Run(tc.name+"/"+tc.flag, func(t *testing.T) {
			for _, prefix := range globPrefixes {
				err := tc.parse(prefix)
				if err == nil {
					t.Fatalf("--%s %q must be rejected", tc.flag, prefix)
				}
				var configErr *forma.ConfigError
				if !errors.As(err, &configErr) || configErr.Field != tc.flag {
					t.Fatalf("--%s %q: want a ConfigError naming the flag, got %T (%v)", tc.flag, prefix, err, err)
				}
				if !strings.Contains(err.Error(), "--"+tc.flag) || !strings.Contains(err.Error(), "glob metacharacter") {
					t.Errorf("--%s %q: error %q must name the flag and the reason", tc.flag, prefix, err.Error())
				}
			}
			for _, prefix := range []string{"", "data", "delta/v2", "base_2026-11"} {
				if err := tc.parse(prefix); err != nil {
					t.Fatalf("--%s %q must parse: %v", tc.flag, prefix, err)
				}
			}
		})
	}
}
