package httpapi

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
)

// TestErrorClassResolvesSentinelsThroughWraps pins that the public error-class
// vocabulary is resolved with errors.Is through arbitrary wrap depth, never by
// matching message text — the #301 body carries no message text, so the token is
// the only thing a client can discriminate on.
func TestErrorClassResolvesSentinelsThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "parquet set inconsistent behind two wraps",
			err: fmt.Errorf("execute duckdb query: %w: %w",
				&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"base/k.parquet"}},
				fmt.Errorf("IO Error: driver text")),
			want: errorClassParquetSetInconsistent,
		},
		{
			name: "no parquet paths",
			err:  fmt.Errorf("resolve paths: %w", &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true}),
			want: errorClassNoParquetPaths,
		},
		{
			name: "manifest schema mismatch",
			err: fmt.Errorf("manifest parquet source: %w",
				&forma.ManifestSchemaMismatchError{RequestedSchemaID: 1, ManifestSchemaID: 2, Path: "manifests/1.json"}),
			want: errorClassManifestSchemaMismatch,
		},
		{
			name: "unclassified falls back to internal",
			err:  fmt.Errorf("db timeout"),
			want: errorClassInternal,
		},
		{
			name: "federated read failure is not given its own token",
			err:  fmt.Errorf("execute duckdb query: federated read failed: IO Error"),
			want: errorClassInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorClass(tt.err); got != tt.want {
				t.Fatalf("errorClass = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPublicErrorMessageCarriesNoDetail pins that the only prose a redacted 5xx
// body carries is a fixed string per class — no schema id, no path, no driver
// text.
func TestPublicErrorMessageCarriesNoDetail(t *testing.T) {
	tests := map[string]string{
		errorClassParquetSetInconsistent: "internal read error",
		errorClassNoParquetPaths:         "internal read error",
		errorClassManifestSchemaMismatch: "internal read error",
		errorClassInternal:               "internal error",
	}

	for class, want := range tests {
		if got := publicErrorMessage(class); got != want {
			t.Fatalf("publicErrorMessage(%q) = %q, want %q", class, got, want)
		}
	}
}
