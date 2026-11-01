package parquetformat

import "testing"

// The rendered string is what older DuckDB readers were validated against
// (docs/duckdb-upgrades.md). A change here changes the files every writer
// produces, so it must come with a fresh scripts/duckdb_storage_compat.sh run.
func TestDefaultCopyOptions(t *testing.T) {
	want := "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 3"
	if got := DefaultCopyOptions(); got != want {
		t.Fatalf("DefaultCopyOptions() = %q, want %q", got, want)
	}
}

func TestCopyOptionsUppercasesCodec(t *testing.T) {
	want := "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION 'SNAPPY', COMPRESSION_LEVEL 1"
	if got := CopyOptions("V2", "snappy", 1); got != want {
		t.Fatalf("CopyOptions() = %q, want %q", got, want)
	}
}
