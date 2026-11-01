// Package parquetformat is the single source of the Parquet COPY options
// Forma's writers hand DuckDB: the CDC delta and base exporters
// (internal/cdc) and the compaction merge (internal/compaction).
//
// It imports nothing but the standard library on purpose. The cross-version
// storage probe (cmd/duckdbcompat) builds this package against the previous
// DuckDB driver as well as the current one (scripts/duckdb_storage_compat.sh),
// so a dependency here would have to resolve under both module graphs. The
// probe writing its files through these options is what ties its verdict to
// the files production writes.
package parquetformat

import (
	"fmt"
	"strings"
)

const (
	// Version is the PARQUET_VERSION every writer requests.
	Version = "V2"
	// DefaultCompression is the codec a writer uses when its config names none.
	DefaultCompression = "zstd"
	// DefaultCompressionLevel is the codec level used when none is configured.
	DefaultCompressionLevel = 3
)

// CopyOptions renders the option list of a DuckDB COPY ... TO statement.
func CopyOptions(version, compression string, level int) string {
	return fmt.Sprintf(
		"FORMAT PARQUET, PARQUET_VERSION %s, COMPRESSION '%s', COMPRESSION_LEVEL %d",
		version,
		strings.ToUpper(compression),
		level,
	)
}

// DefaultCopyOptions is CopyOptions with every default applied: the options
// of a CDC export whose config sets no codec, and of every compaction merge.
func DefaultCopyOptions() string {
	return CopyOptions(Version, DefaultCompression, DefaultCompressionLevel)
}
