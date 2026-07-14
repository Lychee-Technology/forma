package cdc

import (
	"fmt"
	"strings"
)

// ManifestEntry describes a parquet file tracked for a schema.
// It can represent either a base or delta file.
type ManifestEntry struct {
	Path        string `json:"path"`
	MinRowID    string `json:"min_row_id,omitempty"`
	MaxRowID    string `json:"max_row_id,omitempty"`
	MinTimeSlot int64  `json:"min_time_slot,omitempty"`
	MaxTimeSlot int64  `json:"max_time_slot,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	Level       string `json:"level,omitempty"` // "base" or "delta"
}

// Manifest captures the files for a schema and their stats.
type Manifest struct {
	SchemaID int16           `json:"schema_id"`
	Base     []ManifestEntry `json:"base,omitempty"`
	Delta    []ManifestEntry `json:"delta,omitempty"`
}

// BuildDeltaPath returns the canonical delta file path for a schema.
func BuildDeltaPath(prefix string, schemaID int16, fileUUID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/%s.parquet", trimmed, schemaID, fileUUID)
}

// BuildTempPath returns the temp path for a delta file.
func BuildTempPath(prefix string, schemaID int16, fileUUID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/_tmp/%s.parquet", trimmed, schemaID, fileUUID)
}

// BuildBasePath returns the canonical base file path for a schema.
// Base files use min/max row_id naming to indicate the row range covered.
func BuildBasePath(prefix string, schemaID int16, minRowID, maxRowID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/%s_%s.parquet", trimmed, schemaID, minRowID, maxRowID)
}

// BuildBaseTempPath returns the temp path for a base file during init.
func BuildBaseTempPath(prefix string, schemaID int16, fileUUID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/_tmp/%s.parquet", trimmed, schemaID, fileUUID)
}

// BuildMergedBasePath returns the path of a compaction-rewritten base file.
// Merged bases are UUID-named, never {min}_{max}: repeated rewrites over the
// same row range would otherwise reuse a key and overwrite an object still
// listed in the manifest under concurrent readers (#188). The row-ID range
// lives in the manifest FileEntry instead.
func BuildMergedBasePath(prefix string, schemaID int16, fileUUID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/base-%s.parquet", trimmed, schemaID, fileUUID)
}
