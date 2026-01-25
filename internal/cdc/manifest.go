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
