package cdc

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
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

// BuildBasePath returns the path of a cdc-init base file:
// {prefix}/{schema}/{minRowID}_{maxRowID}_{fileUUID}.parquet. The row range
// stays in the name for operators and for manifest-reconcile's
// filename/content cross-check; the UUID makes the key write-once. It used
// to be {min}_{max} alone, so a re-run over an unchanged range overwrote an
// object the manifest still listed under the previous run's column and
// checksum stamps — the compactor's pre-merge checksum gate then read a
// healthy system as corrupt, and a failed re-run left the stale stamps
// behind for good (#416). Flush and compaction never reuse a key
// (BuildDeltaPath, BuildMergedBasePath); init now honours the same rule.
// ParseInitBaseStem is the inverse and still accepts the legacy shape.
func BuildBasePath(prefix string, schemaID int16, minRowID, maxRowID, fileUUID string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	return fmt.Sprintf("%s/%d/%s_%s_%s.parquet", trimmed, schemaID, minRowID, maxRowID, fileUUID)
}

// ParseInitBaseStem recognises an init base object's filename stem (the key's
// last path segment without ".parquet") and returns the row range it names.
// Both shapes are init-shaped: the write-once {min}_{max}_{fileUUID} that
// BuildBasePath mints since #416 and the legacy deterministic {min}_{max}
// still present in buckets written before it. Every part must be a UUID —
// the shape drives repair and GC decisions, so anything that merely
// resembles it must not match. Row-range text is returned verbatim; callers
// that compare ranges canonicalise case themselves.
func ParseInitBaseStem(stem string) (minRowID, maxRowID string, ok bool) {
	parts := strings.Split(stem, "_")
	if len(parts) != 2 && len(parts) != 3 {
		return "", "", false
	}
	for _, p := range parts {
		if uuid.Validate(p) != nil {
			return "", "", false
		}
	}
	return parts[0], parts[1], true
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
