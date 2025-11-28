package internal

import (
	"github.com/lychee-technology/forma"
)

// Type aliases for backward compatibility within internal package

// SchemaMetadata aggregates attribute mappings for a schema version.
type SchemaMetadata struct {
	SchemaName    string              `json:"schema_name"`
	SchemaID      int16               `json:"schema_id"`
	SchemaVersion int                 `json:"schema_version"`
	Attributes    []forma.AttributeMetadata `json:"attributes"`
}
