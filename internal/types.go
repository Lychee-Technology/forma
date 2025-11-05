package internal

import (
	"time"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

type Attribute struct {
	SchemaID     int16
	RowID        uuid.UUID  // UUID v7, identifies data row
	AttrID       int16      // Attribute ID from schema definition
	ArrayIndices string     // Comma-separated array indices (e.g., "0", "1,2", or "" for non-arrays)
	ValueText    *string    // For valueType: "text"
	ValueNumeric *float64   // For valueType: "numeric"
	ValueDate    *time.Time // For valueType: "date"
	ValueBool    *bool      // For valueType: "bool"
}

type AttributeQuery struct {
	SchemaID int16           `json:"schemaId"`
	Filters  []forma.Filter  `json:"filters"`
	OrderBy  []forma.OrderBy `json:"orderBy"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
}
