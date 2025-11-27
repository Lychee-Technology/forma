package internal

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ValueType represents the type of value stored in a row of EAV table.
type EAVRecord struct {
	SchemaID     int16
	RowID        uuid.UUID // UUID v7, identifies data row
	AttrID       int16     // Attribute ID from schema definition
	ArrayIndices string    // Comma-separated array indices (e.g., "0", "1,2", or "" for non-arrays)
	ValueText    *string   // For valueType: "text"
	ValueNumeric *float64  // For valueType: "numeric"
}

// AttributeOrder specifies how to sort by a particular attribute.
type AttributeOrder struct {
	AttrID          int16
	ValueType       ValueType
	SortOrder       forma.SortOrder
	StorageLocation AttributeStorageLocation // main or eav
	ColumnName      string                   // main table column name if StorageLocation == main
}

// AttrIDInt returns the attribute ID as an int (for template compatibility).
func (ao *AttributeOrder) AttrIDInt() int {
	return int(ao.AttrID)
}

// ValueColumn returns the EAV table column name for this attribute's value type.
func (ao *AttributeOrder) ValueColumn() string {
	switch ao.ValueType {
	case ValueTypeText:
		return "value_text"
	case ValueTypeNumeric:
		return "value_numeric"
	default:
		return "value_text"
	}
}

// Desc returns true if the sort order is descending.
func (ao *AttributeOrder) Desc() bool {
	return ao.SortOrder == forma.SortOrderDesc
}

// IsMainColumn returns true if the attribute is stored in the main table.
func (ao *AttributeOrder) IsMainColumn() bool {
	return ao.StorageLocation == AttributeStorageLocationMain && ao.ColumnName != ""
}

// MainColumnName returns the column name in the main table.
func (ao *AttributeOrder) MainColumnName() string {
	return ao.ColumnName
}

type AttributeQuery struct {
	SchemaID        int16            `json:"schemaId"`
	Condition       forma.Condition  `json:"condition,omitempty"`
	OrderBy         []forma.OrderBy  `json:"orderBy"`
	AttributeOrders []AttributeOrder `json:"attributeOrders"`
	Limit           int              `json:"limit"`
	Offset          int              `json:"offset"`
}

type EntityAttribute struct {
	SchemaID     int16
	AttrID       int16
	ArrayIndices string
	ValueType    ValueType
	Value        any
}

func (ea *EntityAttribute) Text() (*string, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeText {
		return nil, fmt.Errorf("expected ValueType 'text', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(string); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a string")
}

func (ea *EntityAttribute) SmallInt() (*int16, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeSmallInt {
		return nil, fmt.Errorf("expected ValueType 'smallint', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(int16); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not an int16")
}

func (ea *EntityAttribute) Integer() (*int32, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeInteger {
		return nil, fmt.Errorf("expected ValueType 'integer', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(int32); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not an int32")
}

func (ea *EntityAttribute) BigInt() (*int64, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeBigInt {
		return nil, fmt.Errorf("expected ValueType 'bigint', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(int64); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not an int64")
}

func (ea *EntityAttribute) Numeric() (*float64, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeNumeric {
		return nil, fmt.Errorf("expected ValueType 'numeric', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(float64); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a float64")
}

func (ea *EntityAttribute) Date() (*time.Time, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeDate {
		return nil, fmt.Errorf("expected ValueType 'date', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(time.Time); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a time.Time")
}

func (ea *EntityAttribute) DateTime() (*time.Time, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeDateTime {
		return nil, fmt.Errorf("expected ValueType 'datetime', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(time.Time); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a time.Time")
}

func (ea *EntityAttribute) UUID() (*uuid.UUID, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeUUID {
		return nil, fmt.Errorf("expected ValueType 'uuid', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(uuid.UUID); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a uuid.UUID")
}

func (ea *EntityAttribute) Bool() (*bool, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != ValueTypeBool {
		return nil, fmt.Errorf("expected ValueType 'bool', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(bool); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a bool")
}
