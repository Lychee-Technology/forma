package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// PersistentRecord represents a persisted entity row plus EAV attributes.
type PersistentRecord struct {
	SchemaID        int16
	RowID           uuid.UUID
	TextItems       map[string]string
	Int16Items      map[string]int16
	Int32Items      map[string]int32
	Int64Items      map[string]int64
	Float64Items    map[string]float64
	UUIDItems       map[string]uuid.UUID
	CreatedAt       int64
	UpdatedAt       int64
	DeletedAt       *int64
	OtherAttributes []EAVRecord
}

// EAVRecord represents one attribute row in EAV storage.
type EAVRecord struct {
	SchemaID     int16
	RowID        uuid.UUID
	AttrID       int16
	ArrayIndices string
	ValueText    *string
	ValueNumeric *float64
}

// AttributeOrder specifies how to sort by a particular attribute.
type AttributeOrder struct {
	AttrID          int16
	ValueType       forma.ValueType
	SortOrder       forma.SortOrder
	StorageLocation forma.AttributeStorageLocation
	ColumnName      string
	AttrName        string
}

func (ao *AttributeOrder) AttrIDInt() int {
	return int(ao.AttrID)
}

func (ao *AttributeOrder) ValueColumn() string {
	switch ao.ValueType {
	case forma.ValueTypeNumeric,
		forma.ValueTypeInteger,
		forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt,
		forma.ValueTypeBool,
		forma.ValueTypeDate,
		forma.ValueTypeDateTime:
		return "value_numeric"
	default:
		return "value_text"
	}
}

func (ao *AttributeOrder) Desc() bool {
	return ao.SortOrder == forma.SortOrderDesc
}

func (ao *AttributeOrder) IsMainColumn() bool {
	return ao.StorageLocation == forma.AttributeStorageLocationMain && ao.ColumnName != ""
}

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
	RowID        uuid.UUID
	AttrID       int16
	ArrayIndices string
	ValueType    forma.ValueType
	Value        any
}

func (ea *EntityAttribute) Text() (*string, error) {
	if ea.Value == nil {
		return nil, nil
	}
	if ea.ValueType != forma.ValueTypeText {
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
	if ea.ValueType != forma.ValueTypeSmallInt {
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
	if ea.ValueType != forma.ValueTypeInteger {
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
	if ea.ValueType != forma.ValueTypeBigInt {
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
	if ea.ValueType != forma.ValueTypeNumeric {
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
	if ea.ValueType != forma.ValueTypeDate {
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
	if ea.ValueType != forma.ValueTypeDateTime {
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
	if ea.ValueType != forma.ValueTypeUUID {
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
	if ea.ValueType != forma.ValueTypeBool {
		return nil, fmt.Errorf("expected ValueType 'bool', got '%s'", ea.ValueType)
	}
	if v, ok := ea.Value.(bool); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("value is not a bool")
}
