package forma

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type DataRecord struct {
	SchemaName string         `json:"schemaName"`
	RowID      uuid.UUID      `json:"rowId"`
	Attributes map[string]any `json:"attributes"`
}

// FilterType defines supported filter operations
type FilterType string

const (
	FilterEquals      FilterType = "equals"
	FilterNotEquals   FilterType = "not_equals"
	FilterStartsWith  FilterType = "starts_with"
	FilterContains    FilterType = "contains"
	FilterGreaterThan FilterType = "gt"
	FilterLessThan    FilterType = "lt"
	FilterGreaterEq   FilterType = "gte"
	FilterLessEq      FilterType = "lte"
	FilterIn          FilterType = "in"
	FilterNotIn       FilterType = "not_in"
)

// SortOrder defines sort direction
type SortOrder string

const (
	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"
)

type OrderBy struct {
	Field     FilterField `json:"field"`
	SortOrder SortOrder   `json:"sort_order,omitempty"`
}

type FilterField string

const (
	FilterFieldAttributeName  FilterField = "attr_name"
	FilterFieldAttributeValue FilterField = "attr_value"
	FilterFieldRowID          FilterField = "row_id"
	FilterFieldSchemaName     FilterField = "schema_name"
)

// Filter supports all filter types from both modules
type Filter struct {
	Type  FilterType  `json:"type" validate:"required"`
	Value any         `json:"value" validate:"required"`
	Field FilterField `json:"field,omitempty"` // For entity operations
}

// OperationType represents CRUD operations
type OperationType string

const (
	OperationCreate OperationType = "create"
	OperationRead   OperationType = "read"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
	OperationQuery  OperationType = "query"
)

// EntityIdentifier identifies an entity for operations
type EntityIdentifier struct {
	SchemaName string    `json:"schemaName"`
	RowID      uuid.UUID `json:"rowId"`
}

// EntityOperation represents CRUD operations
type EntityOperation struct {
	EntityIdentifier
	Type    OperationType  `json:"type"`
	Data    map[string]any `json:"data,omitempty"`
	Updates map[string]any `json:"updates,omitempty"`
}

// BatchOperation represents batch entity operations
type BatchOperation struct {
	Operations []EntityOperation `json:"operations"`
	Atomic     bool              `json:"atomic"` // Whether to use transactions
}

// BatchResult represents results from batch operations
type BatchResult struct {
	Successful []*DataRecord    `json:"successful"`
	Failed     []OperationError `json:"failed"`
	TotalCount int              `json:"totalCount"`
	Duration   int64            `json:"duration"` // microseconds
}

// OperationError represents an error for a specific operation
type OperationError struct {
	Operation EntityOperation `json:"operation"`
	Error     string          `json:"error"`
	Code      string          `json:"code"`
	Details   map[string]any  `json:"details,omitempty"`
}

// EntityUpdate represents an update operation
type EntityUpdate struct {
	EntityIdentifier
	Updates any `json:"updates"`
}

// Reference represents a reference from one entity to another
type Reference struct {
	SourceSchemaName string        `json:"sourceSchemaName"`
	SourceRowID      uuid.UUID     `json:"sourceRowId"`
	SourceFieldName  string        `json:"sourceFieldName"`
	TargetSchemaName string        `json:"targetSchemaName"`
	TargetRowID      uuid.UUID     `json:"targetRowId"`
	ReferenceType    ReferenceType `json:"referenceType"`
}

// ReferenceType represents the type of reference
type ReferenceType string

const (
	ReferenceTypeSingle ReferenceType = "single"
	ReferenceTypeArray  ReferenceType = "array"
	ReferenceTypeNested ReferenceType = "nested"
)

// QueryRequest represents a pagination query request.
type QueryRequest struct {
	SchemaName   string            `json:"schema_name" validate:"required"`
	Page         int               `json:"page" validate:"min=1"`
	ItemsPerPage int               `json:"items_per_page" validate:"min=1,max=100"`
	Filters      map[string]Filter `json:"filters,omitempty"`
	SortBy       string            `json:"sort_by,omitempty"`
	SortOrder    SortOrder         `json:"sort_order,omitempty"`
	RowID        *uuid.UUID        `json:"row_id,omitempty"` // For entity-specific operations
}

// CursorQueryRequest represents cursor-based pagination input.
type CursorQueryRequest struct {
	SchemaName string            `json:"schema_name" validate:"required"`
	Cursor     string            `json:"cursor,omitempty"`
	Limit      int               `json:"limit" validate:"min=1,max=100"`
	Filters    map[string]Filter `json:"filters,omitempty"`
}

// CrossSchemaRequest represents a cross-schema search query.
type CrossSchemaRequest struct {
	SchemaNames  []string          `json:"schema_names" validate:"required,min=1"`
	SearchTerm   string            `json:"search_term" validate:"required"`
	Page         int               `json:"page" validate:"min=1"`
	ItemsPerPage int               `json:"items_per_page" validate:"min=1,max=100"`
	Filters      map[string]Filter `json:"filters,omitempty"`
}

// QueryResult represents paginated query results.
type QueryResult struct {
	Data          []*DataRecord `json:"data"`
	TotalRecords  int           `json:"total_records"`
	TotalPages    int           `json:"total_pages"`
	CurrentPage   int           `json:"current_page"`
	ItemsPerPage  int           `json:"items_per_page"`
	HasNext       bool          `json:"has_next"`
	HasPrevious   bool          `json:"has_previous"`
	ExecutionTime time.Duration `json:"execution_time"`
}

// CursorQueryResult represents cursor-based pagination results.
type CursorQueryResult struct {
	Data          []*DataRecord `json:"data"`
	NextCursor    string        `json:"next_cursor,omitempty"`
	HasMore       bool          `json:"has_more"`
	ExecutionTime time.Duration `json:"execution_time"`
}

// AdvancedQueryRequest represents a request payload for condition-based advanced queries.
type AdvancedQueryRequest struct {
	SchemaName   string              `json:"schema_name" validate:"required"`
	Condition    *CompositeCondition `json:"condition" validate:"required"`
	Page         int                 `json:"page"`
	ItemsPerPage int                 `json:"items_per_page"`
}

type JSONSchema struct {
	ID         int16                      `json:"id"`
	Name       string                     `json:"name"`
	Version    int                        `json:"version"`
	Schema     string                     `json:"schema"`
	Properties map[string]*PropertySchema `json:"properties"`
	Required   []string                   `json:"required"`
	CreatedAt  int64                      `json:"created_at"`
}

// PropertySchema defines the schema for a single property
type PropertySchema struct {
	Name       string                     `json:"name"`
	Type       string                     `json:"type"` // "string", "integer", "number", "boolean", "array", "object", "null"
	Format     string                     `json:"format,omitempty"`
	Items      *PropertySchema            `json:"items,omitempty"`
	Properties map[string]*PropertySchema `json:"properties,omitempty"`
	Required   bool                       `json:"required"`
	Default    any                        `json:"default,omitempty"`
	Enum       []any                      `json:"enum,omitempty"`
	Minimum    *float64                   `json:"minimum,omitempty"`
	Maximum    *float64                   `json:"maximum,omitempty"`
	MinLength  *int                       `json:"minLength,omitempty"`
	MaxLength  *int                       `json:"maxLength,omitempty"`
	Pattern    string                     `json:"pattern,omitempty"`
	Relation   *RelationSchema            `json:"x-relation,omitempty"`
	Storage    string                     `json:"x-storage,omitempty"` // "json" for free-form objects
}

// RelationSchema defines reference relationships between objects
type RelationSchema struct {
	Target string `json:"target"` // Target schema name
	Type   string `json:"type"`   // "reference" for foreign key relationships
}

type Logic string

const (
	LogicAnd Logic = "and"
	LogicOr  Logic = "or"
)

// --- 2. Interface (The Core) ---
type Condition interface {
	IsLeaf() bool

	// ToSqlClauses 是将过滤器节点转换为 SQL 子句的核心方法。
	// 它返回一个返回 row_id 集合的 SQL 子句、对应的参数以及一个错误。
	ToSqlClauses(
		schemaID int16,
		cache SchemaAttributeCache,
		paramIndex *int,
	) (sqlClause string, args []any, err error)
}

// --- 3. Composite Condition (Non-Leaf Node) ---
type CompositeCondition struct {
	Logic      Logic       `json:"l"`
	Conditions []Condition `json:"c"`
}

func (c *CompositeCondition) IsLeaf() bool { return false }

// UnmarshalJSON customizes decoding so that nested conditions are turned into the
// appropriate concrete condition implementations.
func (c *CompositeCondition) UnmarshalJSON(data []byte) error {
	type compositeAlias struct {
		Logic      *Logic            `json:"l"`
		Conditions []json.RawMessage `json:"c"`
	}

	var alias compositeAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	if alias.Logic == nil {
		return fmt.Errorf("composite condition missing logic")
	}

	switch *alias.Logic {
	case LogicAnd, LogicOr:
		c.Logic = *alias.Logic
	default:
		return fmt.Errorf("unknown logic: %s", *alias.Logic)
	}

	if len(alias.Conditions) == 0 {
		c.Conditions = nil
		return nil
	}

	conditions := make([]Condition, 0, len(alias.Conditions))
	for _, raw := range alias.Conditions {
		child, err := unmarshalCondition(raw)
		if err != nil {
			return err
		}
		conditions = append(conditions, child)
	}

	c.Conditions = conditions
	return nil
}

// --- 4. KvCondition (Leaf Node) ---
type KvCondition struct {
	Attr  string `json:"a"`
	Value string `json:"v"`
}

func (kv *KvCondition) IsLeaf() bool { return true }

// UnmarshalJSON ensures short-hand keys are present.
func (kv *KvCondition) UnmarshalJSON(data []byte) error {
	type kvAlias struct {
		Attr  string `json:"a"`
		Value string `json:"v"`
	}

	var alias kvAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	if alias.Attr == "" {
		return fmt.Errorf("kv condition missing attr 'a'")
	}

	if alias.Value == "" {
		return fmt.Errorf("kv condition missing value 'v'")
	}

	kv.Attr = alias.Attr
	kv.Value = alias.Value
	return nil
}

// unmarshalCondition inspects the incoming JSON payload and instantiates the
// correct Condition implementation (composite vs kv). This allows us to unmarshal
// nested condition trees directly from JSON inputs.
func unmarshalCondition(data []byte) (Condition, error) {
	var discriminator struct {
		Logic *Logic  `json:"l"`
		Attr  *string `json:"a"`
	}

	if err := json.Unmarshal(data, &discriminator); err != nil {
		return nil, err
	}

	if discriminator.Logic != nil {
		var composite CompositeCondition
		if err := json.Unmarshal(data, &composite); err != nil {
			return nil, err
		}
		return &composite, nil
	}

	if discriminator.Attr != nil {
		var kv KvCondition
		if err := json.Unmarshal(data, &kv); err != nil {
			return nil, err
		}
		return &kv, nil
	}

	return nil, fmt.Errorf("invalid condition payload: expected 'logic' or 'attr'")
}
