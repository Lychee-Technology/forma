package forma

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type DataRecord struct {
	SchemaName string         `json:"schema_name"`
	RowID      uuid.UUID      `json:"row_id"`
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
	Attribute string    `json:"attribute"`
	SortOrder SortOrder `json:"sort_order,omitempty"`
}

// TableNames generates the table names for a specific client and project
type TableNames struct {
	SchemaRegistry string `json:"schemaRegistry"`
	EntityMain     string `json:"entityMain"`
	EAVData        string `json:"eavData"`
	ChangeLog      string `json:"changeLog"`
}

type FilterField string

const (
	FilterFieldAttributeName FilterField = "attr_name"
	FilterFieldValueText     FilterField = "value_text"
	FilterFieldValueNumeric  FilterField = "value_numeric"
	FilterFieldRowID         FilterField = "row_id"
	FilterFieldSchemaName    FilterField = "schema_name"
)

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
	Atomic     bool              `json:"atomic"` // Request all-or-nothing execution; may be rejected when unsupported.
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
	SchemaName   string    `json:"schema_name" validate:"required"`
	Page         int       `json:"page" validate:"min=1"`
	ItemsPerPage int       `json:"items_per_page" validate:"min=1,max=100"`
	Condition    Condition `json:"-"` // Custom unmarshal, can be CompositeCondition or KvCondition
	SortBy       []string  `json:"sort_by,omitempty"`
	SortOrder    SortOrder `json:"sort_order,omitempty"`
	// Sort carries per-key sort directions (#240). Mutually exclusive with
	// SortBy/SortOrder; an entry's empty SortOrder defaults to asc.
	Sort      []OrderBy              `json:"sort,omitempty"`
	RowID     *uuid.UUID             `json:"row_id,omitempty"` // For entity-specific operations
	Attrs     []string               `json:"attrs,omitempty"`  // Attributes to return (field projection)
	Federated *FederatedQueryRequest `json:"federated,omitempty"`
}

// FederatedQueryRequest carries optional hints for routing QueryRequest through the
// federated repository path while preserving the normal API surface for callers that
// do not need DuckDB/S3-backed reads.
type FederatedQueryRequest struct {
	Enabled                  bool     `json:"enabled,omitempty"`
	PreferredTiers           []string `json:"preferred_tiers,omitempty"`
	PreferHot                bool     `json:"prefer_hot,omitempty"`
	UseMainAsAnchor          bool     `json:"use_main_as_anchor,omitempty"`
	S3ParquetPathTemplate    string   `json:"s3_parquet_path_template,omitempty"`
	AllowPartialDegradedMode bool     `json:"allow_partial_degraded_mode,omitempty"`
	IncludeExecutionPlan     bool     `json:"include_execution_plan,omitempty"`
	ConsistencyMode          string   `json:"consistency_mode,omitempty"`
}

func unmarshalConditionField(data []byte) (Condition, bool, error) {
	var aux struct {
		Condition json.RawMessage `json:"condition,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return nil, false, err
	}
	if len(aux.Condition) == 0 || string(aux.Condition) == "null" {
		return nil, false, nil
	}

	cond, err := unmarshalCondition(aux.Condition)
	if err != nil {
		return nil, false, err
	}
	return cond, true, nil
}

func marshalWithConditionField(base any, condition Condition) ([]byte, error) {
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	if condition == nil {
		return baseJSON, nil
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(baseJSON, &payload); err != nil {
		return nil, err
	}

	conditionJSON, err := json.Marshal(condition)
	if err != nil {
		return nil, err
	}
	payload["condition"] = conditionJSON
	return json.Marshal(payload)
}

// UnmarshalJSON implements custom JSON unmarshaling for QueryRequest.
// It allows the Condition field to be either a CompositeCondition or KvCondition.
func (r *QueryRequest) UnmarshalJSON(data []byte) error {
	type Alias QueryRequest
	if err := json.Unmarshal(data, (*Alias)(r)); err != nil {
		return err
	}

	cond, hasCondition, err := unmarshalConditionField(data)
	if err != nil {
		return err
	}
	if hasCondition {
		r.Condition = cond
	}

	return nil
}

// MarshalJSON implements custom JSON marshaling for QueryRequest.
func (r QueryRequest) MarshalJSON() ([]byte, error) {
	type Alias QueryRequest
	return marshalWithConditionField((*Alias)(&r), r.Condition)
}

// CrossSchemaRequest represents a cross-schema search request
type CrossSchemaRequest struct {
	SchemaNames  []string  `json:"schema_names" validate:"required"`
	SearchTerm   string    `json:"search_term" validate:"required"`
	Page         int       `json:"page" validate:"min=1"`
	ItemsPerPage int       `json:"items_per_page" validate:"min=1,max=100"`
	Condition    Condition `json:"-"`               // Custom unmarshal, can be CompositeCondition or KvCondition
	Attrs        []string  `json:"attrs,omitempty"` // Attributes to return (field projection)
}

// UnmarshalJSON implements custom JSON unmarshaling for CrossSchemaRequest.
// It allows the Condition field to be either a CompositeCondition or KvCondition.
func (r *CrossSchemaRequest) UnmarshalJSON(data []byte) error {
	type Alias CrossSchemaRequest
	if err := json.Unmarshal(data, (*Alias)(r)); err != nil {
		return err
	}

	cond, hasCondition, err := unmarshalConditionField(data)
	if err != nil {
		return err
	}
	if hasCondition {
		r.Condition = cond
	}

	return nil
}

// MarshalJSON implements custom JSON marshaling for CrossSchemaRequest.
func (r CrossSchemaRequest) MarshalJSON() ([]byte, error) {
	type Alias CrossSchemaRequest
	return marshalWithConditionField((*Alias)(&r), r.Condition)
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
	// ExecutionPlan is populated only for federated requests that set
	// federated.include_execution_plan; it reports the route the engine
	// actually took (DuckDB vs Postgres-only) and per-tier sources so callers
	// can distinguish federated reads from hot-path reads without guessing.
	ExecutionPlan *ExecutionPlan `json:"execution_plan,omitempty"`
	// Partial marks a response answered from an incomplete data surface
	// (#348); currently the only reason is the #251 corrupt-parquet
	// exclusion. Nil/omitted for complete answers. Unlike ExecutionPlan it
	// does not require federated.include_execution_plan.
	Partial *PartialResultInfo `json:"partial,omitempty"`
}

// ExecutionPlan is the JSON-serializable projection of the engine's internal
// execution plan surfaced on QueryResult. It carries only safe routing/tier
// metadata an external caller needs to understand the route.
//
// SECURITY: the internal plan's SQL, bind params, and free-text notes are
// deliberately NOT projected here. Since #306, the database password is
// redacted at the source (plan SQL and failure notes carry password=***REDACTED***),
// but the rendered DuckDB SQL still embeds host/user/dbname and table internals;
// Params carry query arguments, and notes carry storage keys and engine internals.
// Exposing them on the HTTP API would leak internals to any caller of
// advanced_query. Only enum/numeric/static-string fields are surfaced. It also
// imports nothing so it can live in the public API.
type ExecutionPlan struct {
	Routing ExecutionRouting  `json:"routing"`
	Sources []ExecutionSource `json:"sources,omitempty"`
	Merge   *ExecutionMerge   `json:"merge,omitempty"`
	Timings map[string]int64  `json:"timings,omitempty"`
}

// ExecutionRouting reports the routing decision the engine committed to.
type ExecutionRouting struct {
	UsedDuckDB bool     `json:"used_duckdb"`
	Tiers      []string `json:"tiers,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

// ExecutionSource describes one physical data source (tier) the plan read from.
// It intentionally omits the raw SQL and bind params (see ExecutionPlan).
type ExecutionSource struct {
	Tier              string `json:"tier"`
	Engine            string `json:"engine,omitempty"`
	RowEstimate       int64  `json:"row_estimate,omitempty"`
	ActualRows        int64  `json:"actual_rows,omitempty"`
	PredicatePushdown bool   `json:"predicate_pushdown,omitempty"`
	DurationMs        int64  `json:"duration_ms,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

// ExecutionMerge describes the tier-merge strategy the plan applied.
type ExecutionMerge struct {
	Strategy   string   `json:"strategy,omitempty"`
	PreferHot  bool     `json:"prefer_hot,omitempty"`
	DedupKeys  []string `json:"dedup_keys,omitempty"`
	DurationMs int64    `json:"duration_ms,omitempty"`
}

// PartialResultInfo marks a QueryResult whose page was answered from a
// deliberately reduced data surface. It is the sanctioned HTTP-visible
// partial signal (#348): the #251 corrupt-parquet exclusion note lives in
// internal plan Notes, which never cross the HTTP boundary (#301/#306), so
// without this field an API consumer sees fewer rows and a smaller
// total_records with no explanation. Reason is a closed enum, not free text,
// and the excluded objects are identified only by count — storage keys stay
// internal. It is deliberately NOT Routing.Reason: the route does not change
// on a partial read.
type PartialResultInfo struct {
	Reason              string `json:"reason"`
	ExcludedObjectCount int    `json:"excluded_object_count,omitempty"`
}

// PartialReasonCorruptParquetExcluded reports a #251 partial read: one or
// more verification-confirmed corrupt parquet objects were excluded and the
// page was answered from the readable remainder plus the hot tier.
const PartialReasonCorruptParquetExcluded = "corrupt_parquet_excluded"

// CursorQueryResult represents cursor-based pagination results.
type CursorQueryResult struct {
	Data          []*DataRecord `json:"data"`
	NextCursor    string        `json:"next_cursor,omitempty"`
	HasMore       bool          `json:"has_more"`
	ExecutionTime time.Duration `json:"execution_time"`
}

type Logic string

const (
	LogicAnd Logic = "and"
	LogicOr  Logic = "or"
)

// --- 2. Interface (The Core) ---
type Condition interface {
	IsLeaf() bool
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
