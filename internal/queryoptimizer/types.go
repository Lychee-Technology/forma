package queryoptimizer

import (
	"errors"

	"github.com/lychee-technology/forma"
)

// ErrNotImplemented is returned until the optimizer pipeline is fully implemented.
var ErrNotImplemented = errors.New("query optimizer plan generation not implemented")

// AttributeFallbackKind describes lossy storage behaviors that require rewrites.
type AttributeFallbackKind string

const (
	AttributeFallbackNone            AttributeFallbackKind = "none"
	AttributeFallbackNumericToDouble AttributeFallbackKind = "numeric_to_double"
	AttributeFallbackDateToDouble    AttributeFallbackKind = "date_to_double"
	AttributeFallbackDateToText      AttributeFallbackKind = "date_to_text"
	AttributeFallbackBoolToDouble    AttributeFallbackKind = "bool_to_double"
	AttributeFallbackBoolToText      AttributeFallbackKind = "bool_to_text"
)

// ColumnRef references a concrete hot-column binding.
type ColumnRef struct {
	Name     string
	Type     string
	Encoding string
}

// AttributeBinding exposes metadata needed for normalization.
type AttributeBinding struct {
	AttributeName string
	AttributeID   int16
	ValueType     forma.ValueType
	Storage       StorageTarget
	Column        *ColumnRef
	Fallback      AttributeFallbackKind
	InsideArray   bool
}

// AttributeCatalog is a lookup map for attributes.
type AttributeCatalog map[string]AttributeBinding

// StorageTables captures the physical table names passed in from calling layers.
type StorageTables struct {
	EntityMain string
	EAVData    string
}

// PredicateOp enumerates normalized predicate operators.
type PredicateOp string

// Supported predicate operators.
const (
	PredicateOpEquals      PredicateOp = "="
	PredicateOpNotEquals   PredicateOp = "!="
	PredicateOpGreaterThan PredicateOp = ">"
	PredicateOpGreaterEq   PredicateOp = ">="
	PredicateOpLessThan    PredicateOp = "<"
	PredicateOpLessEq      PredicateOp = "<="
	PredicateOpLike        PredicateOp = "LIKE"
)

// PatternKind describes LIKE pattern semantics for text predicates.
type PatternKind string

const (
	PatternKindNone     PatternKind = ""
	PatternKindPrefix   PatternKind = "prefix"
	PatternKindContains PatternKind = "contains"
)

// Predicate describes a normalized filter expression.
type Predicate struct {
	AttributeName string
	AttributeID   int16
	ValueType     forma.ValueType
	Operator      PredicateOp
	Value         any
	Storage       StorageTarget
	Column        *ColumnRef
	Pattern       PatternKind
	Fallback      AttributeFallbackKind
	InsideArray   bool
}

// StorageTarget indicates whether the attribute lives in the main table or EAV.
type StorageTarget int

const (
	StorageTargetUnknown StorageTarget = iota
	StorageTargetMain
	StorageTargetEAV
)

// LogicOp represents boolean connectors in normalized filters.
type LogicOp string

const (
	LogicOpAnd LogicOp = "AND"
	LogicOpOr  LogicOp = "OR"
)

// FilterNode preserves the original boolean structure of the request.
type FilterNode struct {
	Logic     LogicOp
	Predicate *Predicate
	Children  []*FilterNode
}

// SortDirection enumerates normalized sort orderings.
type SortDirection string

const (
	SortAsc  SortDirection = "ASC"
	SortDesc SortDirection = "DESC"
)

// SortKey represents a normalized ordering requirement.
type SortKey struct {
	AttributeName string
	AttributeID   int16
	ValueType     forma.ValueType
	Direction     SortDirection
	Storage       StorageTarget
	Column        *ColumnRef
	Fallback      AttributeFallbackKind
}

// Pagination captures validated limit/offset.
type Pagination struct {
	Limit  int
	Offset int
}

// Input bundles all data required to build a SQL plan.
type Input struct {
	SchemaID   int16
	SchemaName string
	Tables     StorageTables
	Filter     *FilterNode
	SortKeys   []SortKey
	Pagination Pagination
}

// PlanExplain stores human-readable diagnostics for logging.
type PlanExplain struct {
	Driver       string
	MainFilters  []string
	EAVFilters   []string
	SortStrategy string
}

// Plan represents the executable statement plus diagnostics.
type Plan struct {
	SQL      string
	Params   []any
	Explain  PlanExplain
	Metadata any // placeholder for future metadata payloads
}
