package queryoptimizer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
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

// ...existing code...
// Optimizer coordinates plan generation.
type Optimizer struct{}

// New constructs a new Optimizer instance.
func New() *Optimizer {
	return &Optimizer{}
}

// GeneratePlan builds the SQL plan for the provided input.
func (o *Optimizer) GeneratePlan(ctx context.Context, in *Input) (*Plan, error) {
	if in == nil {
		return nil, fmt.Errorf("optimizer input cannot be nil")
	}
	if in.Tables.EntityMain == "" || in.Tables.EAVData == "" {
		return nil, fmt.Errorf("table names must be provided")
	}
	if in.SchemaID <= 0 {
		return nil, fmt.Errorf("schema id must be positive")
	}

	zap.S().Infow("optimizer inputs", "entityMainTable", in.Tables.EntityMain, "eavTable", in.Tables.EAVData, "schemaID", in.SchemaID)

	// Initialize query builder with SchemaID as $1
	qb := &queryBuilder{
		schemaID: in.SchemaID,
		args:     []any{in.SchemaID},
		argCount: 1,
	}

	// Build filter conditions
	// We currently default to Main-driven strategy (scanning entity_main)
	// because it supports all boolean logic combinations (AND/OR/NOT) and mixed Main/EAV predicates.
	// TODO: Implement EAV-driven strategy for pure-EAV queries with high selectivity.
	filterSQL := "1=1"
	if in.Filter != nil {
		var err error
		filterSQL, err = o.buildFilterSQL(in.Filter, in.Tables, qb)
		if err != nil {
			return nil, fmt.Errorf("failed to build filter SQL: %w", err)
		}
	}

	// Build sort clauses and necessary joins
	sortSQL, sortJoins := o.buildSortSQL(in.SortKeys, in.Tables, qb)

	// Pagination
	limit := in.Pagination.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := in.Pagination.Offset
	if offset < 0 {
		offset = 0
	}
	limitParam := qb.addArg(limit)
	offsetParam := qb.addArg(offset)

	// Build the CTE-based query
	// Structure:
	// 1. anchor: Filter rows from entity_main (Main-driven)
	// 2. sorted: Join sort values (if EAV) and apply ORDER BY + LIMIT
	// 3. final:  Join back to entity_main and EAV to get full projection
	sql := fmt.Sprintf(`
WITH anchor AS (
	SELECT t.row_id
	FROM %s t
	WHERE t.schema_id = $1 AND %s
),
sorted AS (
	SELECT
		a.row_id
		%s
	FROM anchor a
	%s
	ORDER BY %s
	LIMIT %s OFFSET %s
)
SELECT
	t.*, e_all.attr_id, e_all.array_indices, e_all.value_text, e_all.value_numeric
FROM sorted s
JOIN %s t ON t.schema_id = $1 AND t.row_id = s.row_id
LEFT JOIN LATERAL (
	SELECT attr_id, array_indices, value_text, value_numeric
	FROM %s e
	WHERE e.schema_id = $1 AND e.row_id = s.row_id
) e_all ON TRUE
ORDER BY %s`,
		in.Tables.EntityMain, // anchor FROM
		filterSQL,            // anchor WHERE
		"",                   // TODO: Add sort columns to SELECT if needed for debugging, but ORDER BY is enough
		sortJoins,            // sorted JOINs (LATERAL for EAV sorts)
		sortSQL,              // sorted ORDER BY
		limitParam,           // LIMIT
		offsetParam,          // OFFSET
		in.Tables.EntityMain, // final JOIN Main
		in.Tables.EAVData,    // final LATERAL EAV
		sortSQL,              // final ORDER BY
	)

	explain := PlanExplain{
		Driver: "PostgreSQL (Main-Driven)",
		MainFilters: []string{
			fmt.Sprintf("schema_id=%d", in.SchemaID),
			filterSQL,
		},
		SortStrategy: sortSQL,
	}

	return &Plan{
		SQL:     sql,
		Params:  qb.args,
		Explain: explain,
	}, nil
}

// queryBuilder helper to manage positional arguments
type queryBuilder struct {
	schemaID int16
	args     []any
	argCount int
}

func (b *queryBuilder) addArg(val any) string {
	b.argCount++
	b.args = append(b.args, val)
	return fmt.Sprintf("$%d", b.argCount)
}

// buildFilterSQL converts a FilterNode tree into SQL WHERE conditions
func (o *Optimizer) buildFilterSQL(node *FilterNode, tables StorageTables, qb *queryBuilder) (string, error) {
	if node == nil {
		return "", nil
	}

	// Leaf node
	if node.Predicate != nil {
		return o.buildPredicateSQL(node.Predicate, tables, qb)
	}

	// Composite node
	if len(node.Children) > 0 {
		var clauses []string
		for _, child := range node.Children {
			sql, err := o.buildFilterSQL(child, tables, qb)
			if err != nil {
				return "", err
			}
			if sql != "" {
				clauses = append(clauses, fmt.Sprintf("(%s)", sql))
			}
		}

		if len(clauses) == 0 {
			return "", nil
		}

		joiner := " AND "
		if node.Logic == LogicOpOr {
			joiner = " OR "
		}
		return strings.Join(clauses, joiner), nil
	}

	return "", nil
}

// buildPredicateSQL converts a single Predicate into SQL
func (o *Optimizer) buildPredicateSQL(pred *Predicate, tables StorageTables, qb *queryBuilder) (string, error) {
	if pred == nil {
		return "", nil
	}

	// Case 1: Main Table Attribute
	if pred.Storage == StorageTargetMain {
		if pred.Column == nil {
			return "", fmt.Errorf("missing column info for main attribute %s", pred.AttributeName)
		}
		return o.buildMainPredicate(pred, qb)
	}

	// Case 2: EAV Attribute
	if pred.Storage == StorageTargetEAV {
		return o.buildEAVPredicate(pred, tables.EAVData, qb)
	}

	return "", fmt.Errorf("unknown storage target for attribute %s", pred.AttributeName)
}

func (o *Optimizer) buildMainPredicate(pred *Predicate, qb *queryBuilder) (string, error) {
	colName := "t." + pred.Column.Name

	// Handle Fallback Logic
	switch pred.Fallback {
	case AttributeFallbackNumericToDouble:
		// Rewrite equality to range for floating point comparison
		if pred.Operator == PredicateOpEquals {
			val, ok := toFloat64(pred.Value)
			if !ok {
				return "", fmt.Errorf("invalid numeric value for fallback")
			}
			epsilon := 0.00001 // Configurable?
			minParam := qb.addArg(val - epsilon)
			maxParam := qb.addArg(val + epsilon)
			return fmt.Sprintf("%s >= %s AND %s <= %s", colName, minParam, colName, maxParam), nil
		}
	// For other operators, use standard comparison but cast value if needed
	// (Assuming DB handles int vs float comparison fine, but we might need to ensure param is float)
	case AttributeFallbackBoolToText:
		// Rewrite bool value to "true"/"false" string
		boolVal, ok := pred.Value.(bool)
		if !ok {
			return "", fmt.Errorf("invalid bool value for fallback")
		}
		strVal := "false"
		if boolVal {
			strVal = "true"
		}
		param := qb.addArg(strVal)
		return fmt.Sprintf("%s %s %s", colName, pred.Operator, param), nil
	}

	// Handle date values based on column encoding
	convertedValue := pred.Value
	if pred.ValueType == forma.ValueTypeDate {
		if pred.Column != nil && pred.Column.Encoding != "" {
			convertedValue = convertDateValueForStorage(pred.Value, pred.Column.Encoding)
		}
	}

	// Standard handling
	param := qb.addArg(convertedValue)
	return fmt.Sprintf("%s %s %s", colName, pred.Operator, param), nil
}

// convertDateValueForStorage converts a time.Time value to the appropriate storage format
// based on the column encoding.
func convertDateValueForStorage(value any, encoding string) any {
	timeVal, ok := value.(time.Time)
	if !ok {
		return value
	}

	switch encoding {
	case "unix_ms":
		return timeVal.UnixMilli()
	case "iso8601":
		return timeVal.Format(time.RFC3339)
	default:
		// For other encodings or no encoding, return unix ms as default
		return timeVal.UnixMilli()
	}
}

func (o *Optimizer) buildEAVPredicate(pred *Predicate, eavTable string, qb *queryBuilder) (string, error) {
	valueColumn := o.getValueColumnName(pred.ValueType)

	// TODO: Handle EAV Fallbacks if any (currently design says EAV is strong typed, but check fallback enum)

	attrIDParam := qb.addArg(pred.AttributeID)
	valParam := qb.addArg(pred.Value)

	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s e WHERE e.schema_id = $1 AND e.row_id = t.row_id AND e.attr_id = %s AND e.%s %s %s)",
		eavTable,
		attrIDParam,
		valueColumn,
		pred.Operator,
		valParam,
	), nil
}

// getValueColumnName returns the appropriate column name for a value type
func (o *Optimizer) getValueColumnName(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeNumeric, forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime, forma.ValueTypeBool:
		return "value_numeric"
	default:
		return "value_text"
	}
}

// buildSortSQL builds the ORDER BY clause and necessary LATERAL JOINs
func (o *Optimizer) buildSortSQL(sortKeys []SortKey, tables StorageTables, qb *queryBuilder) (string, string) {
	if len(sortKeys) == 0 {
		return "t.row_id ASC", ""
	}

	var orderClauses []string
	var joinClauses []string

	// We need to track joined aliases to avoid duplicates if sorting by same attr twice (unlikely but possible)
	// For simplicity, we'll generate a new alias for each sort key that is EAV.

	needMainJoin := false
	eavJoinCount := 0

	for _, key := range sortKeys {
		direction := "ASC"
		if key.Direction == SortDesc {
			direction = "DESC"
		}

		switch key.Storage {
		case StorageTargetMain:
			if key.Column != nil {
				needMainJoin = true
				orderClauses = append(orderClauses, fmt.Sprintf("m.%s %s", key.Column.Name, direction))
			}
		case StorageTargetEAV:
			eavJoinCount++
			alias := fmt.Sprintf("s%d", eavJoinCount)
			valueCol := o.getValueColumnName(key.ValueType)

			// LATERAL JOIN to get the single value for sorting
			// We limit 1 to ensure we don't multiply rows (though EAV should be unique per attr_id? Arrays?)
			// If array, which one to sort by? Usually first or min/max.
			// For now, LIMIT 1.
			attrIDParam := qb.addArg(key.AttributeID)

			joinSQL := fmt.Sprintf(`LEFT JOIN LATERAL (
			SELECT %s as val
			FROM %s e
			WHERE e.schema_id = $1 AND e.row_id = a.row_id AND e.attr_id = %s
			ORDER BY e.array_indices ASC
			LIMIT 1
		) %s ON TRUE`, valueCol, tables.EAVData, attrIDParam, alias)

			joinClauses = append(joinClauses, joinSQL)
			orderClauses = append(orderClauses, fmt.Sprintf("%s.val %s", alias, direction))
		}
	}

	if needMainJoin {
		// Prepend the main join
		mainJoin := fmt.Sprintf("JOIN %s m ON m.schema_id = $1 AND m.row_id = a.row_id", tables.EntityMain)
		joinClauses = append([]string{mainJoin}, joinClauses...)
	}

	orderClauses = append(orderClauses, "a.row_id ASC") // Deterministic tie-breaker

	return strings.Join(orderClauses, ", "), strings.Join(joinClauses, "\n")
}

func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	default:
		return 0, false
	}
}
