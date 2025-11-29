package queryoptimizer

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
)

// NormalizerOptions controls pagination defaults when requests omit values.
type NormalizerOptions struct {
	DefaultLimit int
	MaxLimit     int
}

// NormalizeQuery converts a QueryRequest into the optimizer Input IR.
func NormalizeQuery(req *forma.QueryRequest, schemaID int16, schemaName string, tables StorageTables, attrs AttributeCatalog, opts NormalizerOptions) (*Input, error) {
	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil")
	}
	if req.Condition == nil {
		return nil, fmt.Errorf("query condition is required")
	}
	if len(attrs) == 0 {
		return nil, fmt.Errorf("attribute catalog cannot be empty")
	}
	if schemaName == "" {
		schemaName = req.SchemaName
	}
	if schemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	limit := req.ItemsPerPage
	if limit < 1 {
		limit = opts.DefaultLimit
	}
	if limit < 1 {
		limit = 1
	}
	if opts.MaxLimit > 0 && limit > opts.MaxLimit {
		limit = opts.MaxLimit
	}

	page := req.Page
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	filter, err := normalizeConditionTree(req.Condition, attrs)
	if err != nil {
		return nil, err
	}

	sortDir := SortAsc
	if strings.EqualFold(string(req.SortOrder), string(forma.SortOrderDesc)) {
		sortDir = SortDesc
	}

	sortKeys := make([]SortKey, 0, len(req.SortBy))
	for _, attrName := range req.SortBy {
		attrName = strings.TrimSpace(attrName)
		if attrName == "" {
			return nil, fmt.Errorf("sort attribute name cannot be empty")
		}

		meta, ok := attrs[attrName]
		if !ok {
			return nil, fmt.Errorf("cannot sort by unknown attribute '%s'", attrName)
		}

		key := SortKey{
			AttributeName: resolvedAttributeName(attrName, meta),
			AttributeID:   meta.AttributeID,
			ValueType:     meta.ValueType,
			Direction:     sortDir,
			Storage:       meta.Storage,
			Column:        meta.Column,
			Fallback:      meta.Fallback,
		}
		sortKeys = append(sortKeys, key)
	}

	return &Input{
		SchemaID:   schemaID,
		SchemaName: schemaName,
		Tables:     tables,
		Filter:     filter,
		SortKeys:   sortKeys,
		Pagination: Pagination{Limit: limit, Offset: offset},
	}, nil
}

func normalizeConditionTree(cond forma.Condition, attrs AttributeCatalog) (*FilterNode, error) {
	switch typed := cond.(type) {
	case *forma.CompositeCondition:
		if len(typed.Conditions) == 0 {
			return nil, fmt.Errorf("composite condition requires children")
		}

		logic, err := convertLogic(typed.Logic)
		if err != nil {
			return nil, err
		}

		children := make([]*FilterNode, 0, len(typed.Conditions))
		for _, child := range typed.Conditions {
			node, err := normalizeConditionTree(child, attrs)
			if err != nil {
				return nil, err
			}
			children = append(children, node)
		}

		return &FilterNode{Logic: logic, Children: children}, nil

	case *forma.KvCondition:
		predicate, err := normalizeKvPredicate(typed, attrs)
		if err != nil {
			return nil, err
		}
		return &FilterNode{Predicate: predicate}, nil

	default:
		return nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}

func normalizeKvPredicate(kv *forma.KvCondition, attrs AttributeCatalog) (*Predicate, error) {
	meta, ok := attrs[kv.Attr]
	if !ok {
		return nil, fmt.Errorf("attribute '%s' not found in schema", kv.Attr)
	}

	opName, valStr := parseOperatorValue(kv.Value)
	predOp, pattern, parsedValue, err := normalizeValue(meta, opName, valStr)
	if err != nil {
		return nil, err
	}

	return &Predicate{
		AttributeName: resolvedAttributeName(kv.Attr, meta),
		AttributeID:   meta.AttributeID,
		ValueType:     meta.ValueType,
		Operator:      predOp,
		Value:         parsedValue,
		Storage:       meta.Storage,
		Column:        meta.Column,
		Pattern:       pattern,
		Fallback:      meta.Fallback,
		InsideArray:   meta.InsideArray,
	}, nil
}

func parseOperatorValue(raw string) (string, string) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) == 1 {
		return "equals", parts[0]
	}
	if parts[0] == "" {
		return "equals", parts[1]
	}
	return parts[0], parts[1]
}

func normalizeValue(meta AttributeBinding, opName, value string) (PredicateOp, PatternKind, any, error) {
	switch opName {
	case "equals", "eq":
		return convertValue(meta, PredicateOpEquals, value)
	case "not_equals", "neq":
		return convertValue(meta, PredicateOpNotEquals, value)
	case "gt":
		return convertValue(meta, PredicateOpGreaterThan, value)
	case "gte":
		return convertValue(meta, PredicateOpGreaterEq, value)
	case "lt":
		return convertValue(meta, PredicateOpLessThan, value)
	case "lte":
		return convertValue(meta, PredicateOpLessEq, value)
	case "starts_with":
		return convertTextPattern(meta, value+"%", PatternKindPrefix)
	case "contains":
		return convertTextPattern(meta, "%"+value+"%", PatternKindContains)
	default:
		return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("unsupported operator '%s'", opName)
	}
}

func convertTextPattern(meta AttributeBinding, patternValue string, pattern PatternKind) (PredicateOp, PatternKind, any, error) {
	if meta.ValueType != forma.ValueTypeText {
		return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("operator only supported for text attributes: %s", meta.AttributeName)
	}
	return PredicateOpLike, pattern, patternValue, nil
}

func convertValue(meta AttributeBinding, op PredicateOp, raw string) (PredicateOp, PatternKind, any, error) {
	switch meta.ValueType {
	case forma.ValueTypeText:
		return op, PatternKindNone, raw, nil
	case forma.ValueTypeNumeric, forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		value, err := parseNumeric(raw)
		if err != nil {
			return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("invalid numeric value for '%s': %w", meta.AttributeName, err)
		}
		return op, PatternKindNone, value, nil
	case forma.ValueTypeDate:
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("invalid date value for '%s': %w", meta.AttributeName, err)
		}
		return op, PatternKindNone, parsed, nil
	case forma.ValueTypeBool:
		if op != PredicateOpEquals && op != PredicateOpNotEquals {
			return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("operator '%s' is not supported for boolean attributes", op)
		}
		val, err := strconv.ParseBool(raw)
		if err != nil {
			return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("invalid boolean value for '%s': %w", meta.AttributeName, err)
		}
		return op, PatternKindNone, val, nil
	default:
		return PredicateOp(""), PatternKindNone, nil, fmt.Errorf("unsupported value type '%s'", meta.ValueType)
	}
}

func parseNumeric(raw string) (any, error) {
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("value '%s' is not numeric", raw)
}

func convertLogic(logic forma.Logic) (LogicOp, error) {
	switch logic {
	case forma.LogicAnd:
		return LogicOpAnd, nil
	case forma.LogicOr:
		return LogicOpOr, nil
	default:
		return "", fmt.Errorf("unsupported logic operator '%s'", logic)
	}
}

func resolvedAttributeName(requestName string, meta AttributeBinding) string {
	if meta.AttributeName != "" {
		return meta.AttributeName
	}
	return requestName
}
