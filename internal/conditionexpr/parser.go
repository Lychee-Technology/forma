package conditionexpr

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OperatorValue contains the parsed operator + value pair from an expression like "op:value".
type OperatorValue struct {
	Operator string
	Value    string
}

// SQLOperatorResult maps an operator token to SQL syntax and the normalized value.
type SQLOperatorResult struct {
	SQLOperator string
	Value       string
}

// ParseOperatorValueLenient parses "op:value"; malformed pairs fallback to equals(raw).
func ParseOperatorValueLenient(raw string) OperatorValue {
	if idx := strings.Index(raw, ":"); idx >= 0 {
		opPart := raw[:idx]
		valPart := raw[idx+1:]
		if opPart != "" && valPart != "" {
			return OperatorValue{Operator: opPart, Value: valPart}
		}
	}
	return OperatorValue{Operator: "equals", Value: raw}
}

// ParseOperatorValueStrict parses "op:value"; malformed pairs return an error.
func ParseOperatorValueStrict(raw string) (OperatorValue, error) {
	if idx := strings.Index(raw, ":"); idx >= 0 {
		opPart := raw[:idx]
		valPart := raw[idx+1:]
		if opPart == "" || valPart == "" {
			return OperatorValue{}, fmt.Errorf("invalid KvCondition value format: %s", raw)
		}
		return OperatorValue{Operator: opPart, Value: valPart}, nil
	}
	return OperatorValue{Operator: "equals", Value: raw}, nil
}

// ParseOperatorValueEqualsOnEmptyOperator parses "op:value" while treating empty op as equals.
// This preserves legacy behavior used by query normalizer.
func ParseOperatorValueEqualsOnEmptyOperator(raw string) OperatorValue {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) == 1 {
		return OperatorValue{Operator: "equals", Value: parts[0]}
	}
	if parts[0] == "" {
		return OperatorValue{Operator: "equals", Value: parts[1]}
	}
	return OperatorValue{Operator: parts[0], Value: parts[1]}
}

// CanonicalOperator normalizes alias operators to canonical names.
func CanonicalOperator(op string) string {
	normalized := strings.ToLower(strings.TrimSpace(op))
	switch normalized {
	case "eq":
		return "equals"
	case "neq":
		return "not_equals"
	default:
		return normalized
	}
}

// ToSQLOperator converts a logical operator to SQL syntax.
// For LIKE operators, it also applies wildcard normalization to value.
func ToSQLOperator(op, value string) (SQLOperatorResult, error) {
	switch CanonicalOperator(op) {
	case "equals":
		return SQLOperatorResult{SQLOperator: "=", Value: value}, nil
	case "gt":
		return SQLOperatorResult{SQLOperator: ">", Value: value}, nil
	case "gte":
		return SQLOperatorResult{SQLOperator: ">=", Value: value}, nil
	case "lt":
		return SQLOperatorResult{SQLOperator: "<", Value: value}, nil
	case "lte":
		return SQLOperatorResult{SQLOperator: "<=", Value: value}, nil
	case "not_equals":
		return SQLOperatorResult{SQLOperator: "!=", Value: value}, nil
	case "starts_with":
		return SQLOperatorResult{SQLOperator: "LIKE", Value: value + "%"}, nil
	case "contains":
		return SQLOperatorResult{SQLOperator: "LIKE", Value: "%" + value + "%"}, nil
	default:
		return SQLOperatorResult{}, fmt.Errorf("unsupported operator: %s", op)
	}
}

// ParseNumeric parses a numeric literal into int64 or float64.
func ParseNumeric(raw string) (any, error) {
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("value '%s' is not numeric", raw)
}

// ParseRFC3339OrUnixMs parses ISO8601 datetime string or unix milliseconds.
func ParseRFC3339OrUnixMs(raw string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed, nil
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date value: expected ISO 8601 format or unix milliseconds, got '%s'", raw)
	}
	return time.UnixMilli(ms), nil
}
