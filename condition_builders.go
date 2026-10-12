package forma

import (
	"encoding/json"
	"fmt"
	"time"
)

// Constructors for the condition DSL leaves added in #646, so Go callers
// never hand-build the "op:value" string. Each returns the exact KvCondition
// a JSON caller would send; the server validates it like any other leaf.

// In matches rows whose attribute equals one of values. The values are
// JSON-encoded into the list operand: strings, numbers, booleans, and types
// that encode as one of those (uuid.UUID, time.Time). The server accepts 1
// to 1,000 non-null values.
func In(attr string, values ...any) *KvCondition {
	return listCondition(attr, FilterIn, values)
}

// NotIn matches rows whose attribute has a value equal to none of values. A
// row with no value never matches; combine with IsNull to include it.
func NotIn(attr string, values ...any) *KvCondition {
	return listCondition(attr, FilterNotIn, values)
}

// IsNull matches rows where the attribute has no value.
func IsNull(attr string) *KvCondition {
	return &KvCondition{Attr: attr, Value: string(FilterIsNull) + ":true"}
}

// IsNotNull matches rows where the attribute has a value.
func IsNotNull(attr string) *KvCondition {
	return &KvCondition{Attr: attr, Value: string(FilterIsNotNull) + ":true"}
}

// Now compares a date or datetime attribute against the database's
// transaction timestamp shifted by offset, for the comparison operators
// FilterEquals, FilterNotEquals, FilterGreaterThan, FilterGreaterEq,
// FilterLessThan and FilterLessEq:
//
//	Now("locked_until", FilterLessEq, 0)                  // {a: "locked_until", v: "lte:$now"}
//	Now("seen_at", FilterGreaterThan, -30*time.Second)   // v: "gt:$now-30s"
//
// The server refuses an offset finer than a millisecond, and refuses the
// operand on a federated query.
func Now(attr string, op FilterType, offset time.Duration) *KvCondition {
	operand := "$now"
	switch {
	case offset > 0:
		operand += "+" + offset.String()
	case offset < 0:
		operand += offset.String()
	}
	return &KvCondition{Attr: attr, Value: string(op) + ":" + operand}
}

// listCondition encodes values as the JSON list operand. A value
// encoding/json cannot encode (a NaN, a channel) yields a JSON string
// operand naming the failure, which the server refuses as invalid input
// naming the attribute, rather than a panic in the caller.
func listCondition(attr string, op FilterType, values []any) *KvCondition {
	if values == nil {
		values = []any{}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		encoded, _ = json.Marshal(fmt.Sprintf("unencodable list operand: %v", err))
	}
	return &KvCondition{Attr: attr, Value: string(op) + ":" + string(encoded)}
}
