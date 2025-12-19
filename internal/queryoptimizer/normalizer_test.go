package queryoptimizer

import (
"strings"
"testing"
"time"

"github.com/lychee-technology/forma"
)

func baseAttrs() AttributeCatalog {
	return AttributeCatalog{
		"status": {
			AttributeName: "status",
			AttributeID:   1,
			ValueType:     forma.ValueTypeText,
			Storage:       StorageTargetEAV,
			Fallback:      AttributeFallbackNone,
		},
		"amount": {
			AttributeName: "amount",
			AttributeID:   2,
			ValueType:     forma.ValueTypeNumeric,
			Storage:       StorageTargetMain,
			Column: &ColumnRef{
				Name: "amount_col",
				Type: "numeric",
			},
			Fallback: AttributeFallbackNone,
		},
		"flag": {
			AttributeName: "flag",
			AttributeID:   3,
			ValueType:     forma.ValueTypeBool,
			Storage:       StorageTargetMain,
			Column: &ColumnRef{
				Name: "flag_col",
				Type: "bool",
			},
			Fallback: AttributeFallbackNone,
		},
		"created_at": {
			AttributeName: "created_at",
			AttributeID:   4,
			ValueType:     forma.ValueTypeDate,
			Storage:       StorageTargetMain,
			Column: &ColumnRef{
				Name: "created_at_col",
				Type: "timestamp",
			},
			Fallback: AttributeFallbackNone,
		},
		"array_status": {
			AttributeName: "custom_status",
			AttributeID:   5,
			ValueType:     forma.ValueTypeText,
			Storage:       StorageTargetEAV,
			InsideArray:   true,
			Fallback:      AttributeFallbackNone,
		},
	}
}

var defaultTables = StorageTables{
	EntityMain: "entity_main",
	EAVData:    "eav_data",
}

func TestNormalizeQuery_ValidationErrors(t *testing.T) {
	opts := NormalizerOptions{DefaultLimit: 10, MaxLimit: 50}

	_, err := NormalizeQuery(nil, 1, "schema", defaultTables, baseAttrs(), opts)
	if err == nil || err.Error() != "query request cannot be nil" {
		t.Fatalf("expected nil request error, got %v", err)
	}

	_, err = NormalizeQuery(&forma.QueryRequest{}, 1, "schema", defaultTables, baseAttrs(), opts)
	if err == nil || err.Error() != "query condition is required" {
		t.Fatalf("expected missing condition error, got %v", err)
	}

	_, err = NormalizeQuery(&forma.QueryRequest{Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}, 1, "schema", defaultTables, AttributeCatalog{}, opts)
	if err == nil || err.Error() != "attribute catalog cannot be empty" {
		t.Fatalf("expected empty attrs error, got %v", err)
	}

	_, err = NormalizeQuery(&forma.QueryRequest{Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}, 1, "", defaultTables, baseAttrs(), opts)
	if err == nil || err.Error() != "schema name is required" {
		t.Fatalf("expected missing schema name error, got %v", err)
	}
}

func TestNormalizeQuery_SchemaNameFallback(t *testing.T) {
	req := &forma.QueryRequest{
		SchemaName: "lead",
		Condition:  &forma.KvCondition{Attr: "status", Value: "hot"},
	}
	in, err := NormalizeQuery(req, 1, "", defaultTables, baseAttrs(), NormalizerOptions{DefaultLimit: 10})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if in.SchemaName != "lead" {
		t.Fatalf("expected schema name from request, got %s", in.SchemaName)
	}
}

func TestNormalizeQuery_Pagination(t *testing.T) {
	attrs := baseAttrs()

	t.Run("default limit applies", func(t *testing.T) {
		req := &forma.QueryRequest{ItemsPerPage: 0, Page: 0, Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{DefaultLimit: 20, MaxLimit: 50})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if in.Pagination.Limit != 20 || in.Pagination.Offset != 0 {
			t.Fatalf("unexpected pagination %+v", in.Pagination)
		}
	})

	t.Run("minimum limit enforced", func(t *testing.T) {
		req := &forma.QueryRequest{ItemsPerPage: 0, Page: 1, Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{DefaultLimit: 0})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if in.Pagination.Limit != 1 {
			t.Fatalf("expected limit 1, got %d", in.Pagination.Limit)
		}
	})

	t.Run("max limit capped", func(t *testing.T) {
		req := &forma.QueryRequest{ItemsPerPage: 200, Page: 2, Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{DefaultLimit: 10, MaxLimit: 50})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if in.Pagination.Limit != 50 {
			t.Fatalf("expected limit 50, got %d", in.Pagination.Limit)
		}
		if in.Pagination.Offset != 50 {
			t.Fatalf("expected offset 50, got %d", in.Pagination.Offset)
		}
	})

	t.Run("offset computation", func(t *testing.T) {
		req := &forma.QueryRequest{ItemsPerPage: 5, Page: 3, Condition: &forma.KvCondition{Attr: "status", Value: "hot"}}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if in.Pagination.Limit != 5 || in.Pagination.Offset != 10 {
			t.Fatalf("unexpected pagination %+v", in.Pagination)
		}
	})
}

func TestNormalizeQuery_SortHandling(t *testing.T) {
	attrs := baseAttrs()

	t.Run("desc sort", func(t *testing.T) {
		req := &forma.QueryRequest{
			Condition: &forma.KvCondition{Attr: "status", Value: "hot"},
			SortBy:    []string{" status "},
			SortOrder: forma.SortOrderDesc,
		}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(in.SortKeys) != 1 {
			t.Fatalf("expected 1 sort key, got %d", len(in.SortKeys))
		}
		if in.SortKeys[0].Direction != SortDesc {
			t.Fatalf("expected desc direction, got %s", in.SortKeys[0].Direction)
		}
		if in.SortKeys[0].AttributeName != "status" {
			t.Fatalf("expected resolved attr name 'status', got %s", in.SortKeys[0].AttributeName)
		}
	})

	t.Run("error on empty sort attr", func(t *testing.T) {
		req := &forma.QueryRequest{
			Condition: &forma.KvCondition{Attr: "status", Value: "hot"},
			SortBy:    []string{""},
		}
		_, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err == nil || err.Error() != "sort attribute name cannot be empty" {
			t.Fatalf("expected empty sort attr error, got %v", err)
		}
	})

	t.Run("error on unknown sort attr", func(t *testing.T) {
		req := &forma.QueryRequest{
			Condition: &forma.KvCondition{Attr: "status", Value: "hot"},
			SortBy:    []string{"unknown"},
		}
		_, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err == nil || err.Error() != "cannot sort by unknown attribute 'unknown'" {
			t.Fatalf("expected unknown sort attr error, got %v", err)
		}
	})
}

func TestNormalizeQuery_ConditionNormalization(t *testing.T) {
	attrs := baseAttrs()

	t.Run("composite with no children", func(t *testing.T) {
		req := &forma.QueryRequest{
			Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil},
		}
		_, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err == nil || err.Error() != "composite condition requires children" {
			t.Fatalf("expected composite child error, got %v", err)
		}
	})

	t.Run("kv predicate normalization", func(t *testing.T) {
		req := &forma.QueryRequest{
			Condition: &forma.KvCondition{Attr: "status", Value: "starts_with:ho"},
		}
		in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if in.Filter == nil || in.Filter.Predicate == nil {
			t.Fatal("expected predicate filter")
		}
		pred := in.Filter.Predicate
		if pred.Operator != PredicateOpLike || pred.Pattern != PatternKindPrefix {
			t.Fatalf("expected LIKE with prefix pattern, got op=%s pattern=%s", pred.Operator, pred.Pattern)
		}
		if pred.AttributeName != "status" || pred.AttributeID != 1 {
			t.Fatalf("unexpected predicate attributes %+v", pred)
		}
	})
}

func TestNormalizeQuery_OperatorAndValueValidation(t *testing.T) {
	attrs := baseAttrs()

	cases := []struct {
		name      string
		cond      forma.Condition
		expectErr string
	}{
		{
			name:      "unsupported operator",
			cond:      &forma.KvCondition{Attr: "status", Value: "unknown:hot"},
			expectErr: "unsupported operator 'unknown'",
		},
		{
			name:      "numeric parse error",
			cond:      &forma.KvCondition{Attr: "amount", Value: "eq:notanumber"},
			expectErr: "invalid numeric value for 'amount'",
		},
		{
			name:      "date parse error",
			cond:      &forma.KvCondition{Attr: "created_at", Value: "eq:not-a-date"},
			expectErr: "invalid date value for 'created_at'",
		},
		{
			name:      "bool with invalid operator",
			cond:      &forma.KvCondition{Attr: "flag", Value: "gt:true"},
			expectErr: "operator '>' is not supported for boolean attributes",
		},
		{
			name:      "text pattern on numeric",
			cond:      &forma.KvCondition{Attr: "amount", Value: "starts_with:10"},
			expectErr: "operator only supported for text attributes: amount",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeQuery(&forma.QueryRequest{Condition: tc.cond}, 1, "lead", defaultTables, attrs, NormalizerOptions{})
			if err == nil || !contains(err.Error(), tc.expectErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectErr, err)
			}
		})
	}
}

func TestNormalizeQuery_ResolvedAttributeNameAndInsideArray(t *testing.T) {
	attrs := baseAttrs()
	req := &forma.QueryRequest{
		Condition: &forma.KvCondition{Attr: "array_status", Value: "contains:hot"},
	}
	in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	pred := in.Filter.Predicate
	if pred.AttributeName != "custom_status" {
		t.Fatalf("expected resolved name 'custom_status', got %s", pred.AttributeName)
	}
	if !pred.InsideArray {
		t.Fatalf("expected InsideArray true")
	}
	if pred.Pattern != PatternKindContains {
		t.Fatalf("expected contains pattern, got %s", pred.Pattern)
	}
}

func TestNormalizeQuery_DateConversionSuccess(t *testing.T) {
attrs := baseAttrs()
date := time.Date(2023, 3, 14, 15, 9, 26, 0, time.UTC)
req := &forma.QueryRequest{
Condition: &forma.KvCondition{Attr: "created_at", Value: "eq:" + date.Format(time.RFC3339)},
}
in, err := NormalizeQuery(req, 1, "lead", defaultTables, attrs, NormalizerOptions{})
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
pred := in.Filter.Predicate
if pred.ValueType != forma.ValueTypeDate {
t.Fatalf("unexpected value type %s", pred.ValueType)
}
if pred.Value.(time.Time) != date {
t.Fatalf("expected parsed date %v, got %v", date, pred.Value)
}
}

func TestNormalizeConditionTree(t *testing.T) {
attrs := baseAttrs()

t.Run("and with kv children", func(t *testing.T) {
cond := &forma.CompositeCondition{
Logic: forma.LogicAnd,
Conditions: []forma.Condition{
&forma.KvCondition{Attr: "status", Value: "hot"},
&forma.KvCondition{Attr: "amount", Value: "gt:10"},
},
}
node, err := normalizeConditionTree(cond, attrs)
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if node.Logic != LogicOpAnd {
t.Fatalf("expected LogicOpAnd, got %s", node.Logic)
}
if len(node.Children) != 2 {
t.Fatalf("expected 2 children, got %d", len(node.Children))
}
p1 := node.Children[0].Predicate
if p1 == nil || p1.Operator != PredicateOpEquals || p1.Value != "hot" {
t.Fatalf("unexpected first predicate %+v", p1)
}
p2 := node.Children[1].Predicate
if p2 == nil || p2.Operator != PredicateOpGreaterThan || p2.Value != int64(10) {
t.Fatalf("unexpected second predicate %+v", p2)
}
})

t.Run("or with nested composite", func(t *testing.T) {
nested := &forma.CompositeCondition{
Logic: forma.LogicAnd,
Conditions: []forma.Condition{
&forma.KvCondition{Attr: "amount", Value: "lt:5"},
&forma.KvCondition{Attr: "flag", Value: "eq:true"},
},
}
cond := &forma.CompositeCondition{
Logic: forma.LogicOr,
Conditions: []forma.Condition{
&forma.KvCondition{Attr: "status", Value: "eq:hot"},
nested,
},
}
node, err := normalizeConditionTree(cond, attrs)
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if node.Logic != LogicOpOr || len(node.Children) != 2 {
t.Fatalf("unexpected root node %+v", node)
}
if node.Children[1].Logic != LogicOpAnd || len(node.Children[1].Children) != 2 {
t.Fatalf("unexpected nested node %+v", node.Children[1])
}
})

t.Run("child error bubbles", func(t *testing.T) {
cond := &forma.CompositeCondition{
Logic: forma.LogicAnd,
Conditions: []forma.Condition{
&forma.KvCondition{Attr: "missing", Value: "eq:1"},
},
}
_, err := normalizeConditionTree(cond, attrs)
if err == nil || !strings.Contains(err.Error(), "attribute 'missing' not found in schema") {
t.Fatalf("expected missing attribute error, got %v", err)
}
})

t.Run("composite with no children", func(t *testing.T) {
cond := &forma.CompositeCondition{Logic: forma.LogicAnd}
_, err := normalizeConditionTree(cond, attrs)
if err == nil || err.Error() != "composite condition requires children" {
t.Fatalf("expected composite child error, got %v", err)
}
})

t.Run("unsupported condition type", func(t *testing.T) {
_, err := normalizeConditionTree(&unknownCond{}, attrs)
if err == nil || !strings.Contains(err.Error(), "unsupported condition type") {
t.Fatalf("expected unsupported condition type error, got %v", err)
}
})
}

type unknownCond struct{}

func (unknownCond) IsLeaf() bool { return true }

func TestNormalizeValue(t *testing.T) {
textMeta := AttributeBinding{AttributeName: "status", AttributeID: 1, ValueType: forma.ValueTypeText}
numericMeta := AttributeBinding{AttributeName: "amount", AttributeID: 2, ValueType: forma.ValueTypeNumeric}
dateMeta := AttributeBinding{AttributeName: "created_at", AttributeID: 3, ValueType: forma.ValueTypeDate}
boolMeta := AttributeBinding{AttributeName: "flag", AttributeID: 4, ValueType: forma.ValueTypeBool}
unsupportedMeta := AttributeBinding{AttributeName: "x", AttributeID: 5, ValueType: forma.ValueType("custom")}

t.Run("text equals", func(t *testing.T) {
op, pattern, val, err := normalizeValue(textMeta, "equals", "abc")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpEquals || pattern != PatternKindNone || val != "abc" {
t.Fatalf("unexpected result op=%s pattern=%s val=%v", op, pattern, val)
}
})

t.Run("text starts_with", func(t *testing.T) {
op, pattern, val, err := normalizeValue(textMeta, "starts_with", "abc")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpLike || pattern != PatternKindPrefix || val != "abc%" {
t.Fatalf("unexpected result op=%s pattern=%s val=%v", op, pattern, val)
}
})

t.Run("text contains", func(t *testing.T) {
op, pattern, val, err := normalizeValue(textMeta, "contains", "abc")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpLike || pattern != PatternKindContains || val != "%abc%" {
t.Fatalf("unexpected result op=%s pattern=%s val=%v", op, pattern, val)
}
})

t.Run("numeric eq int", func(t *testing.T) {
op, pattern, val, err := normalizeValue(numericMeta, "eq", "42")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpEquals || pattern != PatternKindNone {
t.Fatalf("unexpected op/pattern %s/%s", op, pattern)
}
if v, ok := val.(int64); !ok || v != 42 {
t.Fatalf("expected int64(42), got %v", val)
}
})

t.Run("numeric gt float", func(t *testing.T) {
op, _, val, err := normalizeValue(numericMeta, "gt", "10.5")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpGreaterThan {
t.Fatalf("unexpected op %s", op)
}
if v, ok := val.(float64); !ok || v != 10.5 {
t.Fatalf("expected float64(10.5), got %v", val)
}
})

t.Run("numeric gte int", func(t *testing.T) {
op, _, val, err := normalizeValue(numericMeta, "gte", "7")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpGreaterEq {
t.Fatalf("unexpected op %s", op)
}
if v, ok := val.(int64); !ok || v != 7 {
t.Fatalf("expected int64(7), got %v", val)
}
})

t.Run("numeric lte float", func(t *testing.T) {
op, _, val, err := normalizeValue(numericMeta, "lte", "3.25")
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpLessEq {
t.Fatalf("unexpected op %s", op)
}
if v, ok := val.(float64); !ok || v != 3.25 {
t.Fatalf("expected float64(3.25), got %v", val)
}
})

t.Run("date eq", func(t *testing.T) {
raw := "2023-03-14T15:09:26Z"
op, pattern, val, err := normalizeValue(dateMeta, "eq", raw)
if err != nil {
t.Fatalf("unexpected err: %v", err)
}
if op != PredicateOpEquals || pattern != PatternKindNone {
t.Fatalf("unexpected op/pattern %s/%s", op, pattern)
}
if v, ok := val.(time.Time); !ok || v.Format(time.RFC3339) != raw {
t.Fatalf("expected parsed date %s, got %v", raw, val)
}
})

t.Run("bool equals and not_equals", func(t *testing.T) {
op, pattern, val, err := normalizeValue(boolMeta, "eq", "true")
if err != nil || op != PredicateOpEquals || pattern != PatternKindNone {
t.Fatalf("unexpected eq result op=%s pattern=%s err=%v", op, pattern, err)
}
if vb, ok := val.(bool); !ok || vb != true {
t.Fatalf("expected true, got %v", val)
}

op, pattern, val, err = normalizeValue(boolMeta, "not_equals", "false")
if err != nil || op != PredicateOpNotEquals || pattern != PatternKindNone {
t.Fatalf("unexpected neq result op=%s pattern=%s err=%v", op, pattern, err)
}
if vb, ok := val.(bool); !ok || vb != false {
t.Fatalf("expected false, got %v", val)
}
})

t.Run("unsupported value type", func(t *testing.T) {
_, _, _, err := normalizeValue(unsupportedMeta, "eq", "x")
if err == nil || !strings.Contains(err.Error(), "unsupported value type") {
t.Fatalf("expected unsupported value type error, got %v", err)
}
})

t.Run("unsupported operator", func(t *testing.T) {
_, _, _, err := normalizeValue(textMeta, "weird_op", "x")
if err == nil || !strings.Contains(err.Error(), "unsupported operator") {
t.Fatalf("expected unsupported operator error, got %v", err)
}
})
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || len(needle) == 0 || (len(needle) > 0 && indexOf(haystack, needle) >= 0))
}

func indexOf(s, substr string) int {
	return len([]rune(s[:len(s)])) - len([]rune(s[len(substr):]))
}
