package queryoptimizer

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestGeneratePlanBasic(t *testing.T) {
	optimizer := New()

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data_2",
		},
		Filter:     nil,
		SortKeys:   []SortKey{},
		Pagination: Pagination{Limit: 10, Offset: 0},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	if plan == nil {
		t.Fatal("plan should not be nil")
	}

	if plan.SQL == "" {
		t.Fatal("SQL should not be empty")
	}

	if len(plan.Params) == 0 {
		t.Fatal("Params should not be empty")
	}

	t.Logf("Generated SQL:\n%s\n", plan.SQL)
	t.Logf("Params: %v\n", plan.Params)
	t.Logf("Explain: %+v\n", plan.Explain)
}

func TestGeneratePlanWithFilter(t *testing.T) {
	optimizer := New()

	// Create a simple predicate
	pred := &Predicate{
		AttributeName: "status",
		AttributeID:   1,
		ValueType:     forma.ValueTypeText,
		Operator:      PredicateOpEquals,
		Value:         "hot",
		Storage:       StorageTargetEAV,
		Pattern:       PatternKindNone,
		Fallback:      AttributeFallbackNone,
		InsideArray:   false,
	}

	// Create a filter node
	filter := &FilterNode{
		Logic:     LogicOpAnd,
		Predicate: pred,
	}

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data_2",
		},
		Filter: filter,
		SortKeys: []SortKey{
			{
				AttributeName: "status",
				AttributeID:   1,
				ValueType:     forma.ValueTypeText,
				Direction:     SortAsc,
				Storage:       StorageTargetEAV,
			},
		},
		Pagination: Pagination{Limit: 10, Offset: 0},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	if plan == nil {
		t.Fatal("plan should not be nil")
	}

	if plan.SQL == "" {
		t.Fatal("SQL should not be empty")
	}

	t.Logf("Generated SQL with filter:\n%s\n", plan.SQL)
	t.Logf("Params: %v\n", plan.Params)
}

func TestGeneratePlanWithCompositeFilter(t *testing.T) {
	optimizer := New()

	// Create two predicates
	pred1 := &Predicate{
		AttributeName: "status",
		AttributeID:   1,
		ValueType:     forma.ValueTypeText,
		Operator:      PredicateOpEquals,
		Value:         "hot",
		Storage:       StorageTargetEAV,
	}

	pred2 := &Predicate{
		AttributeName: "age",
		AttributeID:   2,
		ValueType:     forma.ValueTypeNumeric,
		Operator:      PredicateOpGreaterThan,
		Value:         25,
		Storage:       StorageTargetEAV,
	}

	// Create leaf nodes
	node1 := &FilterNode{Predicate: pred1}
	node2 := &FilterNode{Predicate: pred2}

	// Create composite node
	filter := &FilterNode{
		Logic:    LogicOpAnd,
		Children: []*FilterNode{node1, node2},
	}

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data_2",
		},
		Filter:     filter,
		SortKeys:   []SortKey{},
		Pagination: Pagination{Limit: 10, Offset: 0},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	if plan == nil {
		t.Fatal("plan should not be nil")
	}

	t.Logf("Generated SQL with composite filter:\n%s\n", plan.SQL)
	t.Logf("Params: %v\n", plan.Params)
}

func TestGeneratePlan_MainTableFilter(t *testing.T) {
	optimizer := New()

	// Predicate on a Main table column
	pred := &Predicate{
		AttributeName: "score",
		AttributeID:   10,
		ValueType:     forma.ValueTypeNumeric,
		Operator:      PredicateOpEquals,
		Value:         100,
		Storage:       StorageTargetMain,
		Column: &ColumnRef{
			Name: "score_col",
			Type: "int",
		},
		Fallback: AttributeFallbackNone,
	}

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data",
		},
		Filter:     &FilterNode{Predicate: pred},
		Pagination: Pagination{Limit: 10},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	// Check that SQL contains direct column reference
	if !strings.Contains(plan.SQL, "t.score_col = $") {
		t.Errorf("Expected SQL to contain 't.score_col = $', got:\n%s", plan.SQL)
	}

	// Check that it does NOT contain EXISTS (since it's main table)
	if strings.Contains(plan.SQL, "EXISTS") {
		t.Errorf("Expected SQL NOT to contain 'EXISTS' for main table filter, got:\n%s", plan.SQL)
	}
}

func TestGeneratePlan_FallbackNumeric(t *testing.T) {
	optimizer := New()

	// Predicate with NumericToDouble fallback
	pred := &Predicate{
		AttributeName: "amount",
		AttributeID:   11,
		ValueType:     forma.ValueTypeNumeric,
		Operator:      PredicateOpEquals,
		Value:         123.45,
		Storage:       StorageTargetMain,
		Column: &ColumnRef{
			Name: "amount_col",
			Type: "double precision",
		},
		Fallback: AttributeFallbackNumericToDouble,
	}

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data",
		},
		Filter:     &FilterNode{Predicate: pred},
		Pagination: Pagination{Limit: 10},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	// Check for range query
	if !strings.Contains(plan.SQL, "t.amount_col >= $") || !strings.Contains(plan.SQL, "t.amount_col <= $") {
		t.Errorf("Expected SQL to contain range check for fallback, got:\n%s", plan.SQL)
	}
}

func TestGeneratePlan_SortEAV(t *testing.T) {
	optimizer := New()

	input := &Input{
		SchemaID:   1,
		SchemaName: "lead",
		Tables: StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data",
		},
		SortKeys: []SortKey{
			{
				AttributeName: "custom_field",
				AttributeID:   55,
				ValueType:     forma.ValueTypeText,
				Direction:     SortDesc,
				Storage:       StorageTargetEAV,
			},
		},
		Pagination: Pagination{Limit: 10},
	}

	plan, err := optimizer.GeneratePlan(context.Background(), input)
	if err != nil {
		t.Fatalf("GeneratePlan failed: %v", err)
	}

	// Check for LATERAL JOIN
	if !strings.Contains(plan.SQL, "LEFT JOIN LATERAL") {
		t.Errorf("Expected SQL to contain 'LEFT JOIN LATERAL' for EAV sort, got:\n%s", plan.SQL)
	}

	// Check for ORDER BY alias
	if !strings.Contains(plan.SQL, "ORDER BY s1.val DESC") {
		t.Errorf("Expected SQL to contain 'ORDER BY s1.val DESC', got:\n%s", plan.SQL)
	}
}
