package internal

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestSQLGenerator_ToSQLClauses(t *testing.T) {
	jsonFilter := `
{
    "l": "and",
    "c": [
        {
            "a": "price",
            "v": "gt:10"
        },
        {
            "l": "or",
            "c": [
                {
                    "a": "status",
                    "v": "active"
                },
                {
                    "a": "category",
                    "v": "starts_with:A"
                }
            ]
        }
    ]
}
`

	var root forma.CompositeCondition
	if err := json.Unmarshal([]byte(jsonFilter), &root); err != nil {
		t.Fatalf("failed to unmarshal composite condition: %v", err)
	}

	if root.Logic != forma.LogicAnd {
		t.Fatalf("expected root logic to be 'and', got %s", root.Logic)
	}

	cache := forma.SchemaAttributeCache{
		"price": forma.AttributeMetadata{
			AttributeID: 10,
			ValueType:   forma.ValueTypeNumeric,
		},
		"status": forma.AttributeMetadata{
			AttributeID: 11,
			ValueType:   forma.ValueTypeText,
		},
		"category": forma.AttributeMetadata{
			AttributeID: 12,
			ValueType:   forma.ValueTypeText,
		},
	}

	paramCounter := 1
	sqlGenerator := NewSQLGenerator()
	sqlClause, args, err := sqlGenerator.ToSQLClauses(&root, "eav_table", 1, cache, &paramCounter)
	if err != nil {
		t.Fatalf("failed to convert composite condition to SQL: %v", err)
	}

	expectedClause := "((EXISTS (SELECT 1 FROM eav_table x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $2 AND x.value_numeric > $3))" +
		" AND (((EXISTS (SELECT 1 FROM eav_table x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $4 AND x.value_text = $5))" +
		" OR (EXISTS (SELECT 1 FROM eav_table x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $6 AND x.value_text LIKE $7)))))"
	if sqlClause != expectedClause {
		t.Fatalf("unexpected SQL clause.\nexpected: %s\nactual:   %s", expectedClause, sqlClause)
	}

	expectedArgs := []any{
		int16(10), float64(10),
		int16(11), "active",
		int16(12), "A%",
	}

	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected SQL arguments.\nexpected: %#v\nactual:   %#v", expectedArgs, args)
	}

	if paramCounter != 7 {
		t.Fatalf("unexpected param counter, expected 7 got %d", paramCounter)
	}
}

// TestSQLGenerator_EmptyComposite_AND_Returns1_1 verifies that an empty AND composite
// produces "1=1" (matches everything) instead of an empty string.
func TestSQLGenerator_EmptyComposite_AND_Returns1_1(t *testing.T) {
	gen := NewSQLGenerator()
	paramIndex := 1
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil}
	clause, args, err := gen.ToSQLClauses(cond, "eav", 1, forma.SchemaAttributeCache{}, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Empty(t, args)
}

// TestSQLGenerator_EmptyComposite_OR_Returns1_0 verifies that an empty OR composite
// produces "1=0" (matches nothing) instead of an empty string.
func TestSQLGenerator_EmptyComposite_OR_Returns1_0(t *testing.T) {
	gen := NewSQLGenerator()
	paramIndex := 1
	cond := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: nil}
	clause, args, err := gen.ToSQLClauses(cond, "eav", 1, forma.SchemaAttributeCache{}, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "1=0", clause)
	require.Empty(t, args)
}

// TestBuildHybridConditions_EmptyComposite_AND verifies that buildHybridConditions with
// an empty AND composite returns "1=1" (not an empty string) so runOptimizedQuery accepts it.
func TestBuildHybridConditions_EmptyComposite_AND(t *testing.T) {
	r := &DBPersistentRecordRepository{}
	query := AttributeQuery{
		SchemaID:  1,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil},
	}
	clause, args, err := r.buildHybridConditions("eav_data", "entity_main", query, 1, false)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Empty(t, args)
}

// TestBuildHybridConditions_EmptyComposite_OR verifies that buildHybridConditions with
// an empty OR composite returns "1=0" (not an empty string).
func TestBuildHybridConditions_EmptyComposite_OR(t *testing.T) {
	r := &DBPersistentRecordRepository{}
	query := AttributeQuery{
		SchemaID:  1,
		Condition: &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: nil},
	}
	clause, args, err := r.buildHybridConditions("eav_data", "entity_main", query, 1, false)
	require.NoError(t, err)
	require.Equal(t, "1=0", clause)
	require.Empty(t, args)
}

// TestSQLGenerator_BoolEAV_UsesValueNumeric verifies that a bool EAV filter generates
// a predicate against value_numeric (not value_text) and binds float64(1)/float64(0)
// arguments, matching the storage path in attribute_converter.go.
func TestSQLGenerator_BoolEAV_UsesValueNumeric(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"active": forma.AttributeMetadata{
			AttributeID: 20,
			ValueType:   forma.ValueTypeBool,
		},
	}

	tests := []struct {
		name      string
		value     string
		wantArg   float64
		wantOp    string
	}{
		{name: "truthy value=1", value: "1", wantArg: float64(1), wantOp: "="},
		{name: "falsy value=0", value: "0", wantArg: float64(0), wantOp: "="},
		{name: "not-equal truthy neq:1", value: "neq:1", wantArg: float64(1), wantOp: "!="},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cond := &forma.CompositeCondition{
				Logic: forma.LogicAnd,
				Conditions: []forma.Condition{
					&forma.KvCondition{Attr: "active", Value: tc.value},
				},
			}
			paramIndex := 1
			gen := NewSQLGenerator()
			clause, args, err := gen.ToSQLClauses(cond, "eav_data", 1, cache, &paramIndex)
			require.NoError(t, err)
			require.Contains(t, clause, "value_numeric", "bool filter must use value_numeric column")
			require.NotContains(t, clause, "value_text", "bool filter must not use value_text column")
			require.Len(t, args, 2)
			require.Equal(t, int16(20), args[0], "first arg must be attr_id as int16")
			require.Equal(t, tc.wantArg, args[1], "second arg must be float64")
		})
	}
}
