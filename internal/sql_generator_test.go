package internal

import (
	"encoding/json"
	"reflect"
	"testing"

	"lychee.technology/ltbase/forma"
)

func TestSQLGenerator_ToSqlClauses(t *testing.T) {
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

	cache := SchemaAttributeCache{
		"price": AttributeMetadata{
			AttributeID: 10,
			ValueType:   ValueTypeNumeric,
		},
		"status": AttributeMetadata{
			AttributeID: 11,
			ValueType:   ValueTypeText,
		},
		"category": AttributeMetadata{
			AttributeID: 12,
			ValueType:   ValueTypeText,
		},
	}

	paramCounter := 1
	sqlGenerator := NewSQLGenerator()
	sqlClause, args, err := sqlGenerator.ToSqlClauses(&root, "eav_table", 1, cache, &paramCounter)
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
		int16(10), int64(10),
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
