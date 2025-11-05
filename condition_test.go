package forma

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCompositeCondition_UnmarshalToSqlClauses(t *testing.T) {
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

	var root CompositeCondition
	if err := json.Unmarshal([]byte(jsonFilter), &root); err != nil {
		t.Fatalf("failed to unmarshal composite condition: %v", err)
	}

	if root.Logic != LogicAnd {
		t.Fatalf("expected root logic to be 'and', got %s", root.Logic)
	}

	cache := SchemaAttributeCache{
		"price": AttributeMeta{
			AttributeID: 10,
			ValueType:   ValueTypeNumeric,
		},
		"status": AttributeMeta{
			AttributeID: 11,
			ValueType:   ValueTypeText,
		},
		"category": AttributeMeta{
			AttributeID: 12,
			ValueType:   ValueTypeText,
		},
	}

	var paramCounter int
	sqlClause, args, err := root.ToSqlClauses(1, cache, &paramCounter)
	if err != nil {
		t.Fatalf("failed to convert composite condition to SQL: %v", err)
	}

	expectedClause := "((SELECT row_id FROM public.eav_data WHERE schema_id = $1 AND attr_id = $2 AND value_numeric > $3) INTERSECT ((SELECT row_id FROM public.eav_data WHERE schema_id = $4 AND attr_id = $5 AND value_text = $6) UNION (SELECT row_id FROM public.eav_data WHERE schema_id = $7 AND attr_id = $8 AND value_text LIKE $9)))"
	if sqlClause != expectedClause {
		t.Fatalf("unexpected SQL clause.\nexpected: %s\nactual:   %s", expectedClause, sqlClause)
	}

	expectedArgs := []any{
		int16(1), int16(10), int64(10),
		int16(1), int16(11), "active",
		int16(1), int16(12), "A%",
	}

	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected SQL arguments.\nexpected: %#v\nactual:   %#v", expectedArgs, args)
	}

	if paramCounter != 9 {
		t.Fatalf("unexpected param counter, expected 9 got %d", paramCounter)
	}
}
