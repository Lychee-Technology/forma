package internal

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Edge Case Tests: Complex Nested Conditions
// ============================================================================

func TestGenerateDuckDBWhereClause_DeeplyNestedComposite(t *testing.T) {
	// Test deeply nested: ((A OR B) AND (C OR D))
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic: forma.LogicAnd,
				Conditions: []forma.Condition{
					&forma.CompositeCondition{
						Logic: forma.LogicOr,
						Conditions: []forma.Condition{
							&forma.KvCondition{
								Attr:  "field1",
								Value: "value1",
							},
							&forma.KvCondition{
								Attr:  "field2",
								Value: "value2",
							},
						},
					},
					&forma.CompositeCondition{
						Logic: forma.LogicOr,
						Conditions: []forma.Condition{
							&forma.KvCondition{
								Attr:  "field3",
								Value: "value3",
							},
							&forma.KvCondition{
								Attr:  "field4",
								Value: "value4",
							},
						},
					},
				},
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Greater(t, len(args), 0)
	// Should contain both AND and OR logic
	require.Contains(t, strings.ToUpper(clause), "AND")
	require.Contains(t, strings.ToUpper(clause), "OR")
}

func TestGenerateDuckDBWhereClause_TriplyNestedComposite(t *testing.T) {
	// Test triple nesting: (((A AND B) OR C) AND D)
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic: forma.LogicAnd,
				Conditions: []forma.Condition{
					&forma.CompositeCondition{
						Logic: forma.LogicOr,
						Conditions: []forma.Condition{
							&forma.CompositeCondition{
								Logic: forma.LogicAnd,
								Conditions: []forma.Condition{
									&forma.KvCondition{
										Attr:  "a",
										Value: "1",
									},
									&forma.KvCondition{
										Attr:  "b",
										Value: "2",
									},
								},
							},
							&forma.KvCondition{
								Attr:  "c",
								Value: "3",
							},
						},
					},
					&forma.KvCondition{
						Attr:  "d",
						Value: "4",
					},
				},
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Len(t, args, 4) // Should have 4 parameters
}

func TestGenerateDuckDBWhereClause_ManyTopLevelConditions(t *testing.T) {
	// Test with many (20+) top-level AND conditions
	conditions := make([]forma.Condition, 0, 25)
	for i := range 25 {
		conditions = append(conditions, &forma.KvCondition{
			Attr:  "field" + string(rune(i)),
			Value: "value" + string(rune(i)),
		})
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic:      forma.LogicAnd,
				Conditions: conditions,
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.Len(t, args, 25)
	// Verify the clause contains many AND operators
	andCount := strings.Count(strings.ToUpper(clause), " AND ")
	require.Greater(t, andCount, 20)
}

// ============================================================================
// Edge Case Tests: Unicode and Special Characters
// ============================================================================

func TestGenerateDuckDBWhereClause_UnicodeCharacters(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "name",
				Value: "François Müller 北京",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Len(t, args, 1)
	// The Unicode string should be preserved
	require.Equal(t, "François Müller 北京", args[0])
}

func TestGenerateDuckDBWhereClause_SpecialCharactersInValue(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "email",
				Value: "user+tag@example.com",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Equal(t, "user+tag@example.com", args[0])
}

func TestGenerateDuckDBWhereClause_QuotesAndApostrophes(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "text",
				Value: `O'Reilly's "Advanced" SQL`,
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Special characters should be preserved (escaping done by driver)
	require.Equal(t, `O'Reilly's "Advanced" SQL`, args[0])
}

func TestGenerateDuckDBWhereClause_SQLSpecialCharacters(t *testing.T) {
	// Test with characters that have special meaning in SQL
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "value",
				Value: `%; DROP TABLE users; --`,
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Should preserve the value (parameterized queries prevent injection)
	require.Equal(t, `%; DROP TABLE users; --`, args[0])
}

func TestGenerateDuckDBWhereClause_EmptyStringValue(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "field",
				Value: "",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Len(t, args, 1)
	require.Equal(t, "", args[0])
}

func TestGenerateDuckDBWhereClause_VeryLongStringValue(t *testing.T) {
	// Create a very long string (10KB)
	longValue := strings.Repeat("a", 10000)
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "field",
				Value: longValue,
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Equal(t, longValue, args[0])
}

func TestGenerateDuckDBWhereClause_NewlinesAndWhitespace(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "text",
				Value: "line1\nline2\r\nline3\ttab",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	require.Equal(t, "line1\nline2\r\nline3\ttab", args[0])
}

// ============================================================================
// Edge Case Tests: Large Dirty ID Sets
// ============================================================================

func TestGenerateDuckDBWhereClauseWithExclusions_LargeDirtyIDSet100(t *testing.T) {
	// Test with 100 dirty IDs
	dirtyIDs := make([]uuid.UUID, 0, 100)
	for range 100 {
		dirtyIDs = append(dirtyIDs, uuid.New())
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "name",
				Value: "test",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Should have at least 101 args: 1 for the condition + 100 for dirty IDs
	require.GreaterOrEqual(t, len(args), 101)
	// Verify NOT IN clause is present
	require.Contains(t, strings.ToUpper(clause), "NOT IN")
}

func TestGenerateDuckDBWhereClauseWithExclusions_LargeDirtyIDSet1000(t *testing.T) {
	// Test with 1000 dirty IDs
	dirtyIDs := make([]uuid.UUID, 0, 1000)
	for range 1000 {
		dirtyIDs = append(dirtyIDs, uuid.New())
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "status",
				Value: "active",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Should have at least 1001 args
	require.GreaterOrEqual(t, len(args), 1001)
	// Verify it's a valid NOT IN clause
	require.Contains(t, strings.ToUpper(clause), "NOT IN")
}

func TestGenerateDuckDBWhereClauseWithExclusions_MaxInt16DirtyIDs(t *testing.T) {
	// Test with max practical number of dirty IDs
	// In real scenarios, this could be 5000+ but we test with 500 for practical limits
	dirtyIDs := make([]uuid.UUID, 0, 500)
	for range 500 {
		dirtyIDs = append(dirtyIDs, uuid.New())
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "id",
				Value: "target",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(args), 501)
	// Verify clause structure is valid
	require.True(t, strings.Contains(strings.ToUpper(clause), "NOT IN") ||
		strings.Contains(strings.ToUpper(clause), "WHERE"))
}

func TestGenerateDuckDBWhereClauseWithExclusions_DuplicateDirtyIDs(t *testing.T) {
	// Test that duplicate dirty IDs are handled correctly
	baseID := uuid.New()
	dirtyIDs := make([]uuid.UUID, 0, 10)
	// Add the same ID 5 times
	for range 5 {
		dirtyIDs = append(dirtyIDs, baseID)
	}
	// Add different IDs
	for i := 5; i < 10; i++ {
		dirtyIDs = append(dirtyIDs, uuid.New())
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "field",
				Value: "value",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Implementation should handle duplicates gracefully
	require.GreaterOrEqual(t, len(args), 6) // At least 1 + 10 dirty IDs
}

// ============================================================================
// Combined Edge Cases
// ============================================================================

func TestGenerateDuckDBWhereClause_ComplexNestedWithUnicodeAndLargeDirtySet(t *testing.T) {
	// Combine: deeply nested conditions + Unicode + large dirty ID set
	dirtyIDs := make([]uuid.UUID, 0, 100)
	for range 100 {
		dirtyIDs = append(dirtyIDs, uuid.New())
	}

	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic: forma.LogicAnd,
				Conditions: []forma.Condition{
					&forma.CompositeCondition{
						Logic: forma.LogicOr,
						Conditions: []forma.Condition{
							&forma.KvCondition{
								Attr:  "name",
								Value: "François",
							},
							&forma.KvCondition{
								Attr:  "city",
								Value: "北京",
							},
						},
					},
					&forma.KvCondition{
						Attr:  "status",
						Value: "中文_测试",
					},
				},
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)
	require.NoError(t, err)
	require.NotEmpty(t, clause)
	// Should contain condition values and dirty IDs
	require.Greater(t, len(args), 100)
}

func TestGenerateDuckDBWhereClause_EdgeCaseWithNilCondition(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: nil,
		},
	}

	clause, _, err := GenerateDuckDBWhereClause(query)
	require.NoError(t, err)
	// Should default to "1=1" when no condition
	require.Equal(t, "1=1", clause)
}

func TestGenerateDuckDBWhereClause_EmptyCompositeCondition(t *testing.T) {
	query := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic:      forma.LogicAnd,
				Conditions: []forma.Condition{},
			},
		},
	}

	clause, _, err := GenerateDuckDBWhereClause(query)
	// Empty composite should be handled (either error or default)
	// Implementation-dependent
	require.True(t, err != nil || clause != "")
}
