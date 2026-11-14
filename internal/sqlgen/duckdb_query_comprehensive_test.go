package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// TC-1: DuckDB WHERE-clause tests (ported from the retired legacy generator to BuildDuckClause)
// ============================================================================

func TestGenerateDuckDBWhereClause_SimpleKVEquals(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "name",
				Value: "alice",
			},
		},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Equal(t, "name = ?", clause)
	require.Len(t, args, 1)
	require.Equal(t, "alice", args[0])
}

func TestGenerateDuckDBWhereClause_KVWithGTOperator(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "age",
				Value: "gt:30",
			},
		},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Contains(t, clause, "age >")
	require.Len(t, args, 1)
	require.Equal(t, "30", args[0])
}

func TestGenerateDuckDBWhereClause_KVWithStartsWith(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "email",
				Value: "starts_with:test",
			},
		},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Contains(t, clause, "email LIKE")
	require.Len(t, args, 1)
	require.Equal(t, "test%", args[0])
}

func TestGenerateDuckDBWhereClause_KVWithContains(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "description",
				Value: "contains:bug",
			},
		},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Contains(t, clause, "description LIKE")
	require.Len(t, args, 1)
	require.Equal(t, "%bug%", args[0])
}

func TestGenerateDuckDBWhereClause_CompositeAND(t *testing.T) {
	comp := &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "age", Value: "gt:30"},
			&forma.KvCondition{Attr: "name", Value: "alice"},
		},
	}

	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{Condition: comp},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Contains(t, clause, " AND ")
	require.Len(t, args, 2)
	_ = clause // used in assertion
}

func TestGenerateDuckDBWhereClause_CompositeOR(t *testing.T) {
	comp := &forma.CompositeCondition{
		Logic: forma.LogicOr,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "status", Value: "active"},
			&forma.KvCondition{Attr: "status", Value: "pending"},
		},
	}

	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{Condition: comp},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Contains(t, clause, " OR ")
	require.Len(t, args, 2)
}

func TestGenerateDuckDBWhereClause_NilCondition(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{Condition: nil},
	}

	clause, args, err := BuildDuckClause(q.Condition, nil)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Nil(t, args)
}

func TestGenerateDuckDBWhereClause_NilQuery(t *testing.T) {
	clause, args, err := BuildDuckClause(nil, nil)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Nil(t, args)
}
