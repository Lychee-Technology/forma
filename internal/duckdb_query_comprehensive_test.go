package internal

import (
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// TC-1: GenerateDuckDBWhereClause Tests
// ============================================================================

func TestGenerateDuckDBWhereClause_SimpleKVEquals(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "name",
				Value: "alice",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Equal(t, "name = ?", clause)
	require.Len(t, args, 1)
	require.Equal(t, "alice", args[0])
}

func TestGenerateDuckDBWhereClause_KVWithGTOperator(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "age",
				Value: "gt:30",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Contains(t, clause, "age >")
	require.Len(t, args, 1)
	require.Equal(t, 30.0, args[0])
}

func TestGenerateDuckDBWhereClause_KVWithStartsWith(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "email",
				Value: "starts_with:test",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Contains(t, clause, "email LIKE")
	require.Len(t, args, 1)
	require.Equal(t, "test%", args[0])
}

func TestGenerateDuckDBWhereClause_KVWithContains(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "description",
				Value: "contains:bug",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
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

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{Condition: comp},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
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

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{Condition: comp},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Contains(t, clause, " OR ")
	require.Len(t, args, 2)
}

func TestGenerateDuckDBWhereClause_NilCondition(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{Condition: nil},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Nil(t, args)
}

func TestGenerateDuckDBWhereClause_NilQuery(t *testing.T) {
	clause, args, err := GenerateDuckDBWhereClause(nil)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Nil(t, args)
}

// ============================================================================
// TC-2: GenerateDuckDBWhereClauseWithExclusions Tests
// ============================================================================

func TestGenerateDuckDBWhereClauseWithExclusions_AppendsDirtyIDs(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "age",
				Value: "gt:30",
			},
		},
	}

	u1 := uuid.New()
	u2 := uuid.New()

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1, u2})
	require.NoError(t, err)
	require.Contains(t, clause, "age >")
	require.Contains(t, clause, "row_id NOT IN")
	require.Len(t, args, 3) // age arg + 2 dirty IDs
}

func TestGenerateDuckDBWhereClauseWithExclusions_ParameterOrder(t *testing.T) {
	comp := &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "field1", Value: "val1"},
			&forma.KvCondition{Attr: "field2", Value: "val2"},
		},
	}

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{Condition: comp},
	}

	u1 := uuid.New()
	u2 := uuid.New()
	u3 := uuid.New()

	_, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1, u2, u3})
	require.NoError(t, err)
	require.Len(t, args, 5) // 2 query args + 3 dirty ID args

	// First 2 args should be query args
	require.Equal(t, "val1", args[0])
	require.Equal(t, "val2", args[1])

	// Last 3 args should be dirty ID strings
	require.Equal(t, u1.String(), args[2])
	require.Equal(t, u2.String(), args[3])
	require.Equal(t, u3.String(), args[4])
}

func TestGenerateDuckDBWhereClauseWithExclusions_EmptyDirtyIDs(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "name",
				Value: "bob",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{})
	require.NoError(t, err)
	require.Equal(t, "name = ?", clause)
	require.Len(t, args, 1)
	require.Equal(t, "bob", args[0])
}

func TestGenerateDuckDBWhereClauseWithExclusions_SingleDirtyID(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{Condition: nil},
	}

	u1 := uuid.New()

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1})
	require.NoError(t, err)
	require.Contains(t, clause, "1=1")
	require.Contains(t, clause, "row_id NOT IN")
	require.Len(t, args, 1)
	require.Equal(t, u1.String(), args[0])
}

func TestGenerateDuckDBWhereClauseWithExclusions_ManyDirtyIDs(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "active",
				Value: "true",
			},
		},
	}

	// Create 10 dirty IDs
	dirtyIDs := make([]uuid.UUID, 10)
	for i := range 10 {
		dirtyIDs[i] = uuid.New()
	}

	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)
	require.NoError(t, err)
	require.Contains(t, clause, "row_id NOT IN")
	// 1 query arg (true) + 10 dirty ID args
	require.Len(t, args, 11)

	// Verify dirty ID order
	for i := range 10 {
		require.Equal(t, dirtyIDs[i].String(), args[i+1])
	}
}

// ============================================================================
// TC-3: AppendDirtyExclusion Tests
// ============================================================================

func TestAppendDirtyExclusion_BasicUsage(t *testing.T) {
	baseClause := "age > 30"
	u1 := uuid.New()
	u2 := uuid.New()

	clause, args := AppendDirtyExclusion(baseClause, []uuid.UUID{u1, u2})
	require.Contains(t, clause, "age > 30")
	require.Contains(t, clause, "row_id NOT IN")
	require.Len(t, args, 2)
	require.Equal(t, u1.String(), args[0])
	require.Equal(t, u2.String(), args[1])
}

func TestAppendDirtyExclusion_EmptyDirtyIDs(t *testing.T) {
	baseClause := "status = ?"
	clause, args := AppendDirtyExclusion(baseClause, []uuid.UUID{})
	require.Equal(t, baseClause, clause)
	require.Len(t, args, 0)
}

// ============================================================================
// TC-4: RenderDirtyIDsValuesCSV Tests
// ============================================================================

func TestRenderDirtyIDsValuesCSV_MultipleUUIDs(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()

	csv := RenderDirtyIDsValuesCSV([]uuid.UUID{u1, u2})
	require.Contains(t, csv, "(")
	require.Contains(t, csv, u1.String())
	require.Contains(t, csv, u2.String())
}

func TestRenderDirtyIDsValuesCSV_EmptyList(t *testing.T) {
	csv := RenderDirtyIDsValuesCSV([]uuid.UUID{})
	require.Equal(t, "", csv)
}

// ============================================================================
// TC-5: BuildDuckDBQuery Tests
// ============================================================================

func TestBuildDuckDBQuery_SimpleTemplate(t *testing.T) {
	// Create a simple template that doesn't use postgres_scan
	tmpl := template.Must(template.New("test").Parse(
		`SELECT 1 WHERE {{.Anchor.Condition}}`,
	))

	params := map[string]any{
		"Anchor": map[string]any{
			"Condition": "1=1",
		},
	}

	q := &FederatedAttributeQuery{}
	sql, args, err := BuildDuckDBQuery(tmpl, params, q, []uuid.UUID{}, nil)

	require.NoError(t, err)
	require.Contains(t, sql, "SELECT 1")
	require.Len(t, args, 0)
}

func TestBuildDuckDBQuery_WithDirtyIDs(t *testing.T) {
	tmpl := template.Must(template.New("test").Parse(
		`SELECT * WHERE {{.Anchor.Condition}}`,
	))

	params := map[string]any{
		"Anchor": map[string]any{
			"Condition": "1=1",
		},
	}

	u1 := uuid.New()
	u2 := uuid.New()

	q := &FederatedAttributeQuery{}
	sql, args, err := BuildDuckDBQuery(tmpl, params, q, []uuid.UUID{u1, u2}, nil)

	require.NoError(t, err)
	require.Contains(t, sql, "row_id NOT IN")
	require.Len(t, args, 2)
	require.Equal(t, u1.String(), args[0])
	require.Equal(t, u2.String(), args[1])
}

// ============================================================================
// TC-6: Routing & Configuration Tests
// ============================================================================

func TestValidateDuckDBConfig_InvalidMemoryLimit(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:       true,
		MemoryLimitMB: -1,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid memory_limit_mb")
}

func TestValidateDuckDBConfig_InvalidParallelism(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: -1,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid max_parallelism")
}

func TestValidateDuckDBConfig_InvalidMaxConnections(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 0,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_connections must be >= 1")
}

func TestValidateDuckDBConfig_InvalidQueryTimeout(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 1,
		QueryTimeout:   0,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query_timeout must be > 0")
}

func TestValidateDuckDBConfig_DisabledIsValid(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: false,
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}

func TestValidateDuckDBConfig_ValidConfig(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
		DBPath:         ":memory:",
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}
