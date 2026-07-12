package sqlgen

import (
	"regexp"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// findRenderedScanCalls returns the raw argument text of every postgres_scan
// invocation in sqlText, matching parentheses so multi-line calls are
// captured whole. (design_doc_sql_test.go in internal/federated keeps the
// documentation side of this contract; this file pins the runtime side.)
func findRenderedScanCalls(sqlText string) []string {
	const marker = "postgres_scan("
	var calls []string
	for i := 0; ; {
		idx := strings.Index(sqlText[i:], marker)
		if idx < 0 {
			break
		}
		start := i + idx + len(marker)
		depth := 1
		j := start
		for ; j < len(sqlText) && depth > 0; j++ {
			switch sqlText[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		calls = append(calls, strings.TrimSpace(sqlText[start:j-1]))
		i = j
	}
	return calls
}

// TestAdvancedTemplate_PostgresScanContract pins the runtime scan contract
// that docs/federated-query/design.md §5 documents (#214): every rendered
// postgres_scan is the 3-arg (connection, schema, table) form with plain
// identifiers, and flushed_at = 0 is expressed only as a WHERE predicate —
// never smuggled into a scan argument as a predicate or dynamic SELECT.
func TestAdvancedTemplate_PostgresScanContract(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
	}
	dual := &DualClauses{DuckClause: "1=1"}
	// Mirror the production params from buildDuckDBQueryWithPlan
	// (internal/federated/duckdb_query.go); injectDuckDBTemplateParams fills
	// the projection defaults, including HasEAVPivot = true.
	params := map[string]any{
		"PG_CONN":              "dbname=forma host=localhost",
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"S3_PATHS":             "['s3://bucket/base/*.parquet']",
		"LOGICAL_WHERE_CLAUSE": "1=1",
	}

	sqlText, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	calls := findRenderedScanCalls(sqlText)
	require.Len(t, calls, 4,
		"dirty_ids, pg_source change_log, entity_main, and the EAV pivot must all scan via postgres_scan")

	threeArg := regexp.MustCompile(
		`^'[^']*'\s*,\s*'[A-Za-z_][A-Za-z0-9_]*'\s*,\s*'[A-Za-z_][A-Za-z0-9_]*'$`)
	for _, args := range calls {
		require.Regexp(t, threeArg, args,
			"postgres_scan must stay the 3-arg (connection, schema, table) form; "+
				"predicates and dynamic SELECTs never belong in scan arguments")
		require.NotContains(t, args, "flushed_at",
			"flushed_at = 0 is a WHERE predicate, not a scan argument")
	}

	require.Contains(t, sqlText, "AND flushed_at = 0",
		"dirty_ids must restrict to unflushed rows via a WHERE predicate")
	require.Contains(t, sqlText, "AND cl.flushed_at = 0",
		"pg_source must restrict to unflushed rows via a WHERE predicate")
}
