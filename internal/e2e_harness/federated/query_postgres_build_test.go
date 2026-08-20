package federated

import (
	"strings"
	"testing"
)

// Unit tests for query_postgres_build.go, split out of query_unit_test.go when
// that file reached 496 of the 500-line limit. The file-size guard does not
// watch it: listNonTestSources excludes _test.go (#324).

func TestBuildPostgresOnlyQueriesSupportBenchmarkFilters(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade}
	selectQuery, _ := h.buildPostgresOnlySelectQuery(&QueryOptions{PreferHot: true, Filter: &Filter{Conditions: map[string]any{"tradeType": 0, "exchange": "NYSE"}}, TradeTimeStart: 1000, TradeTimeEnd: 2000, SortBy: "tradeTime", SortDesc: true, Limit: 20})
	countQuery, _ := h.buildPostgresOnlyCountQuery(&QueryOptions{PreferHot: true, Filter: &Filter{Conditions: map[string]any{"tradeType": 0, "exchange": "NYSE"}}, TradeTimeStart: 1000, TradeTimeEnd: 2000})
	for _, query := range []string{selectQuery, countQuery} {
		for _, expected := range []string{"COALESCE(hot_vals.trade_type, em.smallint_01::BIGINT, 0)", "COALESCE(hot_vals.exchange, '')", "COALESCE(hot_vals.trade_time, em.bigint_02, 0)", "FROM change_log cl"} {
			if !strings.Contains(query, expected) {
				t.Fatalf("expected postgres-only query to include %q: %s", expected, query)
			}
		}
	}
	if strings.Contains(countQuery, "LIMIT") {
		t.Fatalf("count query should not include pagination: %s", countQuery)
	}
}

// TestPostgresOnlyCountSharesSelectScanSource pins the invariant the two
// Postgres-only builders exist to keep: the count must scan exactly the row set
// the select returns. The count query ends after its filters and the select
// continues with ORDER BY/LIMIT, so the count's scan source and predicates are a
// prefix of the select's. Comparing two independently assembled strings is what
// makes this catch drift; asserting both contain one shared helper's output
// would only restate that they call it (#324).
//
// The options carry at most one Conditions entry on purpose:
// buildPostgresOnlyFilterClauses ranges over that map, so Go randomizes clause
// order per call and two or more entries would make the prefix assertion flaky.
// Ordering does not matter to the SQL — AND commutes and each builder keeps its
// own args in step — so the constraint lives here rather than in the builders.
func TestPostgresOnlyCountSharesSelectScanSource(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade}
	opts := func() *QueryOptions {
		return &QueryOptions{
			Filter:         &Filter{Conditions: map[string]any{"exchange": "NYSE"}},
			TradeTimeStart: 1000,
			TradeTimeEnd:   2000,
			SortBy:         "tradeTime",
			SortDesc:       true,
			Limit:          20,
		}
	}
	selectQuery, _ := h.buildPostgresOnlySelectQuery(opts())
	countQuery, _ := h.buildPostgresOnlyCountQuery(opts())

	const scanSourceAnchor = "FROM change_log cl"
	selectAt := strings.Index(selectQuery, scanSourceAnchor)
	countAt := strings.Index(countQuery, scanSourceAnchor)
	if selectAt < 0 || countAt < 0 {
		t.Fatalf("expected %q in both postgres-only queries:\nselect: %s\ncount: %s", scanSourceAnchor, selectQuery, countQuery)
	}
	if !strings.HasPrefix(selectQuery[selectAt:], countQuery[countAt:]) {
		t.Fatalf("count scan source diverges from select scan source:\nselect: %s\ncount: %s", selectQuery[selectAt:], countQuery[countAt:])
	}
}
