package federated

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestBuildFinalFederatedCount(t *testing.T) {
	query := buildFinalFederatedCount("SELECT row_id, changed_at FROM combined_source")
	if !strings.Contains(query, "SELECT COUNT(*)") {
		t.Fatalf("expected count query, got: %s", query)
	}
	if !strings.Contains(query, "WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)") {
		t.Fatalf("expected deduplicated filter, got: %s", query)
	}
}

func TestBuildFederatedQueryCountSQLDynamic(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: 1, PGHost: "localhost", PGPort: "5432"}
	query := h.buildFederatedQueryCountSQLDynamic("base-path", "delta-path", true, true, []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}, &QueryOptions{Limit: 20, Offset: 40, SortBy: "tradeTime", SortDesc: true})
	if !strings.Contains(query, "SELECT COUNT(*)") {
		t.Fatalf("expected count projection, got: %s", query)
	}
	if strings.Contains(query, "LIMIT 20 OFFSET 40") {
		t.Fatalf("count query should not preserve pagination slice: %s", query)
	}
	if !strings.Contains(query, "row_id NOT IN") {
		t.Fatalf("expected dirty-id exclusion in count query: %s", query)
	}
	if !strings.Contains(query, "postgres_scan") {
		t.Fatalf("expected hot tier in count query: %s", query)
	}
}

func TestBuildFederatedCombinedQueryUsesHotFilterExpressions(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade, PGHost: "localhost", PGPort: "5432"}
	query, _ := h.buildFederatedCombinedQuery("base-path", "delta-path", false, false, nil, &QueryOptions{Filter: &Filter{Conditions: map[string]any{"exchange": "NYSE"}}})
	if !strings.Contains(query, "benchmark_text(hot_vals.attributes, 'exchange', '')") {
		t.Fatalf("expected hot exchange filter expression in combined query: %s", query)
	}
	if !strings.Contains(query, "postgres_scan") {
		t.Fatalf("expected hot tier query in combined query: %s", query)
	}
}

func TestBuildParquetTierQuerySupportsSchemaSpecificProjection(t *testing.T) {
	customerQuery := buildParquetTierQuery("customer-path", benchmarkSchemaIDCustomer, "base", "", "", "AND region = 'NA'", "", true)
	if !strings.Contains(customerQuery, "region") || strings.Contains(customerQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected customer projection without trade time conversion: %s", customerQuery)
	}
	securityQuery := buildParquetTierQuery("security-path", benchmarkSchemaIDSecurity, "base", "", "", "AND symbol = 'SYM00001'", "", true)
	if !strings.Contains(securityQuery, "symbol") || strings.Contains(securityQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected security projection with symbol only: %s", securityQuery)
	}
}

func TestBuildParquetTierQueryKeepsDeletedRowsForDeduplication(t *testing.T) {
	query := buildParquetTierQuery("trade-path", benchmarkSchemaIDTrade, "delta", "", "", "", "", true)
	if strings.Contains(query, "deleted_at = 0") {
		t.Fatalf("expected parquet tier query to defer deleted filtering until after dedup: %s", query)
	}
}

func TestBuildHotTierQueryKeepsDeletedRowsForDeduplication(t *testing.T) {
	query := buildHotTierQuery("pg-conn", benchmarkSchemaIDTrade, "", "", "", true)
	if strings.Contains(query, "deleted_at = 0") || strings.Contains(query, "deleted_at IS NULL") {
		t.Fatalf("expected hot tier query to defer deleted filtering until after dedup: %s", query)
	}
}

func TestBuildFederatedDeduplicatedCTEUsesStableTieBreaks(t *testing.T) {
	query := buildFederatedDeduplicatedCTE("SELECT row_id, changed_at, deleted_at, tier, version FROM source")
	for _, expected := range []string{"changed_at DESC", "CASE tier WHEN 'hot' THEN 3", "version DESC", "deleted_at DESC", "row_id ASC"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected dedup query to include %q: %s", expected, query)
		}
	}
}

func TestBuildFederatedCombinedQuerySupportsTradeTimeWindow(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade, PGHost: "localhost", PGPort: "5432"}
	query, _ := h.buildFederatedCombinedQuery("base-path", "delta-path", true, true, nil, &QueryOptions{TradeTimeStart: 1000, TradeTimeEnd: 2000, SortBy: "tradeTime", SortDesc: true})
	for _, expected := range []string{"tradeTime >= epoch_ms(1000)", "tradeTime <= epoch_ms(2000)", "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) >= 1000", "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) <= 2000"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected combined query to include %q: %s", expected, query)
		}
	}
}

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
