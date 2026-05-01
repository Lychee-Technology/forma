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
	if strings.Contains(query, "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02)") {
		t.Fatalf("sort-only count query should avoid projected tradeTime expression: %s", query)
	}
	if strings.Contains(query, "LEFT JOIN postgres_scan('host=localhost port=5432 user=postgres password=password dbname=postgres sslmode=disable', 'public', 'entity_main')") {
		t.Fatalf("sort-only count query should avoid entity_main join: %s", query)
	}
}

func TestBuildFederatedQueryCountSQLDynamic_PageOneKeepsWideCountPath(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade, PGHost: "localhost", PGPort: "5432"}
	query := h.buildFederatedQueryCountSQLDynamic("base-path", "delta-path", true, true, nil, &QueryOptions{Limit: 20, Offset: 0, SortBy: "tradeTime", SortDesc: true})
	if !strings.Contains(query, "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02)") {
		t.Fatalf("page-one count query should preserve projected tradeTime path: %s", query)
	}
}

func TestBuildFederatedCombinedQueryUsesHotFilterExpressions(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade, PGHost: "localhost", PGPort: "5432"}
	query := h.buildFederatedCombinedQuery("base-path", "delta-path", false, false, nil, &QueryOptions{Filter: &Filter{Conditions: map[string]any{"exchange": "NYSE"}}}, true, false)
	if !strings.Contains(query, "benchmark_text(hot_vals.attributes, 'exchange', '')") {
		t.Fatalf("expected hot exchange filter expression in combined query: %s", query)
	}
	if !strings.Contains(query, "postgres_scan") {
		t.Fatalf("expected hot tier query in combined query: %s", query)
	}
}

func TestBuildParquetTierQuerySupportsSchemaSpecificProjection(t *testing.T) {
	customerQuery := buildParquetTierQuery("customer-path", benchmarkSchemaIDCustomer, "base", "", "", "AND region = 'NA'", "", true, false)
	if !strings.Contains(customerQuery, "region") || strings.Contains(customerQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected customer projection without trade time conversion: %s", customerQuery)
	}
	securityQuery := buildParquetTierQuery("security-path", benchmarkSchemaIDSecurity, "base", "", "", "AND symbol = 'SYM00001'", "", true, false)
	if !strings.Contains(securityQuery, "symbol") || strings.Contains(securityQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected security projection with symbol only: %s", securityQuery)
	}
}

func TestBuildParquetTierQueryKeepsDeletedRowsForDeduplication(t *testing.T) {
	query := buildParquetTierQuery("trade-path", benchmarkSchemaIDTrade, "delta", "", "", "", "", true, false)
	if strings.Contains(query, "deleted_at = 0") {
		t.Fatalf("expected parquet tier query to defer deleted filtering until after dedup: %s", query)
	}
}

func TestBuildHotTierQueryKeepsDeletedRowsForDeduplication(t *testing.T) {
	query := buildHotTierQuery("pg-conn", benchmarkSchemaIDTrade, "", "", "", true, false)
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
	query := h.buildFederatedCombinedQuery("base-path", "delta-path", true, true, nil, &QueryOptions{TradeTimeStart: 1000, TradeTimeEnd: 2000, SortBy: "tradeTime", SortDesc: true}, true, false)
	for _, expected := range []string{"epoch_ms(tradeTime) >= 1000", "epoch_ms(tradeTime) <= 2000", "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) >= 1000", "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) <= 2000"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected combined query to include %q: %s", expected, query)
		}
	}
}

func TestBuildFederatedQueryCountSQLDynamic_UsesProjectedHotPathForTradeTimeWindow(t *testing.T) {
	h := &FederatedTestHarness{SchemaID: benchmarkSchemaIDTrade, PGHost: "localhost", PGPort: "5432"}
	query := h.buildFederatedQueryCountSQLDynamic("base-path", "delta-path", true, true, nil, &QueryOptions{Limit: 20, Offset: 40, SortBy: "tradeTime", SortDesc: true, TradeTimeStart: 1000, TradeTimeEnd: 2000})
	for _, expected := range []string{"benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) >= 1000", "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) <= 2000", "LEFT JOIN postgres_scan"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected projected count query to include %q: %s", expected, query)
		}
	}
}

func TestProjectionSelectionHelpers(t *testing.T) {
	if !usesBenchmarkProjectionForSelect(&QueryOptions{SortBy: "tradeTime", SortDesc: true}) {
		t.Fatal("select query should project benchmark columns for tradeTime sorting")
	}
	if !usesBenchmarkProjectionForCount(&QueryOptions{SortBy: "tradeTime", SortDesc: true}) {
		t.Fatal("page-one count query should preserve projected tradeTime path")
	}
	if usesBenchmarkProjectionForCount(&QueryOptions{SortBy: "tradeTime", SortDesc: true, Offset: 20}) {
		t.Fatal("deep-page count query should stay narrow for sort-only tradeTime pagination")
	}
	if !usesBenchmarkProjectionForCount(&QueryOptions{Filter: &Filter{Conditions: map[string]any{"exchange": "NYSE"}}}) {
		t.Fatal("count query should project benchmark columns for projected attribute filters")
	}
	if !usesBenchmarkProjectionForCount(&QueryOptions{TradeTimeStart: 1000}) {
		t.Fatal("count query should project benchmark columns for tradeTime windows")
	}
	if !usesTradeTimeOnlyBenchmarkProjectionForSelect(&QueryOptions{SortBy: "tradeTime", SortDesc: true, Offset: 20}) {
		t.Fatal("deep-page tradeTime sort should use the tradeTime-only projection")
	}
	if usesTradeTimeOnlyBenchmarkProjectionForSelect(&QueryOptions{SortBy: "tradeTime", SortDesc: true, TradeTimeStart: 1000}) {
		t.Fatal("tradeTime windows should keep the full projected benchmark path")
	}
	if usesTradeTimeOnlyBenchmarkProjectionForSelect(&QueryOptions{SortBy: "tradeTime", SortDesc: true, Filter: &Filter{Conditions: map[string]any{"exchange": "NYSE"}}}) {
		t.Fatal("benchmark attribute filters should keep the full projected benchmark path")
	}
}

func TestBuildParquetTierQueryAppliesProjectedTradeTimeFilter(t *testing.T) {
	query := buildParquetTierQuery("trade-path", benchmarkSchemaIDTrade, "base", "", "", "", "AND epoch_ms(tradeTime) <= 2000", true, false)
	if !strings.Contains(query, "epoch_ms(tradeTime) <= 2000") {
		t.Fatalf("expected projected trade time filter in parquet query: %s", query)
	}
	if strings.Contains(query, "AND tradeTime <= epoch_ms(2000)") {
		t.Fatalf("expected parquet query to avoid raw tradeTime timestamp comparison: %s", query)
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

func TestBuildParquetTierQueryTradeTimeOnlyProjection(t *testing.T) {
	query := buildParquetTierQuery("trade-path", benchmarkSchemaIDTrade, "base", "", "", "", "", true, true)
	for _, expected := range []string{"'' as name", "'' as symbol", "'' as exchange", "'' as region", "0 as tradeType", "epoch_ms(tradeTime) as tradeTime"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected tradeTime-only parquet projection to include %q: %s", expected, query)
		}
	}
	if strings.Contains(query, "SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType") {
		t.Fatalf("expected tradeTime-only parquet projection to avoid wide benchmark columns: %s", query)
	}
}

func TestBuildHotTierQueryTradeTimeOnlyProjection(t *testing.T) {
	query := buildHotTierQuery("pg-conn", benchmarkSchemaIDTrade, "", "", "", true, true)
	for _, unexpected := range []string{"map(list(attr_name), list(attr_value))", "benchmark_text(hot_vals.attributes", "benchmark_int(hot_vals.attributes"} {
		if strings.Contains(query, unexpected) {
			t.Fatalf("expected tradeTime-only hot query to avoid %q: %s", unexpected, query)
		}
	}
	for _, expected := range []string{"MAX(CASE WHEN attr_id =", "COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime", "'' as symbol", "'' as exchange", "'' as region"} {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected tradeTime-only hot query to include %q: %s", expected, query)
		}
	}
}

func TestShouldSkipFederatedSelect(t *testing.T) {
	tests := []struct {
		name         string
		totalRecords int64
		offset       int
		want         bool
	}{
		{name: "empty result set", totalRecords: 0, offset: 0, want: true},
		{name: "offset within range", totalRecords: 100, offset: 40, want: false},
		{name: "offset at total", totalRecords: 100, offset: 100, want: true},
		{name: "offset beyond total", totalRecords: 100, offset: 120, want: true},
		{name: "negative offset does not short circuit", totalRecords: 100, offset: -1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldSkipFederatedSelect(tt.totalRecords, tt.offset); got != tt.want {
				t.Fatalf("shouldSkipFederatedSelect(%d, %d) = %t, want %t", tt.totalRecords, tt.offset, got, tt.want)
			}
		})
	}
}
