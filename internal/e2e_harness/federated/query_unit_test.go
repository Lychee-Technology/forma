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
	if !strings.Contains(query, "WHERE rn = 1") {
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
	customerQuery := buildParquetTierQuery("customer-path", benchmarkSchemaIDCustomer, "base", "", "", "AND region = 'NA'", true)
	if !strings.Contains(customerQuery, "region") || strings.Contains(customerQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected customer projection without trade time conversion: %s", customerQuery)
	}
	securityQuery := buildParquetTierQuery("security-path", benchmarkSchemaIDSecurity, "base", "", "", "AND symbol = 'SYM00001'", true)
	if !strings.Contains(securityQuery, "symbol") || strings.Contains(securityQuery, "epoch_ms(tradeTime)") {
		t.Fatalf("expected security projection with symbol only: %s", securityQuery)
	}
}
