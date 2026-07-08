package benchmark

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
)

func TestWorkloadResolvedFilterConditionsPrefersExplicitMap(t *testing.T) {
	workload := WorkloadDefinition{FilterAttribute: "symbol", FilterValue: "SYM00001", FilterConditions: map[string]any{"symbol": "SYM00002", "exchange": "NYSE"}}
	conditions := workload.ResolvedFilterConditions()
	if len(conditions) != 2 || conditions["symbol"] != "SYM00002" || conditions["exchange"] != "NYSE" {
		t.Fatalf("expected explicit filter conditions to win, got %+v", conditions)
	}
	simple := WorkloadDefinition{FilterAttribute: "symbol", FilterValue: "SYM00001"}
	conditions = simple.ResolvedFilterConditions()
	if len(conditions) != 1 || conditions["symbol"] != "SYM00001" {
		t.Fatalf("expected simple filter fallback, got %+v", conditions)
	}
}

func TestExecuteWorkloadWithRetry_RetriesTransientFailure(t *testing.T) {
	t.Helper()

	originalBackoff := retryBackoffDelay
	retryBackoffDelay = func(int) time.Duration { return 0 }
	defer func() { retryBackoffDelay = originalBackoff }()

	attempts := 0
	wantRecords := []*model.PersistentRecord{{SchemaID: 101}}
	wantRun := WorkloadRunResult{Name: "baseline-page-1", Passed: true}

	run, records, err := executeWorkloadWithRetry(context.Background(), func(ctx context.Context) (WorkloadRunResult, []*model.PersistentRecord, error) {
		attempts++
		if attempts < 3 {
			return WorkloadRunResult{}, nil, errors.New("transient infra failure")
		}
		return wantRun, wantRecords, nil
	})
	if err != nil {
		t.Fatalf("expected retry helper to recover, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if run.Name != wantRun.Name || !run.Passed {
		t.Fatalf("unexpected run result: %+v", run)
	}
	if len(records) != 1 || records[0].SchemaID != wantRecords[0].SchemaID {
		t.Fatalf("unexpected records: %+v", records)
	}
}

func TestExecuteWorkloadWithRetry_ReturnsLastErrorAfterRetries(t *testing.T) {
	originalBackoff := retryBackoffDelay
	retryBackoffDelay = func(int) time.Duration { return 0 }
	defer func() { retryBackoffDelay = originalBackoff }()

	attempts := 0
	wantErr := errors.New("persistent infra failure")

	_, _, err := executeWorkloadWithRetry(context.Background(), func(ctx context.Context) (WorkloadRunResult, []*model.PersistentRecord, error) {
		attempts++
		return WorkloadRunResult{}, nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected last error %v, got %v", wantErr, err)
	}
	if attempts != maxInfraRetries+1 {
		t.Fatalf("expected %d attempts, got %d", maxInfraRetries+1, attempts)
	}
}

func TestBuildExpectedWorkloadResultsRestrictsPostgresOnlyWorkloadsToHotKeys(t *testing.T) {
	// Issue #147: prefer-hot tier-mix workloads execute against the Postgres
	// hot buffer only, so their loaded-state oracle must exclude records that
	// live in parquet tiers even when those records fall inside the window.
	genCfg := GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform, TimeWindowDays: 30, BaseTime: defaultBaseTime}.WithDefaults()
	semantics := semanticsForWorkload(WorkloadDefinition{Name: "hot-only-window", TargetSchema: "trade"}, genCfg)
	hotRow := deterministicRowID(8, "trade", 1)
	coldRow := deterministicRowID(8, "trade", 2)
	workloads := []WorkloadDefinition{{
		Name:         "hot-only-window",
		Category:     WorkloadCategoryTierMix,
		TargetSchema: "trade",
		PageSize:     50,
		PageNumber:   1,
		PreferHot:    true,
	}}
	records := []GeneratedRecord{
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: hotRow, Version: 1, ChangedAt: semantics.TradeTimeStart + 1000, Attributes: map[string]any{"tradeTime": strconv.FormatInt(semantics.TradeTimeStart+1000, 10)}},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: coldRow, Version: 1, ChangedAt: semantics.TradeTimeStart + 2000, Attributes: map[string]any{"tradeTime": strconv.FormatInt(semantics.TradeTimeStart+2000, 10)}},
	}
	hotKeys := map[string]struct{}{schemaRowKey(SchemaIDTrade, hotRow): {}}

	results := buildExpectedWorkloadResultsFromRecords(records, workloads, 20, genCfg, hotKeys)
	expected := results["hot-only-window"]
	if expected.TotalRecords != 1 {
		t.Fatalf("expected only the hot-tier record, got %+v", expected)
	}
	if len(expected.RowIDs) != 1 || expected.RowIDs[0] != hotRow.String() {
		t.Fatalf("unexpected hot-only row ids: %+v", expected.RowIDs)
	}

	unrestricted := buildExpectedWorkloadResultsFromRecords(records, workloads, 20, genCfg, nil)
	if unrestricted["hot-only-window"].TotalRecords != 2 {
		t.Fatalf("expected nil hot keys to keep all records, got %+v", unrestricted["hot-only-window"])
	}
}

func TestRecordMatchesFilterHandlesStorageLayoutRecords(t *testing.T) {
	// Issue #147: service-path workloads rebuild records in storage layout
	// (column bindings + EAV entries) while harness-path records carry
	// attribute names directly; the filter oracle must read both shapes.
	nyse := "NYSE"
	serviceShaped := &model.PersistentRecord{
		SchemaID:   SchemaIDTrade,
		TextItems:  map[string]string{"text_01": "SYM00001", "text_02": "EU"},
		Int16Items: map[string]int16{"smallint_01": 3},
		OtherAttributes: []model.EAVRecord{
			{AttrID: 8, ValueText: &nyse},
		},
	}
	harnessShaped := &model.PersistentRecord{
		SchemaID:   SchemaIDTrade,
		TextItems:  map[string]string{"symbol": "SYM00001", "exchange": "NYSE", "region": "EU"},
		Int64Items: map[string]int64{"tradeType": 3},
	}

	for name, record := range map[string]*model.PersistentRecord{"service": serviceShaped, "harness": harnessShaped} {
		if !recordMatchesFilter(record, "symbol", "SYM00001") {
			t.Fatalf("%s record should match symbol filter", name)
		}
		if !recordMatchesFilter(record, "exchange", "NYSE") {
			t.Fatalf("%s record should match exchange filter", name)
		}
		if !recordMatchesFilter(record, "region", "EU") {
			t.Fatalf("%s record should match region filter", name)
		}
		if !recordMatchesFilter(record, "tradeType", "3") {
			t.Fatalf("%s record should match tradeType filter", name)
		}
		if recordMatchesFilter(record, "symbol", "SYM99999") {
			t.Fatalf("%s record should reject wrong symbol", name)
		}
	}
}

func TestRecordMatchesFilterFailsClosedForUnsupportedAttributes(t *testing.T) {
	// PR #149 review: the filter oracle must not silently pass attributes it
	// cannot resolve — that would mask filter regressions (#147 intent).
	web := "web"
	serviceShaped := &model.PersistentRecord{
		SchemaID: SchemaIDTrade,
		OtherAttributes: []model.EAVRecord{
			{AttrID: 12, ValueText: &web},
		},
	}

	if !recordMatchesFilter(serviceShaped, "orderChannel", "web") {
		t.Fatal("service record should match orderChannel filter via EAV attr 12")
	}
	if recordMatchesFilter(serviceShaped, "orderChannel", "branch") {
		t.Fatal("service record should reject wrong orderChannel value")
	}
	if recordMatchesFilter(serviceShaped, "brokerId", "BRK-0001") {
		t.Fatal("unsupported filter attribute must fail closed")
	}
	if recordMatchesFilter(&model.PersistentRecord{SchemaID: SchemaIDTrade}, "orderChannel", "web") {
		t.Fatal("record without the attribute must not match")
	}
}
