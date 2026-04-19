package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

// RunResult captures benchmark execution output.
type RunResult struct {
	Config         Config               `json:"config"`
	Generator      GeneratorConfig      `json:"generator"`
	Metadata       ArtifactMetadata     `json:"metadata"`
	StartedAt      time.Time            `json:"started_at"`
	CompletedAt    time.Time            `json:"completed_at"`
	ValidationOnly bool                 `json:"validation_only"`
	Passed         bool                 `json:"passed"`
	FailureCount   int                  `json:"failure_count,omitempty"`
	InfraError     string               `json:"infra_error,omitempty"`
	Schemas        []SchemaFixture      `json:"schemas"`
	Workloads      []WorkloadDefinition `json:"workloads"`
	Executions     []WorkloadRunResult  `json:"executions,omitempty"`
	Notes          []string             `json:"notes"`
}

// WorkloadRunResult captures one workload execution result.
type WorkloadRunResult struct {
	Name         string            `json:"name"`
	Category     string            `json:"category"`
	Distribution Distribution      `json:"distribution"`
	PageSize     int               `json:"page_size"`
	PageNumber   int               `json:"page_number"`
	Offset       int               `json:"offset"`
	ResultCount  int               `json:"result_count"`
	TotalRecords int64             `json:"total_records"`
	Duration     time.Duration     `json:"duration"`
	Passed       bool              `json:"passed"`
	FailureKind  string            `json:"failure_kind,omitempty"`
	FailureCount int               `json:"failure_count,omitempty"`
	InfraError   string            `json:"infra_error,omitempty"`
	RowIDs       []string          `json:"row_ids,omitempty"`
	Assertions   []AssertionResult `json:"assertions,omitempty"`
	PlanNotes    []string          `json:"plan_notes,omitempty"`
}

const (
	FailureKindInfra       = "infra"
	FailureKindCorrectness = "correctness"
)

// AssertionResult captures one correctness assertion outcome.
type AssertionResult struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message,omitempty"`
}

// Runner validates benchmark inputs and prepares execution plans.
type Runner struct {
	config    Config
	registry  forma.SchemaRegistry
	schemas   []SchemaFixture
	workloads []WorkloadDefinition
	genConfig GeneratorConfig
}

// NewRunner builds a runner using the benchmark fixture registry.
func NewRunner(cfg Config) (*Runner, error) {
	resolved := cfg.WithDefaults()
	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	registry, err := LoadFixtureRegistry()
	if err != nil {
		return nil, err
	}
	workloads, err := ResolveWorkloads(resolved.Workloads)
	if err != nil {
		return nil, err
	}
	return &Runner{
		config:    resolved,
		registry:  registry,
		schemas:   DefaultSchemaFixtures(),
		workloads: workloads,
		genConfig: GeneratorConfigFromBenchmark(resolved),
	}, nil
}

// RegisterSchemas loads the benchmark schema entries into a harness-backed registry table.
func (r *Runner) RegisterSchemas(registrar SchemaRegistrar) error {
	return RegisterFixtureSchemas(registrar)
}

// Run executes the scaffolded benchmark flow.
func (r *Runner) Run(ctx context.Context) (*RunResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if err := r.validateFixtures(); err != nil {
		return nil, err
	}
	result := &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
		StartedAt:      startedAt,
		ValidationOnly: true,
		Passed:         true,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
	}
	switch r.config.Mode {
	case ExecutionModeSmoke:
		result.Notes = []string{
			"validated benchmark configuration",
			"loaded TPC-E-inspired schema fixtures",
			"resolved workload matrix",
			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
			"smoke mode stops before query execution",
		}
	case ExecutionModePlan:
		result.Notes = []string{
			"validated benchmark configuration",
			"loaded TPC-E-inspired schema fixtures",
			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
			"built execution plan only",
		}
	default:
		return nil, fmt.Errorf("unsupported execution mode %q", r.config.Mode)
	}
	result.CompletedAt = time.Now()
	return result, nil
}

// RunWithHarness executes supported benchmark workloads against a live federated harness.
func (r *Runner) RunWithHarness(ctx context.Context, h *federated.FederatedTestHarness, profile TierMixProfile) (*RunResult, error) {
	if h == nil {
		return nil, fmt.Errorf("federated harness cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if err := r.validateFixtures(); err != nil {
		return nil, err
	}
	generator, err := NewGenerator(r.genConfig)
	if err != nil {
		return nil, err
	}
	dataset, err := generator.Generate()
	if err != nil {
		return nil, err
	}
	tiered, err := SplitIntoTiers(dataset, profile)
	if err != nil {
		return nil, err
	}
	if err := LoadTieredDataset(ctx, h, tiered); err != nil {
		return nil, err
	}
	executions := make([]WorkloadRunResult, 0, len(r.workloads)*r.config.Iterations)
	pageRuns := make(map[string]WorkloadRunResult)
	loadedRecords, err := buildLoadedStateSnapshot(ctx, h, tiered)
	if err != nil {
		return nil, fmt.Errorf("build loaded state snapshot: %w", err)
	}
	expectedByWorkload := buildExpectedWorkloadResultsFromRecords(loadedRecords, r.workloads, r.config.PageSize, r.genConfig)
	if hotExpected, err := buildExpectedWorkloadResultFromFederatedTruth(ctx, h, WorkloadDefinition{Name: "hot-selective-page", TargetSchema: "trade", FilterAttribute: "symbol", FilterValue: "SYM00001", PageSize: 20, PageNumber: 1}, r.config.PageSize, loadedRecords, r.genConfig); err == nil {
		expectedByWorkload["hot-selective-page"] = hotExpected
	}
	if eavExpected, err := buildExpectedWorkloadResultFromFederatedTruth(ctx, h, WorkloadDefinition{Name: "eav-selective-page", TargetSchema: "trade", FilterAttribute: "exchange", FilterValue: "NYSE", PageSize: 20, PageNumber: 1}, r.config.PageSize, loadedRecords, r.genConfig); err == nil {
		expectedByWorkload["eav-selective-page"] = eavExpected
	}
	passed := true
	failureCount := 0
	for _, workload := range r.workloads {
		if !workload.SupportsDistribution(r.genConfig.Distribution) {
			continue
		}
		for iteration := 0; iteration < r.config.Iterations; iteration++ {
			run, records, err := r.executeWorkload(ctx, h, workload)
			if err != nil {
				run = failedWorkloadRunResult(workload, r.genConfig.Distribution, r.config.PageSize, fmt.Sprintf("execute workload: %v", err))
				executions = append(executions, run)
				passed = false
				failureCount++
				continue
			}
			semantics := semanticsForWorkload(workload, r.genConfig)
			run.Assertions = append(run.Assertions, validateBasicWorkloadAssertions(workload, run)...)
			run.Assertions = append(run.Assertions, validateResultLevelAssertions(workload, run, records, semantics)...)
			if expected, ok := expectedByWorkload[workload.Name]; ok {
				run.Assertions = append(run.Assertions, validateExpectedWorkloadOutcome(run, expected)...)
			}
			if workload.Category == WorkloadCategoryPagination || workload.Category == WorkloadCategoryDeepPage {
				if previous, ok := pageRuns[workload.TargetSchema]; ok {
					run.Assertions = append(run.Assertions, validatePaginationTransition(previous, run)...)
				}
				pageRuns[workload.TargetSchema] = run
			}
			run.FailureCount = countFailedAssertions(run.Assertions)
			run.FailureKind = failureKindForRun(run)
			run.Passed = run.FailureCount == 0 && run.InfraError == ""
			if !run.Passed {
				passed = false
				failureCount += maxInt(1, run.FailureCount)
			}
			executions = append(executions, run)
		}
	}
	result := &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
		StartedAt:      startedAt,
		CompletedAt:    time.Now(),
		ValidationOnly: false,
		Passed:         passed,
		FailureCount:   failureCount,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
		Executions:     executions,
		Notes: []string{
			"loaded TPC-E-inspired schema fixtures",
			fmt.Sprintf("generated dataset with distribution=%s", r.genConfig.Distribution),
			fmt.Sprintf("loaded tiered dataset profile=%s", profile.Name),
			fmt.Sprintf("loaded-state snapshot rows=%d", len(loadedRecords)),
			"executed supported federated query workloads",
		},
	}
	return result, nil
}

func (r *Runner) validateFixtures() error {
	for _, fixture := range r.schemas {
		id, _, err := r.registry.GetSchemaAttributeCacheByName(fixture.Name)
		if err != nil {
			return fmt.Errorf("validate fixture %s: %w", fixture.Name, err)
		}
		if id != fixture.ID {
			return fmt.Errorf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
		}
		if _, _, err := r.registry.GetSchemaByName(fixture.Name); err != nil {
			return fmt.Errorf("load JSON schema for %s: %w", fixture.Name, err)
		}
	}
	return nil
}

func (r *Runner) executeWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*internal.PersistentRecord, error) {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	schemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return WorkloadRunResult{}, nil, err
	}
	previousSchemaID := h.SchemaID
	h.SchemaID = schemaID
	defer func() {
		h.SchemaID = previousSchemaID
	}()
	opts := queryOptionsForWorkloadWithConfig(workload, r.config.PageSize, r.genConfig)
	if workload.UsesSimpleFilter() {
		opts.Filter = &federated.Filter{Conditions: map[string]any{workload.FilterAttribute: workload.FilterValue}}
	}
	result, err := h.ExecuteFederatedQuery(ctx, opts)
	if err != nil {
		return WorkloadRunResult{}, nil, err
	}
	run := WorkloadRunResult{
		Name:         workload.Name,
		Category:     string(workload.Category),
		Distribution: r.genConfig.Distribution,
		PageSize:     pageSize,
		PageNumber:   workload.PageNumber,
		Offset:       opts.Offset,
		ResultCount:  len(result.Records),
		TotalRecords: result.TotalRecords,
		Duration:     result.Duration,
		Passed:       true,
		RowIDs:       persistentRecordIDs(result.Records),
	}
	if result.Plan != nil {
		run.PlanNotes = append([]string(nil), result.Plan.Notes...)
	}
	return run, result.Records, nil
}

func failedWorkloadRunResult(workload WorkloadDefinition, distribution Distribution, defaultPageSize int, infraError string) WorkloadRunResult {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	return WorkloadRunResult{
		Name:         workload.Name,
		Category:     string(workload.Category),
		Distribution: distribution,
		PageSize:     pageSize,
		PageNumber:   workload.PageNumber,
		Offset:       workload.DerivedOffset(defaultPageSize),
		Passed:       false,
		FailureKind:  FailureKindInfra,
		FailureCount: 1,
		InfraError:   infraError,
	}
}

type expectedWorkloadResult struct {
	TotalRecords int64
	RowIDs       []string
}

type workloadSemantics struct {
	TradeTimeStart int64
	TradeTimeEnd   int64
}

func buildExpectedWorkloadResults(dataset *GeneratedDataset, workloads []WorkloadDefinition, defaultPageSize int) map[string]expectedWorkloadResult {
	if dataset == nil {
		return map[string]expectedWorkloadResult{}
	}
	return buildExpectedWorkloadResultsFromRecords(dataset.Records, workloads, defaultPageSize, dataset.Config)
}

func buildExpectedWorkloadResultsFromRecords(records []GeneratedRecord, workloads []WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig) map[string]expectedWorkloadResult {
	results := make(map[string]expectedWorkloadResult, len(workloads))
	visible := expectedVisibleRecords(records)
	for _, workload := range workloads {
		matching := filterExpectedRecordsForWorkload(visible, workload, semanticsForWorkload(workload, genCfg))
		sortExpectedRecordsForWorkload(matching, workload)
		pageSize := workload.PageSize
		if pageSize <= 0 {
			pageSize = defaultPageSize
		}
		offset := workload.DerivedOffset(defaultPageSize)
		rowIDs := expectedPageRowIDs(matching, offset, pageSize)
		results[workload.Name] = expectedWorkloadResult{TotalRecords: int64(len(matching)), RowIDs: rowIDs}
	}
	return results
}

func buildLoadedStateSnapshot(ctx context.Context, h *federated.FederatedTestHarness, dataset *TieredDataset) ([]GeneratedRecord, error) {
	if h == nil || dataset == nil {
		return nil, fmt.Errorf("harness and dataset are required")
	}
	hotRecords, hotKeys, err := loadHotStateRecords(ctx, h)
	if err != nil {
		return nil, err
	}
	records := make([]GeneratedRecord, 0, len(dataset.Base)+len(dataset.Delta)+len(hotRecords))
	for _, bucket := range [][]GeneratedRecord{dataset.Base, dataset.Delta} {
		for _, record := range bucket {
			if _, ok := hotKeys[schemaRowKey(record.SchemaID, record.RowID)]; ok {
				continue
			}
			records = append(records, cloneGeneratedRecord(record))
		}
	}
	records = append(records, hotRecords...)
	return records, nil
}

func loadHotStateRecords(ctx context.Context, h *federated.FederatedTestHarness) ([]GeneratedRecord, map[string]struct{}, error) {
	rows, err := h.PGDB.QueryContext(ctx, `
		SELECT cl.schema_id, cl.row_id, cl.changed_at, COALESCE(cl.deleted_at, 0),
			em.text_01, em.text_02, em.smallint_01, em.bigint_02,
			hot_vals.symbol, hot_vals.exchange, hot_vals.region, hot_vals.trade_type, hot_vals.trade_time, hot_vals.name
		FROM change_log cl
		LEFT JOIN entity_main em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
		LEFT JOIN (
			SELECT schema_id, row_id,
				MAX(CASE WHEN attr_id = $1 THEN value_text END) AS symbol,
				MAX(CASE WHEN attr_id = $2 THEN value_text END) AS exchange,
				MAX(CASE WHEN attr_id = $3 THEN value_text END) AS region,
				MAX(CASE WHEN attr_id = $4 THEN value_numeric END) AS trade_type,
				MAX(CASE WHEN attr_id = $5 THEN COALESCE(value_text, CAST(value_numeric AS TEXT)) END) AS trade_time,
				MAX(CASE WHEN attr_id = $6 THEN value_text END) AS name
			FROM eav_data
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
		WHERE cl.flushed_at = 0
	`,
		benchmarkAttributeID(SchemaIDTrade, "symbol"),
		benchmarkAttributeID(SchemaIDTrade, "exchange"),
		benchmarkAttributeID(SchemaIDTrade, "region"),
		benchmarkAttributeID(SchemaIDTrade, "tradeType"),
		benchmarkAttributeID(SchemaIDTrade, "tradeTime"),
		benchmarkAttributeID(SchemaIDTrade, "name"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("load hot state snapshot: %w", err)
	}
	defer rows.Close()
	records := make([]GeneratedRecord, 0)
	keys := make(map[string]struct{})
	for rows.Next() {
		record, err := scanLoadedHotRecord(rows)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
		keys[schemaRowKey(record.SchemaID, record.RowID)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate hot state snapshot: %w", err)
	}
	return records, keys, nil
}

func scanLoadedHotRecord(rows *sql.Rows) (GeneratedRecord, error) {
	var schemaID int16
	var rowID uuid.UUID
	var changedAt int64
	var deletedAt int64
	var text01 sql.NullString
	var text02 sql.NullString
	var smallint01 sql.NullInt16
	var bigint02 sql.NullInt64
	var symbol sql.NullString
	var exchange sql.NullString
	var region sql.NullString
	var tradeType sql.NullFloat64
	var tradeTime sql.NullString
	var name sql.NullString
	if err := rows.Scan(&schemaID, &rowID, &changedAt, &deletedAt, &text01, &text02, &smallint01, &bigint02, &symbol, &exchange, &region, &tradeType, &tradeTime, &name); err != nil {
		return GeneratedRecord{}, fmt.Errorf("scan hot state row: %w", err)
	}
	attrs := make(map[string]any)
	schemaName, err := schemaNameForID(schemaID)
	if err != nil {
		return GeneratedRecord{}, err
	}
	switch schemaID {
	case SchemaIDTrade:
		if symbol.Valid {
			attrs["symbol"] = symbol.String
		} else if text01.Valid {
			attrs["symbol"] = text01.String
		}
		if exchange.Valid {
			attrs["exchange"] = exchange.String
		}
		if region.Valid {
			attrs["region"] = region.String
		} else if text02.Valid {
			attrs["region"] = text02.String
		}
		if tradeType.Valid {
			attrs["tradeType"] = int64(tradeType.Float64)
		} else if smallint01.Valid {
			attrs["tradeType"] = int64(smallint01.Int16)
		}
		if tradeTime.Valid {
			attrs["tradeTime"] = tradeTime.String
		} else if bigint02.Valid {
			attrs["tradeTime"] = strconv.FormatInt(bigint02.Int64, 10)
		}
		if name.Valid {
			attrs["name"] = name.String
		} else if symbol.Valid {
			attrs["name"] = symbol.String
		}
	case SchemaIDCustomer:
		if text02.Valid {
			attrs["region"] = text02.String
		}
		if name.Valid {
			attrs["name"] = name.String
		} else if text01.Valid {
			attrs["name"] = text01.String
		}
	case SchemaIDSecurity:
		if symbol.Valid {
			attrs["symbol"] = symbol.String
		} else if text01.Valid {
			attrs["symbol"] = text01.String
		}
		if name.Valid {
			attrs["companyName"] = name.String
		}
	}
	return GeneratedRecord{SchemaID: schemaID, SchemaName: schemaName, RowID: rowID, Version: 0, ChangedAt: changedAt, DeletedAt: deletedAt, Attributes: attrs}, nil
}

func schemaNameForID(schemaID int16) (string, error) {
	for _, fixture := range DefaultSchemaFixtures() {
		if fixture.ID == schemaID {
			return fixture.Name, nil
		}
	}
	return "", fmt.Errorf("unknown benchmark schema id %d", schemaID)
}

func buildExpectedWorkloadResultFromFederatedTruth(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition, defaultPageSize int, loadedRecords []GeneratedRecord, genCfg GeneratorConfig) (expectedWorkloadResult, error) {
	if h == nil {
		return expectedWorkloadResult{}, fmt.Errorf("harness cannot be nil")
	}
	semantics := semanticsForWorkload(workload, genCfg)
	candidates := filterExpectedRecordsForWorkload(expectedVisibleRecords(loadedRecords), workload, semantics)
	sortExpectedRecordsForWorkload(candidates, workload)
	matching := make([]GeneratedRecord, 0, len(candidates))
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	previousSchemaID := h.SchemaID
	schemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return expectedWorkloadResult{}, err
	}
	h.SchemaID = schemaID
	defer func() {
		h.SchemaID = previousSchemaID
	}()
	for _, candidate := range candidates {
		result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
			Limit: 1,
			Filter: &federated.Filter{
				RowID:      candidate.RowID,
				Conditions: map[string]any{workload.FilterAttribute: workload.FilterValue},
			},
			SortBy:   "tradeTime",
			SortDesc: true,
		})
		if err != nil {
			return expectedWorkloadResult{}, err
		}
		if result.TotalRecords > 0 {
			matching = append(matching, candidate)
		}
	}
	rowIDs := expectedPageRowIDs(matching, workload.DerivedOffset(defaultPageSize), pageSize)
	return expectedWorkloadResult{TotalRecords: int64(len(matching)), RowIDs: rowIDs}, nil
}

func expectedVisibleRecords(records []GeneratedRecord) []GeneratedRecord {
	latest := make(map[string]GeneratedRecord)
	for _, record := range records {
		key := schemaRowKey(record.SchemaID, record.RowID)
		existing, ok := latest[key]
		if ok && !recordWinsExpectedRecord(record, existing) {
			continue
		}
		latest[key] = cloneGeneratedRecord(record)
	}
	out := make([]GeneratedRecord, 0, len(latest))
	for _, record := range latest {
		if record.DeletedAt > 0 {
			continue
		}
		out = append(out, cloneGeneratedRecord(record))
	}
	return out
}

func recordWinsExpectedRecord(candidate, current GeneratedRecord) bool {
	if candidate.ChangedAt != current.ChangedAt {
		return candidate.ChangedAt > current.ChangedAt
	}
	if candidate.Version != current.Version {
		return candidate.Version > current.Version
	}
	if candidate.DeletedAt != current.DeletedAt {
		return candidate.DeletedAt > current.DeletedAt
	}
	return false
}

func filterExpectedRecordsForWorkload(records []GeneratedRecord, workload WorkloadDefinition, semantics workloadSemantics) []GeneratedRecord {
	out := make([]GeneratedRecord, 0)
	for _, record := range records {
		if record.SchemaName != workload.TargetSchema {
			continue
		}
		if semantics.TradeTimeStart > 0 || semantics.TradeTimeEnd > 0 {
			tradeTime := generatedRecordTradeTime(record)
			if semantics.TradeTimeStart > 0 && tradeTime < semantics.TradeTimeStart {
				continue
			}
			if semantics.TradeTimeEnd > 0 && tradeTime > semantics.TradeTimeEnd {
				continue
			}
		}
		if workload.UsesSimpleFilter() && !generatedRecordMatchesFilterForWorkload(record, workload) {
			continue
		}
		out = append(out, cloneGeneratedRecord(record))
	}
	return out
}

func semanticsForWorkload(workload WorkloadDefinition, genCfg GeneratorConfig) workloadSemantics {
	if workload.Name != "mixed-tier-window" {
		return workloadSemantics{}
	}
	baseTime := genCfg.BaseTime
	if baseTime.IsZero() {
		baseTime = defaultBaseTime
	}
	windowDays := genCfg.TimeWindowDays
	if windowDays <= 0 {
		windowDays = DefaultGeneratorConfig().TimeWindowDays
	}
	windowMillis := int64(windowDays) * 24 * int64(time.Hour/time.Millisecond)
	start := baseTime.UnixMilli() - (windowMillis * 4 / 5)
	end := baseTime.UnixMilli() - windowMillis/5
	if start > end {
		start, end = end, start
	}
	return workloadSemantics{TradeTimeStart: start, TradeTimeEnd: end}
}

func benchmarkAttributeID(schemaID int16, name string) int {
	hash := uint32(2166136261)
	input := fmt.Sprintf("%d:%s", schemaID, name)
	for i := 0; i < len(input); i++ {
		hash ^= uint32(input[i])
		hash *= 16777619
	}
	return int(hash%30000) + 1
}

func generatedRecordMatchesFilterForWorkload(record GeneratedRecord, workload WorkloadDefinition) bool {
	value, ok := benchmarkVisibleAttributeValue(record, workload.FilterAttribute)
	if !ok {
		return false
	}
	if workload.FilterAttribute == "tradeType" {
		return fmt.Sprintf("%v", value) == workload.FilterValue
	}
	return fmt.Sprint(value) == workload.FilterValue
}

func benchmarkVisibleAttributeValue(record GeneratedRecord, attribute string) (any, bool) {
	if value, ok := record.Attributes[attribute]; ok {
		return value, true
	}
	if record.SchemaName != "trade" {
		return nil, false
	}
	switch attribute {
	case "symbol", "name":
		value, ok := record.Attributes["symbol"]
		return value, ok
	case "tradeType":
		value, ok := record.Attributes["tradeType"]
		return value, ok
	case "tradeTime":
		value, ok := record.Attributes["tradeTime"]
		return value, ok
	default:
		return nil, false
	}
}

func sortExpectedRecordsForWorkload(records []GeneratedRecord, workload WorkloadDefinition) {
	if workload.TargetSchema == "trade" {
		sort.Slice(records, func(i, j int) bool {
			left := generatedRecordTradeTime(records[i])
			right := generatedRecordTradeTime(records[j])
			if left != right {
				return left > right
			}
			return records[i].RowID.String() < records[j].RowID.String()
		})
		return
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].RowID.String() < records[j].RowID.String()
	})
}

func generatedRecordTradeTime(record GeneratedRecord) int64 {
	value, ok := record.Attributes["tradeTime"]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		parsed, err := time.Parse(time.RFC3339, v)
		if err == nil {
			return parsed.UnixMilli()
		}
		if unixMillis, convErr := strconv.ParseInt(v, 10, 64); convErr == nil {
			return unixMillis
		}
	}
	return 0
}

func expectedPageRowIDs(records []GeneratedRecord, offset, pageSize int) []string {
	if offset >= len(records) {
		return nil
	}
	end := offset + pageSize
	if end > len(records) {
		end = len(records)
	}
	rowIDs := make([]string, 0, end-offset)
	for _, record := range records[offset:end] {
		rowIDs = append(rowIDs, record.RowID.String())
	}
	return rowIDs
}

func validateExpectedWorkloadOutcome(run WorkloadRunResult, expected expectedWorkloadResult) []AssertionResult {
	assertions := []AssertionResult{{
		Name:    "total-records-match-expected",
		Passed:  run.TotalRecords == expected.TotalRecords,
		Message: fmt.Sprintf("actual=%d expected=%d", run.TotalRecords, expected.TotalRecords),
	}}
	actualRows := append([]string(nil), run.RowIDs...)
	expectedRows := append([]string(nil), expected.RowIDs...)
	assertions = append(assertions, AssertionResult{
		Name:    "page-row-ids-match-expected",
		Passed:  stringSlicesEqual(actualRows, expectedRows),
		Message: fmt.Sprintf("actual=%v expected=%v", actualRows, expectedRows),
	})
	return assertions
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func failureKindForRun(run WorkloadRunResult) string {
	if run.InfraError != "" {
		return FailureKindInfra
	}
	if countFailedAssertions(run.Assertions) > 0 {
		return FailureKindCorrectness
	}
	return ""
}

func validateBasicWorkloadAssertions(workload WorkloadDefinition, run WorkloadRunResult) []AssertionResult {
	assertions := []AssertionResult{
		{
			Name:    "non-negative-total-records",
			Passed:  run.TotalRecords >= 0,
			Message: fmt.Sprintf("total_records=%d", run.TotalRecords),
		},
		{
			Name:    "page-size-bound",
			Passed:  run.ResultCount <= run.PageSize,
			Message: fmt.Sprintf("result_count=%d page_size=%d", run.ResultCount, run.PageSize),
		},
		{
			Name:    "result-count-within-total-records",
			Passed:  int64(run.ResultCount) <= run.TotalRecords,
			Message: fmt.Sprintf("result_count=%d total_records=%d", run.ResultCount, run.TotalRecords),
		},
		{
			Name:    "empty-page-only-when-offset-reaches-total",
			Passed:  run.ResultCount > 0 || run.Offset >= int(run.TotalRecords) || run.TotalRecords == 0,
			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
		},
	}
	if workload.Category == WorkloadCategoryDeepPage {
		assertions = append(assertions, AssertionResult{
			Name:    "deep-page-empty-when-offset-exceeds-total",
			Passed:  run.Offset < int(run.TotalRecords) || run.ResultCount == 0,
			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
		})
	}
	return assertions
}

func validatePaginationTransition(previous, current WorkloadRunResult) []AssertionResult {
	assertions := []AssertionResult{{
		Name:    "non-decreasing-offsets-across-pagination-runs",
		Passed:  current.Offset >= previous.Offset,
		Message: fmt.Sprintf("previous_offset=%d current_offset=%d", previous.Offset, current.Offset),
	}}
	if current.Offset > previous.Offset {
		overlap := hasRowIDOverlap(previous.RowIDs, current.RowIDs)
		assertions = append(assertions, AssertionResult{
			Name:    "no-overlap-across-page-slices",
			Passed:  !overlap,
			Message: fmt.Sprintf("previous_rows=%d current_rows=%d", len(previous.RowIDs), len(current.RowIDs)),
		})
	}
	return assertions
}

func validateResultLevelAssertions(workload WorkloadDefinition, run WorkloadRunResult, records []*internal.PersistentRecord, semantics workloadSemantics) []AssertionResult {
	assertions := []AssertionResult{validateUniqueRows(run), validateSchemaScope(workload, records)}
	if workload.UsesSimpleFilter() {
		assertions = append(assertions, validateFilterMatch(workload, records))
	}
	if semantics.TradeTimeStart > 0 || semantics.TradeTimeEnd > 0 {
		assertions = append(assertions, validateTradeTimeWindow(records, semantics))
	}
	if workload.TargetSchema == "trade" && len(records) > 1 {
		assertions = append(assertions, validateSortOrder(records, "tradeTime", true))
	}
	return assertions
}

func validateSchemaScope(workload WorkloadDefinition, records []*internal.PersistentRecord) AssertionResult {
	expectedSchemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: err.Error()}
	}
	for _, record := range records {
		if record.SchemaID != expectedSchemaID {
			return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: fmt.Sprintf("expected_schema=%d actual_schema=%d row=%s", expectedSchemaID, record.SchemaID, record.RowID)}
		}
	}
	return AssertionResult{Name: "schema-scoped-results-match-target", Passed: true, Message: fmt.Sprintf("schema=%s rows=%d", workload.TargetSchema, len(records))}
}

func queryOptionsForWorkload(workload WorkloadDefinition, defaultPageSize int) *federated.QueryOptions {
	return queryOptionsForWorkloadWithConfig(workload, defaultPageSize, DefaultGeneratorConfig())
}

func queryOptionsForWorkloadWithConfig(workload WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig) *federated.QueryOptions {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	opts := &federated.QueryOptions{Limit: pageSize, Offset: workload.DerivedOffset(defaultPageSize)}
	if workload.TargetSchema == "trade" {
		opts.SortBy = "tradeTime"
		opts.SortDesc = true
	}
	semantics := semanticsForWorkload(workload, genCfg)
	opts.TradeTimeStart = semantics.TradeTimeStart
	opts.TradeTimeEnd = semantics.TradeTimeEnd
	return opts
}

func validateUniqueRows(run WorkloadRunResult) AssertionResult {
	seen := make(map[string]struct{}, len(run.RowIDs))
	for _, rowID := range run.RowIDs {
		if _, ok := seen[rowID]; ok {
			return AssertionResult{Name: "unique-row-ids-within-page", Passed: false, Message: rowID}
		}
		seen[rowID] = struct{}{}
	}
	return AssertionResult{Name: "unique-row-ids-within-page", Passed: true, Message: fmt.Sprintf("rows=%d", len(run.RowIDs))}
}

func validateFilterMatch(workload WorkloadDefinition, records []*internal.PersistentRecord) AssertionResult {
	for _, record := range records {
		if !recordMatchesFilter(record, workload.FilterAttribute, workload.FilterValue) {
			return AssertionResult{Name: "filter-results-match-request", Passed: false, Message: fmt.Sprintf("attribute=%s expected=%s row=%s", workload.FilterAttribute, workload.FilterValue, record.RowID)}
		}
	}
	return AssertionResult{Name: "filter-results-match-request", Passed: true, Message: fmt.Sprintf("attribute=%s expected=%s", workload.FilterAttribute, workload.FilterValue)}
}

func validateTradeTimeWindow(records []*internal.PersistentRecord, semantics workloadSemantics) AssertionResult {
	for _, record := range records {
		tradeTime, ok := record.Int64Items["tradeTime"]
		if !ok {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("missing tradeTime row=%s", record.RowID)}
		}
		if semantics.TradeTimeStart > 0 && tradeTime < semantics.TradeTimeStart {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d start=%d", record.RowID, tradeTime, semantics.TradeTimeStart)}
		}
		if semantics.TradeTimeEnd > 0 && tradeTime > semantics.TradeTimeEnd {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d end=%d", record.RowID, tradeTime, semantics.TradeTimeEnd)}
		}
	}
	return AssertionResult{Name: "tradeTime-window-match-request", Passed: true, Message: fmt.Sprintf("rows=%d start=%d end=%d", len(records), semantics.TradeTimeStart, semantics.TradeTimeEnd)}
}

func validateSortOrder(records []*internal.PersistentRecord, attribute string, desc bool) AssertionResult {
	values := make([]int64, 0, len(records))
	for _, record := range records {
		value, ok := record.Int64Items[attribute]
		if !ok {
			continue
		}
		values = append(values, value)
	}
	if len(values) <= 1 {
		return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: "insufficient comparable rows"}
	}
	if desc {
		for i := 1; i < len(values); i++ {
			if values[i] > values[i-1] {
				return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: false, Message: fmt.Sprintf("index=%d prev=%d curr=%d", i, values[i-1], values[i])}
			}
		}
	}
	return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: fmt.Sprintf("comparable_rows=%d", len(values))}
}

func recordMatchesFilter(record *internal.PersistentRecord, attribute, expected string) bool {
	switch attribute {
	case "symbol", "exchange", "region", "name":
		return record.TextItems[attribute] == expected
	case "tradeType":
		return fmt.Sprintf("%d", record.Int64Items[attribute]) == expected
	default:
		return true
	}
}

func persistentRecordIDs(records []*internal.PersistentRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.RowID.String())
	}
	return ids
}

func hasRowIDOverlap(a, b []string) bool {
	seen := make(map[string]struct{}, len(a))
	for _, rowID := range a {
		seen[rowID] = struct{}{}
	}
	for _, rowID := range b {
		if _, ok := seen[rowID]; ok {
			return true
		}
	}
	return false
}

func countFailedAssertions(assertions []AssertionResult) int {
	failed := 0
	for _, assertion := range assertions {
		if !assertion.Passed {
			failed++
		}
	}
	return failed
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
