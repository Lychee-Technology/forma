package benchmark

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

func (r *Runner) buildExpectedResults(ctx context.Context, h *federated.FederatedTestHarness, loadedRecords []GeneratedRecord, hotKeys map[string]struct{}) (map[string]expectedWorkloadResult, map[string]string, []string, error) {
	results := buildExpectedWorkloadResultsFromRecords(loadedRecords, r.workloads, r.config.PageSize, r.genConfig, hotKeys)
	oracleModes := make(map[string]string, len(r.workloads))
	loadedStateCount := 0
	truthPassCount := 0
	sampledCount := 0
	var sampleNotes []string
	for _, workload := range r.workloads {
		mode := string(workload.ResolvedOracleMode())
		oracleModes[workload.Name] = mode
		switch workload.ResolvedOracleMode() {
		case OracleModeTruthPass:
			expected, stats, err := buildExpectedWorkloadResultFromFederatedTruth(ctx, h, workload, r.config.PageSize, loadedRecords, r.genConfig, r.config.TruthPassSampleCap, r.config.Seed)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("build truth-pass expected result for %s: %w", workload.Name, err)
			}
			results[workload.Name] = expected
			truthPassCount++
			if stats.Applied {
				oracleModes[workload.Name] = string(OracleModeTruthPassSampled)
				sampledCount++
				sampleNotes = append(sampleNotes, fmt.Sprintf("truth_pass_sample workload=%s cap=%d candidates=%d sampled=%d", workload.Name, stats.Cap, stats.Candidates, stats.Sampled))
			}
		default:
			loadedStateCount++
		}
	}
	summary := fmt.Sprintf("oracle_modes loaded_state=%d truth_pass=%d", loadedStateCount, truthPassCount)
	if sampledCount > 0 {
		summary = fmt.Sprintf("%s truth_pass_sampled=%d", summary, sampledCount)
	}
	return results, oracleModes, append([]string{summary}, sampleNotes...), nil
}

// resolveRunOracleMode returns the run-time oracle mode for a workload,
// preferring the (possibly sampled) mode recorded in oracleModes and falling
// back to the workload's static resolved mode when the map has no entry.
func resolveRunOracleMode(oracleModes map[string]string, workload WorkloadDefinition) string {
	if mode, ok := oracleModes[workload.Name]; ok && mode != "" {
		return mode
	}
	return string(workload.ResolvedOracleMode())
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
	return buildExpectedWorkloadResultsFromRecords(dataset.Records, workloads, defaultPageSize, dataset.Config, nil)
}

// workloadExecutesPostgresOnly mirrors the execution override in executeWorkload:
// prefer-hot tier-mix workloads run against the Postgres hot buffer only, so
// their loaded-state oracle must be restricted to hot-tier records.
func workloadExecutesPostgresOnly(workload WorkloadDefinition) bool {
	return workload.PreferHot && workload.Category == WorkloadCategoryTierMix
}

func buildExpectedWorkloadResultsFromRecords(records []GeneratedRecord, workloads []WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig, hotKeys map[string]struct{}) map[string]expectedWorkloadResult {
	results := make(map[string]expectedWorkloadResult, len(workloads))
	visible := expectedVisibleRecords(records)
	var hotVisible []GeneratedRecord
	if hotKeys != nil {
		hotVisible = make([]GeneratedRecord, 0, len(hotKeys))
		for _, record := range visible {
			if _, ok := hotKeys[schemaRowKey(record.SchemaID, record.RowID)]; ok {
				hotVisible = append(hotVisible, record)
			}
		}
	}
	for _, workload := range workloads {
		source := visible
		if hotKeys != nil && workloadExecutesPostgresOnly(workload) {
			source = hotVisible
		}
		matching := filterExpectedRecordsForWorkload(source, workload, semanticsForWorkload(workload, genCfg))
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

// truthPassSampleStats reports how truth-pass verification was bounded.
type truthPassSampleStats struct {
	Applied    bool
	Cap        int
	Candidates int
	Sampled    int
}

// truthPassCollectPageSize bounds each batched truth-pass sweep page. It is far
// larger than any small/medium-live hit set, so selective workloads finish in a
// single round trip.
const truthPassCollectPageSize = 1000

// collectVisibleRowIDs pulls the full set of row IDs the engine returns for a
// workload's filter conditions in one paginated sweep (no per-row RowID
// filter). A candidate is visible iff its row id appears here, which is exactly
// the membership the retired per-candidate Limit:1 + RowID probe tested. h.SchemaID
// must already be set to the workload's schema by the caller.
func collectVisibleRowIDs(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (map[uuid.UUID]struct{}, error) {
	visible := make(map[uuid.UUID]struct{})
	conditions := workload.ResolvedFilterConditions()
	for offset := 0; ; offset += truthPassCollectPageSize {
		result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
			Limit:    truthPassCollectPageSize,
			Offset:   offset,
			Filter:   &federated.Filter{Conditions: conditions},
			SortBy:   "tradeTime",
			SortDesc: true,
		})
		if err != nil {
			return nil, fmt.Errorf("execute federated truth query for workload %s (offset %d): %w", workload.Name, offset, err)
		}
		for _, rec := range result.Records {
			if rec != nil {
				visible[rec.RowID] = struct{}{}
			}
		}
		if len(result.Records) < truthPassCollectPageSize || offset+len(result.Records) >= int(result.TotalRecords) {
			break
		}
	}
	return visible, nil
}

// perCandidateVisible probes one candidate's visibility through the engine with
// a RowID-filtered query. Capped (sampled) truth-pass mode uses this so its cost
// stays bounded by the sample cap; the uncapped path uses collectVisibleRowIDs.
func perCandidateVisible(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition, candidate GeneratedRecord) (bool, error) {
	result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
		Limit: 1,
		Filter: &federated.Filter{
			RowID:      candidate.RowID,
			Conditions: workload.ResolvedFilterConditions(),
		},
		SortBy:   "tradeTime",
		SortDesc: true,
	})
	if err != nil {
		return false, fmt.Errorf("execute federated truth query for candidate %s: %w", candidate.RowID, err)
	}
	return result.TotalRecords > 0, nil
}

func buildExpectedWorkloadResultFromFederatedTruth(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition, defaultPageSize int, loadedRecords []GeneratedRecord, genCfg GeneratorConfig, sampleCap int, seed int64) (expectedWorkloadResult, truthPassSampleStats, error) {
	if h == nil {
		return expectedWorkloadResult{}, truthPassSampleStats{}, fmt.Errorf("harness cannot be nil")
	}
	semantics := semanticsForWorkload(workload, genCfg)
	candidates := filterExpectedRecordsForWorkload(expectedVisibleRecords(loadedRecords), workload, semantics)
	sortExpectedRecordsForWorkload(candidates, workload)
	previousSchemaID := h.SchemaID
	schemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return expectedWorkloadResult{}, truthPassSampleStats{}, fmt.Errorf("resolve schema id for workload %s: %w", workload.Name, err)
	}
	h.SchemaID = schemaID
	defer func() {
		h.SchemaID = previousSchemaID
	}()
	// Uncapped mode verifies every candidate, so replace O(candidates)
	// per-candidate RowID probes with one batched visibility sweep: membership
	// is identical (a row is in the swept set iff a RowID-filtered query for it
	// under the same conditions would return a row). Capped (heavy-live) mode
	// only spot-checks a bounded sample, so it keeps the per-candidate probe —
	// sweeping the full hit set there would materialize O(hits) row ids and
	// break the sample cap's cost/memory bound.
	var isVisible func(context.Context, GeneratedRecord) (bool, error)
	if truthPassCapped(len(candidates), sampleCap) {
		isVisible = func(ctx context.Context, candidate GeneratedRecord) (bool, error) {
			return perCandidateVisible(ctx, h, workload, candidate)
		}
	} else {
		visible, err := collectVisibleRowIDs(ctx, h, workload)
		if err != nil {
			return expectedWorkloadResult{}, truthPassSampleStats{}, err
		}
		isVisible = func(_ context.Context, candidate GeneratedRecord) (bool, error) {
			_, ok := visible[candidate.RowID]
			return ok, nil
		}
	}
	return buildTruthPassExpected(ctx, isVisible, workload, defaultPageSize, candidates, sampleCap, seed)
}

// buildTruthPassExpected derives the expected result for a truth-pass
// workload. Uncapped (sampleCap <= 0 or candidates <= cap) it verifies every
// candidate and keeps only the visible ones — existing behavior. Capped, it
// keeps the full reconstructed candidate set as the expected result and
// verifies a seeded deterministic sample; a sampled candidate the engine
// cannot see means reconstruction and engine truth diverge, which no
// sampling rate can absorb, so the run fails hard.
func buildTruthPassExpected(ctx context.Context, isVisible func(context.Context, GeneratedRecord) (bool, error), workload WorkloadDefinition, defaultPageSize int, candidates []GeneratedRecord, sampleCap int, seed int64) (expectedWorkloadResult, truthPassSampleStats, error) {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	sampledIdx := selectTruthPassSampleIndices(seed, workload.Name, len(candidates), sampleCap)
	if sampledIdx == nil {
		matching := make([]GeneratedRecord, 0, len(candidates))
		for _, candidate := range candidates {
			visible, err := isVisible(ctx, candidate)
			if err != nil {
				return expectedWorkloadResult{}, truthPassSampleStats{}, err
			}
			if visible {
				matching = append(matching, candidate)
			}
		}
		rowIDs := expectedPageRowIDs(matching, workload.DerivedOffset(defaultPageSize), pageSize)
		return expectedWorkloadResult{TotalRecords: int64(len(matching)), RowIDs: rowIDs}, truthPassSampleStats{Candidates: len(candidates), Sampled: len(candidates)}, nil
	}
	stats := truthPassSampleStats{Applied: true, Cap: sampleCap, Candidates: len(candidates), Sampled: len(sampledIdx)}
	for i, candidate := range candidates {
		if _, ok := sampledIdx[i]; !ok {
			continue
		}
		visible, err := isVisible(ctx, candidate)
		if err != nil {
			return expectedWorkloadResult{}, stats, err
		}
		if !visible {
			return expectedWorkloadResult{}, stats, fmt.Errorf("truth-pass spot check failed for workload %s: sampled candidate row_id=%s is not visible through the engine; reconstruction diverges from engine truth — investigate at a smaller scale without the sample cap before trusting sampled oracles", workload.Name, candidate.RowID)
		}
	}
	rowIDs := expectedPageRowIDs(candidates, workload.DerivedOffset(defaultPageSize), pageSize)
	return expectedWorkloadResult{TotalRecords: int64(len(candidates)), RowIDs: rowIDs}, stats, nil
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
		if len(workload.ResolvedFilterConditions()) > 0 && !generatedRecordMatchesFilterForWorkload(record, workload) {
			continue
		}
		out = append(out, cloneGeneratedRecord(record))
	}
	return out
}

func semanticsForWorkload(workload WorkloadDefinition, genCfg GeneratorConfig) workloadSemantics {
	if workload.TargetSchema != "trade" {
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
	var start, end int64
	switch workload.Name {
	case "mixed-tier-window":
		start = baseTime.UnixMilli() - (windowMillis * 4 / 5)
		end = baseTime.UnixMilli() - windowMillis/5
	case "hot-only-window":
		start = baseTime.UnixMilli() - windowMillis/5
		end = baseTime.UnixMilli()
	case "cold-only-window":
		start = baseTime.UnixMilli() - windowMillis
		end = baseTime.UnixMilli() - (windowMillis * 4 / 5)
	default:
		return workloadSemantics{}
	}
	if start > end {
		start, end = end, start
	}
	return workloadSemantics{TradeTimeStart: start, TradeTimeEnd: end}
}
func generatedRecordMatchesFilterForWorkload(record GeneratedRecord, workload WorkloadDefinition) bool {
	for key, expected := range workload.ResolvedFilterConditions() {
		value, ok := benchmarkVisibleAttributeValue(record, key)
		if !ok {
			return false
		}
		if key == "tradeType" {
			if fmt.Sprintf("%v", value) != fmt.Sprintf("%v", expected) {
				return false
			}
			continue
		}
		if fmt.Sprint(value) != fmt.Sprint(expected) {
			return false
		}
	}
	return true
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

// sortExpectedRecordsForWorkload orders the oracle's winners the way the
// engine orders the page it is checked against.
//
// Keyset workloads are the exception (#460). Their cursor is built on
// created_at DESC, row_id ASC (execute.go), so a keyset page comes back in
// that order, never in the tradeTime order the offset workloads use. (The
// cursor used to have no other choice: the retired federated allowlist
// accepted system columns only. #381 replaced it with validateKeysetCursor,
// which admits attribute columns too, but this workload stays on the default
// order.)
//
// That divergence used to be invisible: the generator assigns tradeTime FROM
// changed_at, and the reader aliased changed_at into created_at, so the two
// orders were the same sequence by accident. #460 gives every version of a row
// its FIRST version's creation stamp, so for a row with overlapping versions
// created_at (first write) and tradeTime (latest write) now differ and the two
// orders genuinely disagree. The oracle has to follow the engine.
func sortExpectedRecordsForWorkload(records []GeneratedRecord, workload WorkloadDefinition) {
	if workload.UseKeysetPagination {
		sort.Slice(records, func(i, j int) bool {
			if records[i].CreatedAt != records[j].CreatedAt {
				return records[i].CreatedAt > records[j].CreatedAt
			}
			return records[i].RowID.String() < records[j].RowID.String()
		})
		return
	}
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
