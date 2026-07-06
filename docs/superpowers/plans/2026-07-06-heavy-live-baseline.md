# Heavy-Live Baseline Implementation Plan (issue #100)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable `heavy-live` benchmark preset that executes the full 16-workload matrix live at `large` scale (10M trades) with a sampled truth-pass oracle, a run timeout, and documented runtime/environment expectations.

**Architecture:** Minimal wiring over the existing `RunWithHarness` live path. The only semantic change is a spot-check sampling cap on truth-pass oracle construction: expected results stay derived from the full reconstructed candidate set; federated truth queries run only on a seeded deterministic sample, and any sampled-candidate divergence is a hard failure. Everything else is preset/flag/Makefile/docs plumbing.

**Tech Stack:** Go, testcontainers federated harness (`internal/e2e_harness/federated`), `cmd/benchmark` CLI, Makefile targets.

**Spec:** `docs/superpowers/specs/2026-07-06-heavy-live-baseline-design.md` (approved). Issue: Lychee-Technology/forma#100.

## Global Constraints

- **Never `git add -A` or `git add .`** — always explicit paths (workspace hard rule; the repo carries unrelated untracked files like `.opencode/`, `docs/superpowers/`, `.artifacts/`).
- Work on a feature branch in `.worktrees/<branch>` (repo convention). Before branching: clean up merged local branches, `git pull` on main, and verify `git log origin/main..main` is empty.
- `TruthPassSampleCap` (and any new Config field) MUST be `json:"...,omitempty"` — `BuildArtifactMetadata` hashes the whole `bench.Config` and existing baselines' BenchmarkIDs must not change (locked by tests).
- With `TruthPassSampleCap == 0` every existing behavior must be byte-identical, including the exact note string `oracle_modes loaded_state=%d truth_pass=%d` (asserted by `TestBenchmarkWorkloadExecution_RunWithHarness`).
- Existing oracle-mode strings use hyphens (`truth-pass`, `loaded-state`); the new mode is `truth-pass-sampled`.
- Lint arbiter is CI's pinned golangci-lint v1.64.8 (`make lint`). If local lint disagrees with CI, suspect version drift, not the code.
- Unit tests: `go test ./cmd/benchmark/... ./internal/e2e_harness/federated/benchmark/...`. E2E tests need the `e2e` build tag and Docker.
- Include `Closes #100` in the eventual PR body.

## File Structure

| File | Role in this change |
|---|---|
| `internal/e2e_harness/federated/benchmark/config.go` | Add `TruthPassSampleCap` field + validation |
| `internal/e2e_harness/federated/benchmark/config_test.go` | omitempty lock + validation tests |
| `internal/e2e_harness/federated/benchmark/truth_pass_sample.go` (new) | Pure deterministic sampler |
| `internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go` (new) | Sampler determinism tests + spot-check core tests |
| `internal/e2e_harness/federated/benchmark/workload.go` | `OracleModeTruthPassSampled` constant |
| `internal/e2e_harness/federated/benchmark/runner.go` | Spot-check refactor of truth-pass oracle; notes/mode plumbing |
| `internal/e2e_harness/federated/benchmark/report.go` | Per-workload summary prefers run-time oracle mode |
| `cmd/benchmark/main.go` | `heavy-live` preset, `heavy` alias, `-truth-pass-sample-cap`, `-run-timeout`, DuckDB override guard |
| `cmd/benchmark/main_test.go` | Preset/alias/flag/timeout tests |
| `Makefile` | `benchmark-heavy-live` target |
| `internal/e2e_harness/federated/benchmark_workload_execution_test.go` | e2e sampled-oracle feasibility test |
| `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md` | heavy-plan vs heavy-live split |
| `docs/federated-query/federated-query-benchmark-baseline-runbook.md` | heavy-live capture section + calibration ladder |

---

### Task 1: `TruthPassSampleCap` config field

**Files:**
- Modify: `internal/e2e_harness/federated/benchmark/config.go`
- Test: `internal/e2e_harness/federated/benchmark/config_test.go`

**Interfaces:**
- Produces: `Config.TruthPassSampleCap int` (`json:"truth_pass_sample_cap,omitempty"`), validated `>= 0`. Tasks 3 and 4 read this field.

- [ ] **Step 1: Write the failing tests**

Append to `internal/e2e_harness/federated/benchmark/config_test.go` (the file already imports `encoding/json`, `strings`, `testing` — follow the style of `TestConfigJSONOmitsZeroDuckDBResources` at line 54):

```go
// TestConfigJSONOmitsZeroTruthPassSampleCap protects BenchmarkID continuity:
// BuildArtifactMetadata hashes the whole Config, so an unset cap must not
// change the serialized form of existing configs.
func TestConfigJSONOmitsZeroTruthPassSampleCap(t *testing.T) {
	raw, err := json.Marshal(DefaultConfig())
	if err != nil {
		t.Fatalf("marshal default config: %v", err)
	}
	if strings.Contains(string(raw), "truth_pass_sample_cap") {
		t.Fatalf("zero TruthPassSampleCap must be omitted from JSON, got: %s", raw)
	}
}

func TestConfigValidateRejectsNegativeTruthPassSampleCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TruthPassSampleCap = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected negative truth-pass sample cap to be rejected")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -run 'TruthPassSampleCap' -v`
Expected: FAIL — `cfg.TruthPassSampleCap undefined`.

- [ ] **Step 3: Implement the field**

In `internal/e2e_harness/federated/benchmark/config.go`, extend the `Config` struct (after the DuckDB fields, line 55):

```go
	// TruthPassSampleCap bounds truth-pass oracle construction: when > 0 and
	// a workload's candidate set exceeds the cap, only a seeded deterministic
	// sample of candidates is verified through the engine (spot check) and
	// the expected result is taken from the full reconstructed candidate set.
	// 0 = full truth pass (existing behavior). omitempty keeps the
	// BenchmarkID hash of existing artifacts stable.
	TruthPassSampleCap int `json:"truth_pass_sample_cap,omitempty"`
```

Extend `Validate()` (after the DuckDB checks, line 131):

```go
	if c.TruthPassSampleCap < 0 {
		return fmt.Errorf("truth-pass sample cap must be greater than or equal to zero")
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -run 'Config' -v`
Expected: PASS (including the pre-existing Config tests).

- [ ] **Step 5: Commit**

```bash
git add internal/e2e_harness/federated/benchmark/config.go internal/e2e_harness/federated/benchmark/config_test.go
git commit -m "feat(benchmark): add TruthPassSampleCap config field (#100)"
```

---

### Task 2: Deterministic truth-pass sampler

**Files:**
- Create: `internal/e2e_harness/federated/benchmark/truth_pass_sample.go`
- Test: `internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go`

**Interfaces:**
- Produces: `truthPassSampleIndices(seed int64, workloadName string, total, cap int) map[int]struct{}` — returns `nil` when sampling does not apply (`cap <= 0` or `total <= cap`); otherwise exactly `cap` distinct indices in `[0, total)`, deterministic for identical inputs. Task 3 consumes this.

- [ ] **Step 1: Write the failing tests**

Create `internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go`:

```go
package benchmark

import (
	"reflect"
	"testing"
)

func TestTruthPassSampleIndicesNotAppliedWhenCapCoversTotal(t *testing.T) {
	if got := truthPassSampleIndices(42, "eav-selective-page", 100, 0); got != nil {
		t.Fatalf("cap=0 must disable sampling, got %v", got)
	}
	if got := truthPassSampleIndices(42, "eav-selective-page", 100, 100); got != nil {
		t.Fatalf("total<=cap must disable sampling, got %v", got)
	}
}

func TestTruthPassSampleIndicesDeterministicAndBounded(t *testing.T) {
	first := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	second := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed/workload/total/cap must sample identically")
	}
	if len(first) != 25 {
		t.Fatalf("expected exactly 25 sampled indices, got %d", len(first))
	}
	for idx := range first {
		if idx < 0 || idx >= 1000 {
			t.Fatalf("sampled index %d out of range [0,1000)", idx)
		}
	}
}

func TestTruthPassSampleIndicesVaryBySeedAndWorkload(t *testing.T) {
	base := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	otherSeed := truthPassSampleIndices(43, "eav-selective-page", 1000, 25)
	otherWorkload := truthPassSampleIndices(42, "hot-selective-page", 1000, 25)
	if reflect.DeepEqual(base, otherSeed) && reflect.DeepEqual(base, otherWorkload) {
		t.Fatal("expected seed and workload name to influence the sample")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -run 'TruthPassSampleIndices' -v`
Expected: FAIL — `undefined: truthPassSampleIndices`.

- [ ] **Step 3: Implement the sampler**

Create `internal/e2e_harness/federated/benchmark/truth_pass_sample.go`:

```go
package benchmark

import (
	"hash/fnv"
	"math/rand"
)

// truthPassSampleIndices picks the candidate indices to spot-check when the
// truth-pass sample cap applies. It returns nil when sampling is disabled
// (cap <= 0) or unnecessary (total <= cap). The selection is deterministic
// for a given (seed, workloadName, total, cap) so identical benchmark
// configs verify identical candidates — a repeatability requirement for
// heavy-live baselines (#100).
func truthPassSampleIndices(seed int64, workloadName string, total, cap int) map[int]struct{} {
	if cap <= 0 || total <= cap {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(workloadName))
	rng := rand.New(rand.NewSource(seed ^ int64(h.Sum64()))) //nolint:gosec // deterministic sampling, not crypto
	picked := make(map[int]struct{}, cap)
	for _, idx := range rng.Perm(total)[:cap] {
		picked[idx] = struct{}{}
	}
	return picked
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -run 'TruthPassSampleIndices' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/e2e_harness/federated/benchmark/truth_pass_sample.go internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go
git commit -m "feat(benchmark): deterministic truth-pass sample index selection (#100)"
```

---

### Task 3: Spot-check truth-pass oracle

**Files:**
- Modify: `internal/e2e_harness/federated/benchmark/workload.go` (constant, line ~22)
- Modify: `internal/e2e_harness/federated/benchmark/runner.go` (`buildExpectedResults` line 346, `buildExpectedWorkloadResultFromFederatedTruth` line 1312, `RunWithHarness` notes block lines 333-341)
- Modify: `internal/e2e_harness/federated/benchmark/report.go` (`summarizeWorkloads`, line ~669)
- Test: `internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go` (extend)

**Interfaces:**
- Consumes: `truthPassSampleIndices` (Task 2), `Config.TruthPassSampleCap` (Task 1).
- Produces:
  - `OracleModeTruthPassSampled OracleMode = "truth-pass-sampled"` in `workload.go` (Task 6 asserts it).
  - `type truthPassSampleStats struct { Applied bool; Cap, Candidates, Sampled int }` in `runner.go`.
  - `buildTruthPassExpected(ctx context.Context, isVisible func(context.Context, GeneratedRecord) (bool, error), workload WorkloadDefinition, defaultPageSize int, candidates []GeneratedRecord, sampleCap int, seed int64) (expectedWorkloadResult, truthPassSampleStats, error)` — pure core, unit-tested.
  - `buildExpectedWorkloadResultFromFederatedTruth` gains `sampleCap int, seed int64` params and returns the stats as second value.
  - `(*Runner).buildExpectedResults` now returns notes as `[]string` (first element is the existing `oracle_modes ...` summary line).

- [ ] **Step 1: Write the failing tests for the core**

Append to `internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go` (add imports `context`, `fmt`, `strings`, `github.com/google/uuid` to the file's import block):

```go
func spotCheckCandidates(n int) []GeneratedRecord {
	records := make([]GeneratedRecord, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, GeneratedRecord{
			RowID: uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)),
		})
	}
	return records
}

func TestBuildTruthPassExpectedUncappedKeepsFilteringSemantics(t *testing.T) {
	candidates := spotCheckCandidates(5)
	invisible := map[string]struct{}{
		candidates[1].RowID.String(): {},
		candidates[3].RowID.String(): {},
	}
	calls := 0
	isVisible := func(_ context.Context, candidate GeneratedRecord) (bool, error) {
		calls++
		_, hidden := invisible[candidate.RowID.String()]
		return !hidden, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	expected, stats, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 0, 42)
	if err != nil {
		t.Fatalf("uncapped truth pass failed: %v", err)
	}
	if calls != 5 {
		t.Fatalf("uncapped pass must query every candidate, queried %d of 5", calls)
	}
	if expected.TotalRecords != 3 {
		t.Fatalf("expected invisible candidates removed (total=3), got %d", expected.TotalRecords)
	}
	if stats.Applied {
		t.Fatal("cap=0 must not report sampling as applied")
	}
}

func TestBuildTruthPassExpectedCappedSpotCheckPasses(t *testing.T) {
	candidates := spotCheckCandidates(50)
	calls := 0
	isVisible := func(_ context.Context, _ GeneratedRecord) (bool, error) {
		calls++
		return true, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	expected, stats, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 10, 42)
	if err != nil {
		t.Fatalf("capped spot check failed: %v", err)
	}
	if calls != 10 {
		t.Fatalf("capped pass must query exactly cap candidates, queried %d", calls)
	}
	if expected.TotalRecords != 50 {
		t.Fatalf("capped pass must keep the full candidate set as expected (total=50), got %d", expected.TotalRecords)
	}
	if !stats.Applied || stats.Cap != 10 || stats.Candidates != 50 || stats.Sampled != 10 {
		t.Fatalf("unexpected sample stats: %+v", stats)
	}
}

func TestBuildTruthPassExpectedCappedSpotCheckFailsHardOnInvisibleCandidate(t *testing.T) {
	candidates := spotCheckCandidates(50)
	isVisible := func(_ context.Context, _ GeneratedRecord) (bool, error) {
		return false, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	_, _, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 10, 42)
	if err == nil {
		t.Fatal("expected hard failure when a sampled candidate is not visible")
	}
	if !strings.Contains(err.Error(), "spot check failed") {
		t.Fatalf("error must identify the spot-check divergence, got: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -run 'BuildTruthPassExpected' -v`
Expected: FAIL — `undefined: buildTruthPassExpected`.

- [ ] **Step 3: Add the oracle-mode constant**

In `internal/e2e_harness/federated/benchmark/workload.go`, extend the const block (line 21-22):

```go
	OracleModeLoadedState OracleMode = "loaded-state"
	OracleModeTruthPass   OracleMode = "truth-pass"
	// OracleModeTruthPassSampled marks a truth-pass oracle whose engine
	// verification was spot-checked on a seeded sample because the candidate
	// set exceeded Config.TruthPassSampleCap (#100). The expected result is
	// still the full reconstructed candidate set.
	OracleModeTruthPassSampled OracleMode = "truth-pass-sampled"
```

- [ ] **Step 4: Implement the spot-check core and rewire the wrapper**

In `internal/e2e_harness/federated/benchmark/runner.go`, replace `buildExpectedWorkloadResultFromFederatedTruth` (lines 1312-1353) with:

```go
// truthPassSampleStats reports how truth-pass verification was bounded.
type truthPassSampleStats struct {
	Applied    bool
	Cap        int
	Candidates int
	Sampled    int
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
		return expectedWorkloadResult{}, truthPassSampleStats{}, err
	}
	h.SchemaID = schemaID
	defer func() {
		h.SchemaID = previousSchemaID
	}()
	isVisible := func(ctx context.Context, candidate GeneratedRecord) (bool, error) {
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
			return false, err
		}
		return result.TotalRecords > 0, nil
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
	sampledIdx := truthPassSampleIndices(seed, workload.Name, len(candidates), sampleCap)
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
```

Note: the old body's per-candidate query loop moves verbatim into the `isVisible` closure; do not change the `QueryOptions` shape.

- [ ] **Step 5: Thread cap/seed and notes through `buildExpectedResults`**

Replace `buildExpectedResults` (runner.go lines 346-367) with:

```go
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
```

In `RunWithHarness`, the call site (line 222) keeps its shape (`oracleNotes` is now `[]string`), and the `Notes` block (lines 333-341) becomes:

```go
	notes := []string{
		"loaded TPC-E-inspired schema fixtures",
		fmt.Sprintf("generated dataset with distribution=%s", r.genConfig.Distribution),
		fmt.Sprintf("loaded tiered dataset profile=%s", profile.Name),
		fmt.Sprintf("loaded-state snapshot rows=%d", len(loadedRecords)),
	}
	notes = append(notes, oracleNotes...)
	notes = append(notes,
		"prefer_hot expresses workload intent and report provenance, not hard execution routing",
		"executed supported federated query workloads",
	)
```

and set `Notes: notes,` in the `RunResult` literal. With `cap=0` the notes are element-for-element identical to today.

- [ ] **Step 6: Surface the sampled mode in workload summaries**

In `internal/e2e_harness/federated/benchmark/report.go`, inside `summarizeWorkloads` right after the `if def, ok := workloadDefs[name]; ok { ... }` block (line 673), add:

```go
		if mode, ok := result.OracleModes[name]; ok && mode != "" {
			workload.OracleMode = mode
		}
```

This makes the per-workload summary agree with `RunResult.OracleModes` when sampling rewrites the mode; `detectMethodologyChanges` (report.go:963) then flags capped-vs-uncapped comparisons automatically — desired.

- [ ] **Step 7: Run the package tests**

Run: `go test ./internal/e2e_harness/federated/benchmark/ -v`
Expected: PASS — new `BuildTruthPassExpected` tests green, and every pre-existing test green (cap=0 characterization).

- [ ] **Step 8: Commit**

```bash
git add internal/e2e_harness/federated/benchmark/workload.go internal/e2e_harness/federated/benchmark/runner.go internal/e2e_harness/federated/benchmark/report.go internal/e2e_harness/federated/benchmark/truth_pass_sample_test.go
git commit -m "feat(benchmark): spot-check sampled truth-pass oracle behind TruthPassSampleCap (#100)"
```

---

### Task 4: CLI — `heavy-live` preset, alias, cap/timeout flags, DuckDB override guard

**Files:**
- Modify: `cmd/benchmark/main.go`
- Test: `cmd/benchmark/main_test.go`

**Interfaces:**
- Consumes: `bench.Config.TruthPassSampleCap` (Task 1), `bench.OracleModeTruthPassSampled` (Task 3, describe output only).
- Produces: preset name `heavy-live` (alias `heavy`), flags `-truth-pass-sample-cap` and `-run-timeout` on `baseline` and `run`, helper `applyDuckDBOverrides(cfg bench.Config, threads, memoryMB int) bench.Config`. Task 5's Makefile target and Task 8's ladder commands rely on these.

- [ ] **Step 1: Write the failing tests**

Append to `cmd/benchmark/main_test.go` (file already imports `bytes`, `context`, `testing`, and `bench`):

```go
func TestResolveBenchmarkPresetHeavyLive(t *testing.T) {
	preset, err := resolveBenchmarkPreset("heavy", bench.DistributionHotspot)
	if err != nil {
		t.Fatalf("resolveBenchmarkPreset heavy failed: %v", err)
	}
	if preset.Name != "heavy-live" {
		t.Fatalf("expected heavy alias to resolve to heavy-live, got %q", preset.Name)
	}
	if preset.Config.Mode != bench.ExecutionModeLive || preset.Config.Scale != bench.ScaleLarge {
		t.Fatalf("expected live/large heavy-live config, got %+v", preset.Config)
	}
	if preset.Config.TruthPassSampleCap != 10000 {
		t.Fatalf("expected heavy-live truth-pass sample cap 10000, got %d", preset.Config.TruthPassSampleCap)
	}
	if preset.Config.DuckDBMemoryLimitMB != 8192 {
		t.Fatalf("expected heavy-live DuckDB memory 8192MB, got %d", preset.Config.DuckDBMemoryLimitMB)
	}
	if len(preset.Config.Workloads) != len(bench.DefaultWorkloadNames()) {
		t.Fatalf("expected the full workload matrix, got %d workloads", len(preset.Config.Workloads))
	}
	if preset.CISafe {
		t.Fatal("heavy-live must not be CI-safe")
	}
	if preset.BaselineDir != "heavy-live-hotspot-overlap" {
		t.Fatalf("unexpected baseline dir %q", preset.BaselineDir)
	}
}

func TestApplyDuckDBOverridesKeepsPresetValues(t *testing.T) {
	cfg := bench.Config{DuckDBThreads: 4, DuckDBMemoryLimitMB: 8192}
	got := applyDuckDBOverrides(cfg, 0, 0)
	if got.DuckDBThreads != 4 || got.DuckDBMemoryLimitMB != 8192 {
		t.Fatalf("zero flags must keep preset resources, got %+v", got)
	}
	got = applyDuckDBOverrides(cfg, 8, 16384)
	if got.DuckDBThreads != 8 || got.DuckDBMemoryLimitMB != 16384 {
		t.Fatalf("positive flags must override preset resources, got %+v", got)
	}
}

func TestRunBaselineRunTimeoutExpires(t *testing.T) {
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"baseline", "-preset", "ci-smoke", "-run-timeout", "1ns", "-output-dir", dir}, &out, &errOut)
	if exitCode == 0 {
		t.Fatalf("expected non-zero exit when run timeout expires, stderr=%s", errOut.String())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/benchmark/ -run 'HeavyLive|ApplyDuckDBOverrides|RunTimeout' -v`
Expected: FAIL — `unknown baseline preset "heavy"`, `undefined: applyDuckDBOverrides`, and flag-parse error for `-run-timeout`.

- [ ] **Step 3: Implement the CLI changes**

All in `cmd/benchmark/main.go`:

a) Append the preset to `defaultBenchmarkPresets()` (after `heavy-plan`, line ~505):

```go
		{
			Name:          "heavy-live",
			Description:   "Full live workload matrix at large scale for capacity-aware baseline capture.",
			RuntimeClass:  "heavy",
			BaselineDir:   "heavy-live-hotspot-overlap",
			CISafe:        false,
			ExpectedUsage: "manual capacity-aware baseline capture only (idle machine, hours)",
			Config:        bench.Config{Mode: bench.ExecutionModeLive, Scale: bench.ScaleLarge, Distribution: bench.DistributionHotspot, Iterations: 2, PageSize: 20, Seed: 42, TierProfile: bench.DefaultTierMixProfile().Name, Workloads: bench.DefaultWorkloadNames(), TruthPassSampleCap: 10000, DuckDBMemoryLimitMB: 8192}.WithDefaults(),
		},
```

b) In `resolveBenchmarkPreset` (line ~512), after the `medium` alias:

```go
	if presetName == "heavy" {
		presetName = "heavy-live"
	}
```

c) Update the preset flag usage string (line 136):

```go
	preset := flags.String("preset", "small", "Baseline preset: ci-smoke, small, small-live, medium, medium-live, heavy, heavy-live, or heavy-plan")
```

d) Add the override helper near `baselinePresetConfig`:

```go
// applyDuckDBOverrides keeps preset-provided DuckDB resources unless the
// operator explicitly overrides them (flag default 0 = keep preset/harness
// value). Without this guard the zero-valued flags would erase preset
// resource bounds such as heavy-live's 8192MB memory limit.
func applyDuckDBOverrides(cfg bench.Config, threads, memoryMB int) bench.Config {
	if threads > 0 {
		cfg.DuckDBThreads = threads
	}
	if memoryMB > 0 {
		cfg.DuckDBMemoryLimitMB = memoryMB
	}
	return cfg
}
```

e) In `runBaseline`: add flags after `duckDBMemoryMB` (line 146):

```go
	truthPassCap := flags.Int("truth-pass-sample-cap", -1, "Override truth-pass oracle sample cap (-1 = preset value, 0 = uncapped full truth pass)")
	runTimeout := flags.Duration("run-timeout", 0, "Abort the entire run after this duration (0 = no timeout)")
	tierProfile := flags.String("tier-profile", "", "Override preset tier mix profile (empty = preset value)")
```

Replace the unconditional assignments (lines 158-159) with:

```go
	config = applyDuckDBOverrides(config, *duckDBThreads, *duckDBMemoryMB)
	if *truthPassCap >= 0 {
		config.TruthPassSampleCap = *truthPassCap
	}
	if *tierProfile != "" {
		config.TierProfile = *tierProfile
	}
	if *runTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runTimeout)
		defer cancel()
	}
```

f) In `parseRunConfig`: add the same two flags after `duckDBMemoryMB` (line 358) but with `truthPassCap := flags.Int("truth-pass-sample-cap", 0, "Truth-pass oracle sample cap (0 = uncapped full truth pass)")`, set `TruthPassSampleCap: *truthPassCap,` in the `bench.Config` literal, and surface the timeout by adding a `runTimeout time.Duration` field to `runOutputs` set from the flag. Then in `runBenchmark` (line 125), before `executeBenchmarkRun`:

```go
	if outputs.runTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, outputs.runTimeout)
		defer cancel()
	}
```

(`main.go` already imports `time`? If not, add `"time"` to the import block.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/benchmark/ -v`
Expected: PASS — new tests green, `TestResolveBenchmarkPresetSupportsAliasesAndPresets` and the ci-smoke baseline test still green.

- [ ] **Step 5: Commit**

```bash
git add cmd/benchmark/main.go cmd/benchmark/main_test.go
git commit -m "feat(benchmark): heavy-live preset, truth-pass cap and run-timeout flags (#100)"
```

---

### Task 5: Makefile target

**Files:**
- Modify: `Makefile` (`.PHONY` line 1; new target after `benchmark-heavy`, line ~108)

**Interfaces:**
- Consumes: `baseline -preset heavy-live` (Task 4).
- Produces: `make benchmark-heavy-live` writing to `.artifacts/benchmark/heavy-live/heavy-live-hotspot-overlap`. Task 7 documents it; Task 8 runs it.

- [ ] **Step 1: Add the target**

Add `benchmark-heavy-live` to the `.PHONY` list (line 1), and after the `benchmark-heavy` target:

```make
# Full live workload matrix at large scale (10M trades). Hours of wall clock
# and heavy RAM/disk — operator-initiated on an idle machine only, never CI.
# Truth-pass oracles are spot-check sampled (see the baseline runbook).
benchmark-heavy-live: create-build-dir
	@echo "Running benchmark heavy live set (hours; idle machine only)..."
	@mkdir -p .artifacts/benchmark/heavy-live
	@$(GOENV) go run ./cmd/benchmark baseline -preset heavy-live \
		-distribution hotspot-overlap \
		-output-dir .artifacts/benchmark/heavy-live \
		-channel manual \
		-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
		-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
```

Note `-distribution hotspot-overlap` is explicit: the flag default is `uniform` and `resolveBenchmarkPreset` always applies a non-empty distribution flag over the preset value.

- [ ] **Step 2: Verify the target expands correctly**

Run: `make -n benchmark-heavy-live`
Expected: dry-run output shows the `baseline -preset heavy-live -distribution hotspot-overlap ...` command; no execution.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(benchmark): make benchmark-heavy-live target (#100)"
```

---

### Task 6: e2e sampled-oracle feasibility test

**Files:**
- Modify: `internal/e2e_harness/federated/benchmark_workload_execution_test.go`

**Interfaces:**
- Consumes: `bench.Config.TruthPassSampleCap`, `bench.OracleModeTruthPassSampled`, existing `RunWithHarness` tiny-dataset technique (#147: ~30s per run under the `e2e` tag).

- [ ] **Step 1: Write the test**

Append to `internal/e2e_harness/federated/benchmark_workload_execution_test.go` (add `"strings"` to imports):

```go
func TestBenchmarkTruthPassSampledSpotCheck_RunWithHarness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	runner, err := bench.NewRunner(bench.Config{
		Mode:               bench.ExecutionModeLive,
		Scale:              bench.ScaleSmall,
		Distribution:       bench.DistributionHotspot,
		Iterations:         1,
		PageSize:           20,
		Seed:               42,
		TradeCount:         300,
		CustomerCount:      20,
		SecurityCount:      10,
		OverlapRatio:       0.10,
		DeleteRatio:        0.05,
		TruthPassSampleCap: 5,
		Workloads: []string{
			"baseline-page-1",
			"hot-selective-page",
			"eav-selective-page",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.True(t, result.Passed, "sampled heavy-live semantics must still pass at tiny scale")
	require.Equal(t, string(bench.OracleModeTruthPassSampled), result.OracleModes["hot-selective-page"])
	require.Equal(t, string(bench.OracleModeTruthPassSampled), result.OracleModes["eav-selective-page"])

	sampledNote := false
	for _, note := range result.Notes {
		if strings.Contains(note, "truth_pass_sampled=2") {
			sampledNote = true
		}
	}
	require.True(t, sampledNote, "expected sampled oracle summary note, got %v", result.Notes)
}
```

If a selective workload's candidate count at TradeCount=300 turns out to be ≤ 5 (sampling then never engages and the mode stays `truth-pass`), raise `TradeCount` to 600 rather than lowering the cap below 5.

- [ ] **Step 2: Run the e2e tests (Docker required)**

Run: `go test -tags e2e ./internal/e2e_harness/federated/ -run 'TestBenchmarkTruthPassSampledSpotCheck|TestBenchmarkWorkloadExecution_RunWithHarness' -v -timeout 15m`
Expected: both PASS — the new sampled test, and the pre-existing execution test still emitting the exact note `oracle_modes loaded_state=8 truth_pass=8` (cap=0 characterization).

- [ ] **Step 3: Commit**

```bash
git add internal/e2e_harness/federated/benchmark_workload_execution_test.go
git commit -m "test(benchmark): e2e sampled truth-pass spot-check at tiny scale (#100)"
```

---

### Task 7: Documentation — ops guide and runbook

**Files:**
- Modify: `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md` (Heavy Run section, line ~99; recommendations, lines ~186, ~241)
- Modify: `docs/federated-query/federated-query-benchmark-baseline-runbook.md` (preset list line ~15, artifact dirs line ~58, usage guidance lines ~139-155)

**Interfaces:**
- Consumes: preset/flags (Task 4), Makefile target (Task 5). Runtime numbers come from Task 8 and are marked "pending first capture" until then.

- [ ] **Step 1: Update the ops guide**

In `federated-query-benchmark-ci-and-ops-guide.md`, replace the `### Heavy Run` section with two subsections:

```markdown
### Heavy Plan Run

- preset: `heavy-plan`
- mode: plan (validation only — it does not generate data or execute queries)
- scale: `large`
- expected runtime: minutes
- command: `make benchmark-heavy`

### Heavy Live Run

- preset: `heavy-live`
- mode: live, full 16-workload matrix
- scale: `large` (10M trades / 1M customers / 100k securities)
- distribution: `hotspot-overlap`; tier profile: `balanced-60-30-10`
- truth-pass oracles are spot-check sampled (`truth_pass_sample_cap=10000`);
  a sampled pass asserts reconstruction ≡ engine truth on the sample only —
  see the baseline runbook before comparing against uncapped baselines
- resource bounds: DuckDB memory 8192MB (preset), `-duckdb-memory-mb` to
  override; `-run-timeout` aborts a stuck run
- expected runtime and peak RSS/disk: pending first calibrated capture (see
  the calibration ladder in the baseline runbook); plan for multiple hours
  on an idle machine
- policy: manual, on-demand only. Never in CI, no scheduled job. Run on an
  idle machine — loaded machines inflate truth-pass oracle cost by 5-10x
- command: `make benchmark-heavy-live`
```

Update the surrounding references: line ~186 `reserve benchmark-heavy for manual or nightly jobs` becomes `reserve benchmark-heavy and benchmark-heavy-live for manual capacity-aware runs`; keep line ~241 (`large` reserved for capacity-aware environments); remove `heavy preset only runs plan mode` phrasing wherever it appears (it was the gap #100 closes).

- [ ] **Step 2: Update the baseline runbook**

In `federated-query-benchmark-baseline-runbook.md`:

a) Preset list (line ~15): add `- \`heavy-live\`: full live workload matrix at large scale; manual capacity-aware capture only`.

b) Artifact dirs (line ~58): add `- \`.artifacts/benchmark/heavy-live/heavy-live-hotspot-overlap\``.

c) Add a new section after the existing capture instructions:

```markdown
## Heavy-Live Baseline Capture

`heavy-live` executes the full workload matrix live at `large` scale. It is
the only preset that exercises deep pagination and tier hotness behavior at
production-representative volume.

### Calibration ladder (run before the first official capture)

The large-scale pipeline holds the dataset in memory several times over
(generation, tiering, loaded-state snapshot). Measure before committing to
the full 10M run, recording wall clock, peak RSS, and Docker disk usage:

    /usr/bin/time -l go run ./cmd/benchmark run -mode live -scale large \
      -distribution hotspot-overlap -iterations 2 -seed 42 \
      -trade-count 2000000 -customer-count 200000 -security-count 20000 \
      -truth-pass-sample-cap 10000 -duckdb-memory-mb 8192 \
      -json-out .artifacts/benchmark/heavy-live-probe/2m.json \
      -md-out .artifacts/benchmark/heavy-live-probe/2m.md

Repeat with `-trade-count 5000000 -customer-count 500000 -security-count 50000`
(outputs `5m.json` / `5m.md`). Extrapolate: if the projected 10M peak RSS
exceeds machine RAM or the projected runtime is unacceptable, stop and file
the streaming/batched-load follow-up instead of forcing the run.

### Official capture

    /usr/bin/time -l make benchmark-heavy-live

- idle machine only; check load first (truth-pass throughput drops 5-10x
  under load)
- use `-run-timeout` when running unattended
- expected runtime / peak RSS / disk: fill in from the first successful
  capture and keep current

### Reading sampled truth-pass results

`heavy-live` reports `truth-pass-sampled` oracle mode for selective
workloads: the expected result is the full reconstructed candidate set, and
the engine was consulted for a seeded sample of `truth_pass_sample_cap`
candidates. A sampled pass is strictly weaker evidence than an uncapped
truth pass; `compare` flags capped-vs-uncapped runs as a methodology change.
A spot-check failure means reconstruction and engine truth diverge — rerun
the failing workload uncapped at `small` scale to investigate.

### Tier-profile variants

The official baseline uses `balanced-60-30-10`. To exercise the profiles
called out in #100 manually:

    go run ./cmd/benchmark baseline -preset heavy-live \
      -distribution hotspot-overlap -tier-profile high-hot-40-20-40 \
      -output-dir .artifacts/benchmark/heavy-live-high-hot ...

    go run ./cmd/benchmark baseline -preset heavy-live \
      -distribution hotspot-overlap -tier-profile long-history-85-10-5 \
      -output-dir .artifacts/benchmark/heavy-live-long-history ...
```

The `-tier-profile` flag on `baseline` is added in Task 4 step 3e — the variant commands above depend on it. Note the tier profile changes `bench.Config`, so variant runs get their own BenchmarkID and never pollute the balanced baseline trend (same principle as #104's concurrency identity).

d) Update the usage guidance list (lines ~139-155): change `use heavy-plan only when you need planning coverage for the full workload matrix` to also mention `use heavy-live for large-scale executable evidence (manual, hours)`.

- [ ] **Step 3: Verify docs**

Run: `grep -n "heavy-live" docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md docs/federated-query/federated-query-benchmark-baseline-runbook.md | head -20`
Expected: both files reference the preset; no stale "heavy preset only runs plan mode" text remains (`grep -rn "only runs plan" docs/federated-query/` returns nothing).

- [ ] **Step 4: Commit**

```bash
git add docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md docs/federated-query/federated-query-benchmark-baseline-runbook.md
git commit -m "docs(benchmark): heavy-live run policy, calibration ladder, sampled-oracle guidance (#100)"
```

---

### Task 8: Operator checkpoint — calibration ladder and official baseline (manual)

**This task is not for an AFK implementer.** It needs hours on an idle machine and judgment calls; stop here and report. The operator (user) runs:

- [ ] **Step 1: 2M probe** — the `run` command from the runbook's calibration ladder (Task 7 §2c), recording wall clock / peak RSS / Docker disk.
- [ ] **Step 2: 5M probe** — same with the 5M counts. Extrapolate to 10M; abort criteria per runbook. If aborting: draft the streaming/batched-load follow-up issue body under `.artifacts/` for the user to create (do not `gh issue create` from an AFK session — #143 lesson).
- [ ] **Step 3: Official 10M capture** — `/usr/bin/time -l make benchmark-heavy-live` on an idle machine. Artifacts land in `.artifacts/benchmark/heavy-live/heavy-live-hotspot-overlap` (local convention; not committed).
- [ ] **Step 4: Backfill measured numbers** into the two docs from Task 7 (runtime, peak RSS, disk) and commit:

```bash
git add docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md docs/federated-query/federated-query-benchmark-baseline-runbook.md
git commit -m "docs(benchmark): backfill measured heavy-live runtime envelope (#100)"
```

---

## Final Verification (before PR)

- [ ] `go test ./cmd/benchmark/... ./internal/e2e_harness/federated/benchmark/...` — all green.
- [ ] `make lint` — clean (pinned golangci-lint v1.64.8).
- [ ] `go test -tags e2e ./internal/e2e_harness/federated/ -run 'TestBenchmark' -v -timeout 20m` — sampled spot-check test and existing execution test green (Docker required).
- [ ] `go run ./cmd/benchmark describe | grep -A3 heavy-live` — preset visible with cap and memory bounds.
- [ ] PR body: `Closes #100`, notes that Task 8 (calibration ladder + official capture) is an operator step, links `docs/superpowers/specs/2026-07-06-heavy-live-baseline-design.md`. Include the spec and this plan in the PR with explicit `git add` paths.
