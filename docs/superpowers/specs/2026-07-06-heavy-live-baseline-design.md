# Heavy-Live Baseline Design (issue #100)

Date: 2026-07-06
Status: approved in brainstorming session
Issue: Lychee-Technology/forma#100 — Wire up large-scale live benchmark execution

## Goal

Add a repeatable `heavy-live` benchmark preset that executes the full workload
matrix live at `large` scale (10M trades / 1M customers / 100k securities),
with bounded oracle cost, configurable timeout, documented runtime/environment
expectations, and a captured baseline artifact set.

## Decisions Made During Brainstorming

| Question | Decision |
|---|---|
| Run environment / time envelope | Manual, on-demand only. No scheduled job. Document expected runtime instead of enforcing an envelope. |
| Truth-pass oracle at 10M rows | Keep selective workloads; add a sampling cap with spot-check semantics (see §2). |
| Workload set | Full workload matrix, aligned with `heavy-plan` so plan-vs-live artifacts compare directly. |
| Tier profile | Single run on default `balanced-60-30-10`. `high-hot-40-20-40` and `long-history-85-10-5` documented as manual `-tier-profile` variants. |
| Implementation route | Minimal wiring over the existing `RunWithHarness` path + calibration ladder. Memory/streaming hardening only if the ladder proves it necessary (follow-up issue). |

## Key Constraint Discovered

`heavy-plan` never exercises the large-scale pipeline: plan mode
(`Runner.Run`, `runner.go:146-185`) validates fixtures and writes notes
without generating data. The full chain — in-memory generation →
`SplitIntoTiers` → `LoadTieredDataset` → loaded-state snapshot → oracle
construction — is unproven at 10M rows. Peak RSS holds 2-3 copies of the
dataset (dataset / tiered / loadedRecords). This is why §4's calibration
ladder exists.

Truth-pass oracles (`hot-selective-page`, `eav-selective-page`) issue one
federated query per candidate record (`buildExpectedWorkloadResultFromFederatedTruth`,
`runner.go:1312`). At small scale (100k trades, uniform) that was ~24k
queries ≈ 35 min idle (#147). At 10M rows an uncapped pass is days — hence §2.

## §1 Preset and CLI Surface

- New preset `heavy-live` in `defaultBenchmarkPresets()` (`cmd/benchmark/main.go`):
  - `Mode=Live, Scale=Large, Distribution=Hotspot, Iterations=2, PageSize=20, Seed=42`
  - `TierProfile=balanced-60-30-10` (default profile)
  - `Workloads=bench.DefaultWorkloadNames()` (full workload matrix)
  - `BaselineDir="heavy-live-hotspot-overlap"`, `RuntimeClass="heavy"`, `CISafe=false`
  - `ExpectedUsage`: manual capacity-aware baseline capture only
  - `TruthPassSampleCap=10000` (see §2)
  - `DuckDBMemoryLimitMB=8192`; threads stay at 4. Values flow through the
    existing `applyResourcePragmas` single source (#104); flags still override.
- Bare-name alias: `heavy` resolves to `heavy-live`, matching the existing
  `small`→`small-live`, `medium`→`medium-live` convention. `heavy-plan`
  keeps its explicit name and behavior.
- Makefile: new `benchmark-heavy-live` target writing to
  `.artifacts/benchmark/heavy-live` with the usual provenance flags.
  Existing `benchmark-heavy` (plan) target unchanged.

## §2 Truth-Pass Sampling Cap — Spot-Check Semantics

Naively truncating the candidate set would corrupt the expected result
(`TotalRecords` and page rowIDs are derived from it). The cap is therefore a
**spot check**, not a truncation:

- New field `bench.Config.TruthPassSampleCap int`
  (`json:"truth_pass_sample_cap,omitempty"`). `omitempty` preserves
  BenchmarkID continuity for existing baselines (#104 lesson); a test locks
  this.
- `cap == 0` (default): behavior is byte-identical to today. All existing
  presets are unaffected.
- `cap > 0` and `len(candidates) > cap` in
  `buildExpectedWorkloadResultFromFederatedTruth`:
  - The expected result is built from the **full reconstructed candidate
    set** (full sort, full count, unchanged page rowIDs).
  - Federated truth queries run only against a **seeded deterministic
    sample** of `cap` candidates. The sample seed derives from
    `Config.Seed` + workload name, so identical configs sample identically
    (repeatability requirement).
  - Any sampled candidate not visible through the engine → **hard failure**
    with an explicit error. Semantics: the run asserts
    "reconstruction ≡ engine truth" on the sample. A legitimate divergence
    cannot be absorbed by any sampling rate; it requires human
    investigation, never silent acceptance.
- Artifacts: the oracle **mode** is reported as `truth-pass-sampled`
  (hyphenated, consistent with the sibling modes `truth-pass` and
  `loaded-state`); the oracle **note token** is `truth_pass_sampled=N`
  (underscored, consistent with the existing note style
  `oracle_modes loaded_state=… truth_pass=…`). These are two distinct
  identifiers on two different surfaces, not a mismatch. Oracle notes record
  cap, total candidate count, and sampled count. The cap participates in the
  config hash, so capped and uncapped runs never share a BenchmarkID.
- CLI: `-truth-pass-sample-cap` flag on `baseline` and `run` overrides the
  preset value.

## §3 Timeout and Resource Bounds

- New `-run-timeout` duration flag on `baseline` and `run` (default `0` =
  unlimited). When set, wraps the run context with `context.WithTimeout`;
  expiry fails the run with a clear error. Existing signal handling and the
  infra retry policy (`maxInfraRetries=2`) are unchanged.
- DuckDB bounds come from the preset (§1). Postgres-container disk and host
  RAM requirements are not enforced in code; they are measured by the
  calibration ladder (§4) and documented (§5).

## §4 Calibration Ladder (process, no code)

Before the first official 10M baseline, measure incrementally using the
existing `run` subcommand overrides. Record wall clock, peak RSS
(`/usr/bin/time -l` on darwin), and Postgres container disk usage at each
step:

1. **2M probe**: `run -mode live -scale large -trade-count 2000000` +
   full workload set + cap.
2. **5M probe**: same with `-trade-count 5000000`.
3. **10M official**: `make benchmark-heavy-live` on an idle machine.

Abort criterion at each step: extrapolate growth; if projected 10M RSS
exceeds machine RAM or projected runtime is unreasonable, stop and file the
streaming/batched-load follow-up issue (draft body under `.artifacts/`, user
creates or authorizes creation — #143 lesson) instead of forcing the run.
Measured numbers backfill the §5 documentation.

## §5 Documentation Updates

- **Ops guide** (`docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`):
  split the Heavy Run section into `heavy-plan` and `heavy-live`; for
  `heavy-live` document measured expected runtime, RAM/disk/idle-machine
  requirements, manual-on-demand-only policy (no scheduled job), and
  never-in-CI.
- **Runbook** (`docs/federated-query/federated-query-benchmark-baseline-runbook.md`):
  add a heavy-live baseline capture section (commands, artifact dir,
  provenance conventions); manual `-tier-profile` variant commands for
  `high-hot-40-20-40` and `long-history-85-10-5`; a "reading sampled
  oracles" caveat — a sampled pass is weaker than a full truth-pass.

## §6 Testing and Acceptance

Tests:

- Preset resolution: `heavy` alias, `heavy-live` field values.
- Sampling determinism: same seed → same sample.
- Spot-check failure path: injected invisible sampled candidate → hard
  failure with explicit error.
- BenchmarkID continuity: `TruthPassSampleCap` absent ⇒ metadata hash
  unchanged (omitempty lock).
- `cap=0` characterization: existing tests stay green, behavior unchanged.
- Small-scale feasibility (e2e tag, ~30s via the #147 technique): tiny
  TradeCount with cap < candidate count asserts `truth_pass_sampled`
  annotation and a passing run.

Acceptance (mapped to issue #100 AC):

- `benchmark baseline -preset heavy-live` produces live execution artifacts
  without hitting resource limits.
- Runtime envelope documented from calibration-ladder measurements.
- Ops guide updated with large-scale run recommendations.
- Baseline artifact set exists at `.artifacts/benchmark/heavy-live`
  (captured locally per existing convention; not committed to git).

## Non-Goals

- Scheduled (nightly/weekly) execution — explicitly deselected; manual only.
- Distributed or multi-host execution (deferred separately in #100).
- Streaming/batched data generation or loading — only if the calibration
  ladder proves the current in-memory path infeasible, and then as a
  follow-up issue, not in this change.
