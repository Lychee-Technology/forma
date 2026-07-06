# Federated Query Benchmark Baseline Runbook

Last updated: 2026-04-20  
Repository: `forma`

## Purpose

This runbook defines a repeatable baseline capture flow for the federated query benchmark at `small` and `medium` scale.

## Recommended Baseline Presets

- `ci-smoke`: cheapest artifact-generating preset for pull requests and quick local checks
- `small-live`: default live baseline subset for local or controlled review runs
- `medium-live`: medium-scale live subset for manual regression review
- `heavy-plan`: planning-only heavyweight set for manual or nightly use
- `heavy-live`: full live workload matrix at large scale; manual capacity-aware capture only

Use `go run ./cmd/benchmark describe` to inspect the current preset definitions and workload matrix. The CLI also accepts the legacy aliases `small` -> `small-live` and `medium` -> `medium-live` when running `benchmark baseline`.

## Recommended Commands

### CI Smoke Baseline

```bash
go run ./cmd/benchmark baseline \
  -preset ci-smoke \
  -output-dir .artifacts/benchmark
```

### Small Live Baseline

```bash
go run ./cmd/benchmark baseline \
  -preset small-live \
  -output-dir .artifacts/benchmark
```

### Medium Live Baseline

```bash
go run ./cmd/benchmark baseline \
  -preset medium-live \
  -output-dir .artifacts/benchmark
```

## Output Files

Each baseline directory should contain:

- `benchmark-result.json`
- `benchmark-result.md`
- `benchmark-summary.json`

The default baseline directory naming now follows the benchmark preset name, for example:

- `.artifacts/benchmark/ci-smoke-uniform`
- `.artifacts/benchmark/small-live-hotspot-overlap`
- `.artifacts/benchmark/medium-live-zipf`
- `.artifacts/benchmark/heavy-plan-hotspot-overlap`
- `.artifacts/benchmark/heavy-live/heavy-live-hotspot-overlap`

## Heavy-Live Baseline Capture

`heavy-live` executes the full workload matrix live at `large` scale. It is
the only preset that exercises deep pagination and tier hotness behavior at
production-representative volume.

### Calibration ladder (run before the first official capture)

The large-scale pipeline holds the dataset in memory several times over
(generation, tiering, loaded-state snapshot). Measure before committing to
the full 10M run, recording wall clock, peak RSS, and Docker disk usage:

```bash
/usr/bin/time -l go run ./cmd/benchmark run -mode live -scale large \
  -distribution hotspot-overlap -iterations 2 -seed 42 \
  -trade-count 2000000 -customer-count 200000 -security-count 20000 \
  -truth-pass-sample-cap 10000 -duckdb-memory-mb 8192 \
  -json-out .artifacts/benchmark/heavy-live-probe/2m.json \
  -md-out .artifacts/benchmark/heavy-live-probe/2m.md
```

Repeat with `-trade-count 5000000 -customer-count 500000 -security-count 50000`
(outputs `5m.json` / `5m.md`). Extrapolate: if the projected 10M peak RSS
exceeds machine RAM or the projected runtime is unacceptable, stop and file
the streaming/batched-load follow-up instead of forcing the run.

### Official capture

```bash
/usr/bin/time -l make benchmark-heavy-live
```

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

```bash
go run ./cmd/benchmark baseline -preset heavy-live \
  -distribution hotspot-overlap -tier-profile high-hot-40-20-40 \
  -output-dir .artifacts/benchmark/heavy-live-high-hot \
  -channel manual \
  -git-sha $(git rev-parse HEAD) \
  -git-ref $(git rev-parse --abbrev-ref HEAD)

go run ./cmd/benchmark baseline -preset heavy-live \
  -distribution hotspot-overlap -tier-profile long-history-85-10-5 \
  -output-dir .artifacts/benchmark/heavy-live-long-history \
  -channel manual \
  -git-sha $(git rev-parse HEAD) \
  -git-ref $(git rev-parse --abbrev-ref HEAD)
```

The `-tier-profile` flag on `baseline` depends on the tier-profile override
plumbing added alongside the `heavy-live` preset. Note the tier profile
changes `bench.Config`, so variant runs get their own `BenchmarkID` and never
pollute the balanced baseline trend (same principle as #104's concurrency
identity).

## How To Compare Runs

Compare these fields first:

- `execution_count`
- `p50`
- `p95`
- `p99`
- `max`
- `avg`
- `qps`
- assertion pass/fail counts
- top-level `stability`
- top-level `oracle_provenance`

## Oracle Mode

`oracle_mode` describes how the benchmark derived the expected result set for a workload before comparing it with the actual federated query output.

This is important because the benchmark is not only measuring latency. It is also making a correctness judgment. That judgment depends on what the benchmark believes the correct answer should be, so reviewers need to know where that expected answer came from.

### `loaded-state`

Use this interpretation when `oracle_mode` is `loaded-state`.

- the benchmark reconstructs the expected visible rows from the benchmark's loaded tier state
- base and delta rows come from the generated benchmark records after tier splitting
- hot rows come from the loaded Postgres state snapshot used by the live harness path
- delete shadowing, last-write-wins, page slicing, and time-window filtering are then applied to that reconstructed state

This is the default oracle mode and is appropriate when the benchmark can explain the visible result set directly from the loaded benchmark data model.

Typical examples:

- `baseline-page-1`
- schema-scoped workloads such as `customer-region-page` and `security-symbol-page`
- window workloads such as `mixed-tier-window`, `hot-only-window`, and `cold-only-window`
- deep page workloads

### `truth-pass`

Use this interpretation when `oracle_mode` is `truth-pass`.

- the benchmark still starts from the loaded tier state
- but for candidate rows that match the workload shape, it validates expected visibility by running a targeted federated query through the live harness path
- the resulting expected set is therefore backed by executable federated semantics rather than only by synthetic reconstruction

This mode is used when reproducing the final visible filter semantics directly from benchmark records is not yet trustworthy enough. In practice this is most useful for selective workloads that depend on hot-state visibility rules, EAV-backed filters, or combinations of both.

Typical examples:

- `hot-selective-page`
- `hot-low-selectivity-page`
- `eav-selective-page`
- `mixed-hot-eav-page`

### How To Interpret It

- if `oracle_mode` is `loaded-state`, a correctness failure usually means the query result diverged from the benchmark's reconstructed loaded state
- if `oracle_mode` is `truth-pass`, a correctness failure means the benchmark's final expected answer was confirmed through the live federated path itself
- `truth-pass` is usually more expensive than `loaded-state`, so do not assume all workloads should use it by default
- if a workload changes oracle mode between baselines, treat that as a benchmark methodology change, not a pure performance change

## PreferHot

`prefer_hot` currently records workload intent and benchmark provenance. It tells reviewers that a workload is expected to bias toward hot data freshness or hot-tier-heavy access patterns.

At the current stage, `prefer_hot` should mostly be interpreted as metadata. The benchmark now uses a real hot-preferred execution override only for tier-mix workloads where a Postgres-only hot-path comparison is intentional.

- it appears in workload definitions, run results, summaries, and plan notes
- it helps reviewers distinguish hot-biased workloads from neutral workloads when comparing baselines
- for filter-heavy workloads it does not yet mean the benchmark has applied a hard routing override that excludes cold or mixed execution paths
- for tier-mix workloads such as `hot-only-window`, the benchmark may record that a hot-preferred execution override was applied

Treat any future change that turns `prefer_hot` into a real execution flag as a benchmark methodology change and compare baselines accordingly.

## Interpretation Guidance

- use `small` to catch obvious correctness or latency regressions quickly
- use `small-live` to collect executable baseline evidence without the full heavy workload set
- use `medium-live` to compare behavior across distributions and page-depth workloads
- use `heavy-plan` only when you need planning coverage for the full workload matrix
- use `heavy-live` for large-scale executable evidence (manual, hours)
- treat assertion failures as correctness regressions even if latency improves
- treat large `max` growth separately from percentile movement; it is often a sign of tier skew or unstable deep pagination
- read `correctness_failures` and `infra_failures` separately in benchmark summaries; only the latter indicates an execution-environment problem
- for repeated executions with the same seed, expect `FailureKind`, `total_records`, and page `row_ids` to remain stable for supported workloads
- read workload `oracle_mode` when interpreting selective filter workloads; `truth-pass` means expected results were validated through the live federated path rather than only loaded-state reconstruction
- read top-level `stability` before comparing latency deltas; any unstable workload means correctness evidence changed across same-seed runs and should be investigated first
- read top-level `oracle_provenance` to confirm whether a workload group was judged via `loaded-state` or `truth-pass` without inspecting each workload entry individually

## Runtime Envelopes

- `ci-smoke`: usually under 1 minute on CI runners
- `small-live`: roughly 1 to 5 minutes depending on machine size and Docker startup overhead
- `medium-live`: noticeably slower; use in controlled review environments rather than PR-time CI
- `heavy-plan`: planning-only and suitable for manual or nightly review, not routine pre-merge execution
- `heavy-live`: pending first calibrated capture (see the calibration ladder above); plan for multiple hours on an idle machine, manual capacity-aware runs only

## Current Limitations

- live baseline capture now exists through `small-live` and `medium-live`, but CI should still default to the cheaper `ci-smoke` preset
- use `go run ./cmd/benchmark run -mode live ...` when you need a custom executable workload mix instead of the documented presets
- live benchmark correctness checks compare query results against the benchmark's loaded tier state rather than only the pre-split generated dataset
- selective hot/EAV workloads may use a truth-pass-backed oracle mode to align expected results with the executable federated filter semantics
- baseline capture is designed for artifact stability first, not for production-like throughput measurement
- CI integration and operator workflow guidance are documented in `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`

## Historical Trend Analysis

The `benchmark trend` subcommand provides longitudinal regression tracking across stored runs.

### Capturing History

Tag each run with provenance metadata so the trend engine can group and order runs:

```bash
go run ./cmd/benchmark baseline \
  -preset small-live \
  -output-dir .artifacts/benchmark/history \
  -channel ci \
  -git-sha $(git rev-parse HEAD) \
  -git-ref $(git rev-parse --abbrev-ref HEAD)
```

### Running Trend Analysis

```bash
go run ./cmd/benchmark trend \
  -history-dir .artifacts/benchmark/history \
  -baseline-window 5
```

Optional flags:
- `-candidate` — pin a specific `benchmark-summary.json` as the candidate
- `-drift-window` — number of older runs for baseline drift detection (default 5)
- `-protected-workloads` — comma-separated override of protected workloads
- `-json-out` / `-md-out` — write trend report to a file

### Interpreting Trend Output

- `status=pass` — no regressions or methodology changes detected
- `status=hard_stop_regression` — protected workload correctness or instability regressed
- `status=baseline_drift` — recent baseline window already trending slower than older runs
- `status=methodology_change` — oracle mode or workload set changed between runs
- `status=regression` — latency or qps regression detected (review-only)
