# Local Benchmark Autoresearch

A performance-oriented autoresearch scaffold for federated benchmark tuning in Forma.

## Purpose

This loop is separate from `tools/autoresearch/testing/`.

- `testing/` is for BDD-style test generation and uses test gates.
- `benchmark/` is for performance tuning and uses benchmark baseline, candidate, diff, and decision artifacts.

The benchmark controller owns the keep/discard decision. The model does not run git commands and does not decide whether a candidate is committed.

## Layout

- `prompts/`
  - `opencode-single-candidate.md`: single-candidate optimization prompt
- `scripts/`
  - `autoloop.sh`: controller loop for one-candidate-per-iteration benchmark tuning
  - `opencode_autoresearch.sh`: prompt launcher for OpenCode
  - `baseline.sh`: captures a baseline benchmark artifact set for a preset
  - `run_candidate.sh`: runs the candidate benchmark and writes a diff artifact
  - `evaluate_benchmark_run.py`: produces a machine-readable keep/discard decision
  - `test_evaluate_benchmark_run.sh`: evaluator regression test

## Decision Artifact

Each candidate iteration produces a JSON decision artifact with:

- overall `status`: `keep` or `discard`
- `reason`
- baseline/candidate summary paths
- diff path
- correctness and infrastructure deltas
- protected workload regressions
- improved and regressed workloads

The controller commits only when the decision artifact says `keep`.

## Quick Start

```bash
./tools/autoresearch/benchmark/scripts/autoloop.sh \
  --model github-copilot/gpt-5 \
  --preset small-live \
  --goal "Improve deep pagination without regressing protected workloads" \
  --baseline \
  --iterations 5
```

## Safety Model

- refuses to run on `main` unless `--force` is used
- creates a dedicated benchmark worktree under `.worktrees/`
- keeps benchmark evidence and decision artifacts under the git common dir, not in tracked files
- controller, not the model, decides commit vs discard
- discard uses controller-owned restore/clean inside the benchmark worktree only

## Notes

- this scaffold intentionally uses benchmark presets instead of per-target performance briefs; target briefs and deterministic gate tiers land in `#53`
- benchmark evidence currently comes from `cmd/benchmark` baseline and compare commands
- protected workloads default to the readiness-gate set from the benchmark operator guide
