## Summary

Scaffold a performance-oriented autoresearch loop around benchmark evidence.

## Why

The repository already has a local autoresearch loop for BDD-style test generation, but performance tuning needs a different controller contract. It needs benchmark baseline capture, workload-level comparison, and decision rules that reject regressions even when some workloads get faster.

## Scope

- add a performance autoresearch mode alongside the existing testing mode
- define decision artifact fields for correctness status, workload regressions, and protected benchmark deltas
- add controller support for baseline, candidate run, and keep or discard evaluation
- preserve the same commit-per-keep and controller-owned git safety model

## Acceptance Criteria

- one performance autoresearch iteration can run a benchmark subset and emit a keep or discard decision
- the controller can reject a candidate when correctness fails or protected workloads regress
- the new performance loop does not weaken the current test-oriented autoresearch safety model

## References

- `tools/autoresearch/testing/README.md`
- `tools/autoresearch/testing/scripts/autoloop.sh`
- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`
