## Summary

Add an executable benchmark runtime backed by the live federated harness.

## Why

The current benchmark CLI is still primarily a scaffolded validation path. Before the benchmark can guide optimization or autoresearch decisions, it needs a supported live-execution mode that runs the benchmark flow end to end through the harness.

## Scope

- add a live-execution benchmark mode in `cmd/benchmark`
- wire benchmark runtime setup to the federated harness instead of only validation-only runner paths
- preserve `smoke` and `plan` modes for cheap validation and artifact checks
- document the runtime contract for validation-only versus live benchmark execution

## Acceptance Criteria

- the benchmark CLI can execute supported workloads against a live harness-backed environment
- local and CI operators no longer need to rely on ad hoc test entrypoints for the primary executable benchmark path
- benchmark results clearly identify whether the run was validation-only or live

## References

- `cmd/benchmark/main.go`
- `internal/e2e_harness/federated/benchmark/runner.go`
- `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`
