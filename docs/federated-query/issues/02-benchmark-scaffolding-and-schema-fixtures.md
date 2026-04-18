## Summary

Add benchmark scaffolding and TPC-E-inspired schema fixtures for `trade`, `customer`, and `security`.

## Why

The benchmark needs a stable entrypoint, shared config model, and benchmark-specific schema definitions before any workload or dataset work can be built on top of it.

## Scope

- add benchmark config and result types
- add workload registration interfaces
- create benchmark package layout near the federated harness
- add schema fixtures for `trade`, `customer`, and `security`
- add a thin CLI entrypoint for benchmark execution

## Acceptance Criteria

- benchmark config can be parsed and validated
- schema registration can be invoked through the harness
- benchmark package layout is committed and documented
- schema fixtures cover both hot fields and EAV fields
- a no-op or smoke benchmark run can execute end to end

## References

- `docs/federated-query/federated-query-benchmark-hld-en.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
