## Summary

Add tier-aware dataset preparation for base, delta, and hot data, including overlap and soft-delete cases.

## Why

Hybrid query behavior depends on how data is split across parquet and Postgres, not just on row count. The benchmark must be able to prepare realistic cold, warm, and hot datasets with controlled overlap.

## Scope

- add tier mix profiles such as `60/30/10`, `40/20/40`, and `85/10/5`
- write base and delta data to parquet via the existing harness
- insert hot rows into Postgres via seed helpers
- support overlap rows across tiers
- support update, delete, and restore cases

## Acceptance Criteria

- tier preparation works for at least `small` scale
- overlap ratio and delete ratio can be configured
- generated datasets can include last-write-wins cases across tiers
- dataset loading is reusable by all benchmark workloads

## References

- `docs/cn/federated-query-benchmark-hld.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
