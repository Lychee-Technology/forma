## Summary

Add benchmark reporting, metrics export, and baseline capture support.

## Why

The benchmark is only useful if its output can be compared across runs and used to guide optimization work.

## Scope

- export console summaries
- export JSON result files
- export Markdown reports
- record latency percentiles, QPS, result count, tier mix, dedup count, and pushdown efficiency
- add baseline capture guidance for `small` and `medium`

## Acceptance Criteria

- benchmark output can be consumed by both humans and automation
- report formats are stable across runs
- at least one documented baseline procedure exists for `small` and `medium`
- deep-pagination cases are clearly identifiable in exported results

## References

- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
