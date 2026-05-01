# Federated Query Benchmark Report

- Benchmark ID: `bench-3e87a82eb3e68160`
- Format version: `v1`
- Validation only: false
- Passed: false
- Failure count: 2
- Correctness failures: 1
- Distribution: hotspot-overlap
- Scale: small
- Executions: 3
- Total duration: 3.875072144s
- Min: 111.579705ms
- P50: 1.801934984s
- P95: 1.961557455s
- P99: 1.961557455s
- Max: 1.961557455s

## Executions

- `baseline-page-1`: passed=false failure_kind=correctness oracle_mode=loaded-state prefer_hot=false failures=2 count=20 total=12958 duration=111.579705ms offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.801934984s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.961557455s offset=19980

## Assertions

- `deep-page-empty-when-offset-exceeds-total`: passed=1 failed=0
- `empty-page-only-when-offset-reaches-total`: passed=3 failed=0
- `no-overlap-across-page-slices`: passed=1 failed=0
- `non-decreasing-offsets-across-pagination-runs`: passed=1 failed=0
- `non-negative-total-records`: passed=3 failed=0
- `page-row-ids-match-expected`: passed=2 failed=1
- `page-size-bound`: passed=3 failed=0
- `result-count-within-total-records`: passed=3 failed=0
- `schema-scoped-results-match-target`: passed=3 failed=0
- `sorted-by-tradeTime-desc`: passed=3 failed=0
- `total-records-match-expected`: passed=2 failed=1
- `tradeTime-window-match-request`: passed=1 failed=0
- `unique-row-ids-within-page`: passed=3 failed=0

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=false qps=8.96 p95=111.579705ms avg=111.579705ms avg_result_count=20.00 avg_total_records=12958.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.51 p95=1.961557455s avg=1.961557455s avg_result_count=20.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.55 p95=1.801934984s avg=1.801934984s avg_result_count=50.00 avg_total_records=27035.00
