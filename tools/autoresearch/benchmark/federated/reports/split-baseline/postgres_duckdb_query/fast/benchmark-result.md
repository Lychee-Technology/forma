# Federated Query Benchmark Report

- Benchmark ID: `bench-3e87a82eb3e68160`
- Format version: `v1`
- Validation only: false
- Passed: true
- Failure count: 0
- Correctness failures: 0
- Distribution: hotspot-overlap
- Scale: small
- Executions: 3
- Total duration: 6.049622716s
- Min: 1.887341306s
- P50: 2.046538695s
- P95: 2.115742715s
- P99: 2.115742715s
- Max: 2.115742715s

## Executions

- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.115742715s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.887341306s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.046538695s offset=19980

## Assertions

- `deep-page-empty-when-offset-exceeds-total`: passed=1 failed=0
- `empty-page-only-when-offset-reaches-total`: passed=3 failed=0
- `no-overlap-across-page-slices`: passed=1 failed=0
- `non-decreasing-offsets-across-pagination-runs`: passed=1 failed=0
- `non-negative-total-records`: passed=3 failed=0
- `page-row-ids-match-expected`: passed=3 failed=0
- `page-size-bound`: passed=3 failed=0
- `result-count-within-total-records`: passed=3 failed=0
- `schema-scoped-results-match-target`: passed=3 failed=0
- `sorted-by-tradeTime-desc`: passed=3 failed=0
- `total-records-match-expected`: passed=3 failed=0
- `tradeTime-window-match-request`: passed=1 failed=0
- `unique-row-ids-within-page`: passed=3 failed=0

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.47 p95=2.115742715s avg=2.115742715s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.49 p95=2.046538695s avg=2.046538695s avg_result_count=20.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.53 p95=1.887341306s avg=1.887341306s avg_result_count=50.00 avg_total_records=27035.00
