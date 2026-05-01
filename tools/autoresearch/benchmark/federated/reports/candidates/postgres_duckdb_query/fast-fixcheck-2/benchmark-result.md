# Federated Query Benchmark Report

- Benchmark ID: `bench-3e87a82eb3e68160`
- Format version: `v1`
- Validation only: false
- Passed: false
- Failure count: 1
- Correctness failures: 0
- Distribution: hotspot-overlap
- Scale: small
- Executions: 3
- Total duration: 3.810663836s
- Min: 0s
- P50: 1.822843054s
- P95: 1.987820782s
- P99: 1.987820782s
- Max: 1.987820782s

## Executions

- `baseline-page-1`: passed=false failure_kind=infra oracle_mode= prefer_hot=false failures=1 count=0 total=0 duration=0s offset=0
  infra_error=execute workload: failed to query persistent records: execute optimized query: ERROR: column e.array_indices does not exist (SQLSTATE 42703)
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.822843054s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.987820782s offset=19980

## Assertions

- `deep-page-empty-when-offset-exceeds-total`: passed=1 failed=0
- `empty-page-only-when-offset-reaches-total`: passed=2 failed=0
- `non-negative-total-records`: passed=2 failed=0
- `page-row-ids-match-expected`: passed=2 failed=0
- `page-size-bound`: passed=2 failed=0
- `result-count-within-total-records`: passed=2 failed=0
- `schema-scoped-results-match-target`: passed=2 failed=0
- `sorted-by-tradeTime-desc`: passed=2 failed=0
- `total-records-match-expected`: passed=2 failed=0
- `tradeTime-window-match-request`: passed=1 failed=0
- `unique-row-ids-within-page`: passed=2 failed=0

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=false qps=0.00 p95=0s avg=0s avg_result_count=0.00 avg_total_records=0.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.50 p95=1.987820782s avg=1.987820782s avg_result_count=20.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.55 p95=1.822843054s avg=1.822843054s avg_result_count=50.00 avg_total_records=27035.00
