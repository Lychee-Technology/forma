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
- Total duration: 5.621728595s
- Min: 1.75313109s
- P50: 1.887336443s
- P95: 1.981261062s
- P99: 1.981261062s
- Max: 1.981261062s
- Repeated-run checks enabled: false

## Executions

- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.981261062s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.75313109s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.887336443s offset=19980

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

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.50 p95=1.981261062s avg=1.981261062s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.53 p95=1.887336443s avg=1.887336443s avg_result_count=20.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.57 p95=1.75313109s avg=1.75313109s avg_result_count=50.00 avg_total_records=27035.00

## Oracle Provenance

- `loaded-state`: `baseline-page-1`, `deep-page-1000`, `mixed-tier-window`
