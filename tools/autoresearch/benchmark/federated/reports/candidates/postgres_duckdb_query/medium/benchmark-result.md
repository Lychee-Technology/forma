# Federated Query Benchmark Report

- Benchmark ID: `bench-58e03175693cee3d`
- Format version: `v1`
- Validation only: false
- Passed: true
- Failure count: 0
- Correctness failures: 0
- Distribution: hotspot-overlap
- Scale: small
- Executions: 8
- Total duration: 13.112792794s
- Min: 905.491345ms
- P50: 1.754624502s
- P95: 2.039657966s
- P99: 2.039657966s
- Max: 2.039657966s
- Repeated-run checks enabled: true
- Repeated-run workloads checked: 4
- Repeated-run failure-kind failures: 0
- Repeated-run total-record failures: 0
- Repeated-run page-row-id failures: 0

## Executions

- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.039657966s offset=0
- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.9818277s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.754624502s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=1.744222936s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.891042843s offset=19980
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=1.876942045s offset=19980
- `deep-page-100000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=0 total=96265 duration=918.983457ms offset=1999980
- `deep-page-100000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=0 total=96265 duration=905.491345ms offset=1999980

## Assertions

- `deep-page-empty-when-offset-exceeds-total`: passed=4 failed=0
- `empty-page-only-when-offset-reaches-total`: passed=8 failed=0
- `no-overlap-across-page-slices`: passed=2 failed=0
- `non-decreasing-offsets-across-pagination-runs`: passed=5 failed=0
- `non-negative-total-records`: passed=8 failed=0
- `page-row-ids-match-expected`: passed=8 failed=0
- `page-size-bound`: passed=8 failed=0
- `repeated-run-failure-kind-stable`: passed=4 failed=0
- `repeated-run-page-row-ids-stable`: passed=4 failed=0
- `repeated-run-total-records-stable`: passed=4 failed=0
- `result-count-within-total-records`: passed=8 failed=0
- `schema-scoped-results-match-target`: passed=8 failed=0
- `sorted-by-tradeTime-desc`: passed=6 failed=0
- `total-records-match-expected`: passed=8 failed=0
- `tradeTime-window-match-request`: passed=2 failed=0
- `unique-row-ids-within-page`: passed=8 failed=0

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.50 p95=2.039657966s avg=2.010742833s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.53 p95=1.891042843s avg=1.883992444s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-100000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=1.10 p95=918.983457ms avg=912.237401ms avg_result_count=0.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.57 p95=1.754624502s avg=1.749423719s avg_result_count=50.00 avg_total_records=27035.00

## Oracle Provenance

- `loaded-state`: `baseline-page-1`, `deep-page-1000`, `deep-page-100000`, `mixed-tier-window`
