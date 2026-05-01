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
- Total duration: 19.958967418s
- Min: 2.305490744s
- P50: 2.488014587s
- P95: 2.721175324s
- P99: 2.721175324s
- Max: 2.721175324s

## Executions

- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.721175324s offset=0
- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.488014587s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=2.305490744s offset=0
- `mixed-tier-window`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=50 total=27035 duration=2.325123607s offset=0
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.513782625s offset=19980
- `deep-page-1000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.483899031s offset=19980
- `deep-page-100000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=0 total=96265 duration=2.574199592s offset=1999980
- `deep-page-100000`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=0 total=96265 duration=2.547281908s offset=1999980

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

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.38 p95=2.721175324s avg=2.604594955s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.40 p95=2.513782625s avg=2.498840828s avg_result_count=20.00 avg_total_records=96265.00
- `deep-page-100000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.39 p95=2.574199592s avg=2.56074075s avg_result_count=0.00 avg_total_records=96265.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=2 passed=true qps=0.43 p95=2.325123607s avg=2.315307175s avg_result_count=50.00 avg_total_records=27035.00
