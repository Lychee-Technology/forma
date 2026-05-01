# Federated Query Benchmark Report

- Benchmark ID: `bench-717c6e3be4a076d4`
- Format version: `v1`
- Validation only: false
- Passed: true
- Failure count: 0
- Correctness failures: 0
- Distribution: hotspot-overlap
- Scale: small
- Executions: 3
- Total duration: 2.542856329s
- Min: 212.087841ms
- P50: 221.897004ms
- P95: 2.108871484s
- P99: 2.108871484s
- Max: 2.108871484s

## Executions

- `baseline-page-1`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=96265 duration=2.108871484s offset=0
- `customer-region-page`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=20 total=5495 duration=221.897004ms offset=0
- `security-symbol-page`: passed=true failure_kind= oracle_mode=loaded-state prefer_hot=false failures=0 count=1 total=1 duration=212.087841ms offset=0

## Assertions

- `empty-page-only-when-offset-reaches-total`: passed=3 failed=0
- `filter-results-match-request`: passed=2 failed=0
- `non-negative-total-records`: passed=3 failed=0
- `page-row-ids-match-expected`: passed=3 failed=0
- `page-size-bound`: passed=3 failed=0
- `result-count-within-total-records`: passed=3 failed=0
- `schema-scoped-results-match-target`: passed=3 failed=0
- `sorted-by-tradeTime-desc`: passed=1 failed=0
- `total-records-match-expected`: passed=3 failed=0
- `unique-row-ids-within-page`: passed=3 failed=0

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=0.47 p95=2.108871484s avg=2.108871484s avg_result_count=20.00 avg_total_records=96265.00
- `customer-region-page`: schema=customer oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=4.51 p95=221.897004ms avg=221.897004ms avg_result_count=20.00 avg_total_records=5495.00
- `security-symbol-page`: schema=security oracle_mode=loaded-state prefer_hot=false executions=1 passed=true qps=4.72 p95=212.087841ms avg=212.087841ms avg_result_count=1.00 avg_total_records=1.00
