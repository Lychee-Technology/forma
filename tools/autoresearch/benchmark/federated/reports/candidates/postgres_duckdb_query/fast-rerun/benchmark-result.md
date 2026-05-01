# Federated Query Benchmark Report

- Benchmark ID: `bench-3e87a82eb3e68160`
- Format version: `v1`
- Validation only: false
- Passed: false
- Failure count: 3
- Correctness failures: 0
- Distribution: hotspot-overlap
- Scale: small
- Executions: 3
- Total duration: 0s
- Min: 0s
- P50: 0s
- P95: 0s
- P99: 0s
- Max: 0s

## Executions

- `baseline-page-1`: passed=false failure_kind=infra oracle_mode= prefer_hot=false failures=1 count=0 total=0 duration=0s offset=0
  infra_error=execute workload: build benchmark schema registry: failed to read attributes file /home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/test_entity_attributes.json: open /home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/test_entity_attributes.json: no such file or directory
- `mixed-tier-window`: passed=false failure_kind=infra oracle_mode= prefer_hot=false failures=1 count=0 total=0 duration=0s offset=0
  infra_error=execute workload: count query: IO Error: Failed to read connection error for HTTP GET to 'http://localhost:33355/test-bucket/test-project/102/base/benchmark_base_trade.parquet'

LINE 5: ..., epoch_ms(tradeTime) as tradeTime, 'base' as tier FROM read_parquet('s3://test-bucket/test-project/102/base/*.parquet...
                                                                   ^
- `deep-page-1000`: passed=false failure_kind=infra oracle_mode= prefer_hot=false failures=1 count=0 total=0 duration=0s offset=19980
  infra_error=execute workload: count query: IO Error: Failed to read connection error for HTTP GET to 'http://localhost:33355/test-bucket/test-project/102/base/benchmark_base_trade.parquet'

LINE 5: ..., epoch_ms(tradeTime) as tradeTime, 'base' as tier FROM read_parquet('s3://test-bucket/test-project/102/base/*.parquet...
                                                                   ^

## Workload Summaries

- `baseline-page-1`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=false qps=0.00 p95=0s avg=0s avg_result_count=0.00 avg_total_records=0.00
- `deep-page-1000`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=false qps=0.00 p95=0s avg=0s avg_result_count=0.00 avg_total_records=0.00
- `mixed-tier-window`: schema=trade oracle_mode=loaded-state prefer_hot=false executions=1 passed=false qps=0.00 p95=0s avg=0s avg_result_count=0.00 avg_total_records=0.00
