#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

write_json() {
  local path="$1"
  local body="$2"
  printf '%s\n' "$body" > "$path"
}

assert_contains() {
  local path="$1"
  local pattern="$2"
  rg "$pattern" "$path" >/dev/null
}

baseline_summary="$TMP_DIR/baseline-summary.json"
candidate_summary_keep="$TMP_DIR/candidate-keep-summary.json"
candidate_summary_discard="$TMP_DIR/candidate-discard-summary.json"
keep_diff="$TMP_DIR/keep-diff.json"
discard_diff="$TMP_DIR/discard-diff.json"
keep_decision="$TMP_DIR/keep-decision.json"
discard_decision="$TMP_DIR/discard-decision.json"

write_json "$baseline_summary" '{"metadata":{"benchmark_id":"bench-a"},"passed":true,"workloads":[{"name":"baseline-page-1","passed":true}]}'
write_json "$candidate_summary_keep" '{"metadata":{"benchmark_id":"bench-b"},"passed":true,"workloads":[{"name":"baseline-page-1","passed":true}]}'
write_json "$candidate_summary_discard" '{"metadata":{"benchmark_id":"bench-c"},"passed":false,"workloads":[{"name":"baseline-page-1","passed":false}]}'

write_json "$keep_diff" '{"summary":{"correctness_failures_delta":0,"infra_failures_delta":0},"workloads":[{"name":"baseline-page-1","passed_changed":false,"correctness_failures_delta":0,"infra_failures_delta":0,"avg_latency_delta":-1000,"p95_latency_delta":-1000,"qps_delta":1.5}]}'
write_json "$discard_diff" '{"summary":{"correctness_failures_delta":0,"infra_failures_delta":0},"workloads":[{"name":"baseline-page-1","passed_changed":false,"correctness_failures_delta":0,"infra_failures_delta":0,"avg_latency_delta":1000,"p95_latency_delta":1000,"qps_delta":-1.5}]}'

python3 "$SCRIPT_DIR/evaluate_benchmark_run.py" \
  --baseline-summary "$baseline_summary" \
  --candidate-summary "$candidate_summary_keep" \
  --diff "$keep_diff" \
  --decision-out "$keep_decision" \
  --protected-workloads baseline-page-1

assert_contains "$keep_decision" '"status": "keep"'
assert_contains "$keep_decision" '"improved_workloads": \['

python3 "$SCRIPT_DIR/evaluate_benchmark_run.py" \
  --baseline-summary "$baseline_summary" \
  --candidate-summary "$candidate_summary_discard" \
  --diff "$discard_diff" \
  --decision-out "$discard_decision" \
  --protected-workloads baseline-page-1

assert_contains "$discard_decision" '"status": "discard"'
assert_contains "$discard_decision" '"reason": "candidate benchmark did not pass"'

printf 'benchmark decision evaluator test passed\n'
