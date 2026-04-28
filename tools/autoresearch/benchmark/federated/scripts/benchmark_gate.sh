#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/federated/common.sh
source "$SCRIPT_DIR/../common.sh"

TARGET="${1:-$(default_benchmark_target)}"
GATE="${2:-}"
DRY_RUN=0

if [[ "${3:-}" == "--dry-run" || "${2:-}" == "--dry-run" || "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  if [[ "${1:-}" == "--dry-run" ]]; then
    TARGET="$(default_benchmark_target)"
    GATE=""
  elif [[ "${2:-}" == "--dry-run" ]]; then
    GATE=""
  fi
fi

GATE="$(resolve_target_gate "$TARGET" "$GATE")"

ensure_report_dirs
configure_external_federated_env

BASELINE_DIR="$(resolve_target_report_dir baseline "$TARGET" "$GATE")"
CANDIDATE_DIR="$(resolve_target_report_dir candidates "$TARGET" "$GATE")"
DIFF_DIR="$(resolve_target_report_dir diff "$TARGET" "$GATE")"
LOG_PATH="$(benchmark_log_path benchmark-gate-${TARGET}-${GATE})"
DIFF_JSON="$DIFF_DIR/benchmark-diff.json"
DIFF_STDOUT_JSON="$DIFF_DIR/benchmark-diff.stdout.json"
DIFF_SUMMARY="$DIFF_DIR/benchmark-diff.txt"

mkdir -p "$DIFF_DIR"
rm -rf "$DIFF_DIR"/*
rm -f "$LOG_PATH" "$DIFF_JSON" "$DIFF_STDOUT_JSON" "$DIFF_SUMMARY"

if [[ ! -f "$BASELINE_DIR/benchmark-summary.json" ]]; then
  bash "$SCRIPT_DIR/benchmark_baseline.sh" "$TARGET" "$GATE" $([[ "$DRY_RUN" -eq 1 ]] && printf '%s' '--dry-run')
fi

bash "$SCRIPT_DIR/benchmark_candidate.sh" "$TARGET" "$GATE" $([[ "$DRY_RUN" -eq 1 ]] && printf '%s' '--dry-run')

if [[ "$DRY_RUN" -eq 1 ]]; then
  printf 'dry-run complete; compare step not executed because benchmark artifacts were not produced.\n'
  exit 0
fi

{
  printf 'benchmark gate compare\n'
  print_target_context "$TARGET" "$GATE"
  printf 'baseline_summary: %s\n' "$BASELINE_DIR/benchmark-summary.json"
  printf 'candidate_summary: %s\n' "$CANDIDATE_DIR/benchmark-summary.json"
  run_or_print "$DRY_RUN" benchmark_cli compare \
    -baseline "$BASELINE_DIR/benchmark-summary.json" \
    -candidate "$CANDIDATE_DIR/benchmark-summary.json" \
    -diff-out "$DIFF_JSON" \
    > "$DIFF_STDOUT_JSON" \
    2> "$DIFF_SUMMARY"
  printf '\nreview defaults:\n'
  printf -- '- discard on any correctness regression\n'
  printf -- '- rerun on infra failure before judging\n'
  printf -- '- keep only if a target workload clearly improves for this gate\n'
  printf -- '- discard if protected workloads regress clearly\n'
} | tee "$LOG_PATH"

printf 'diff json: %s\n' "$DIFF_JSON"
printf 'diff stdout json: %s\n' "$DIFF_STDOUT_JSON"
printf 'diff summary: %s\n' "$DIFF_SUMMARY"
printf 'gate log: %s\n' "$LOG_PATH"
