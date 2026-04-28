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

CANDIDATE_DIR="$(resolve_target_report_dir candidates "$TARGET" "$GATE")"
LOG_PATH="$(benchmark_log_path benchmark-candidate-${TARGET}-${GATE})"
WORKLOADS="$(resolve_target_workloads "$TARGET" "$GATE")"
MODE="$(resolve_target_mode "$TARGET" "$GATE")"
SCALE="$(resolve_target_scale "$TARGET" "$GATE")"
DISTRIBUTION="$(resolve_target_distribution "$TARGET" "$GATE")"
TIER_PROFILE="$(resolve_target_tier_profile "$TARGET" "$GATE")"
ITERATIONS="$(resolve_target_iterations "$TARGET" "$GATE")"
SEED="$(resolve_target_seed "$TARGET" "$GATE")"

mkdir -p "$CANDIDATE_DIR"
rm -rf "$CANDIDATE_DIR"/*
rm -f "$LOG_PATH"

{
  printf 'benchmark candidate capture\n'
  print_target_context "$TARGET" "$GATE"
  printf 'output_dir: %s\n' "$CANDIDATE_DIR"
  run_or_print "$DRY_RUN" benchmark_cli run \
    -mode "$MODE" \
    -scale "$SCALE" \
    -distribution "$DISTRIBUTION" \
    -tier-profile "$TIER_PROFILE" \
    -iterations "$ITERATIONS" \
    -seed "$SEED" \
    -workloads "$WORKLOADS" \
    -baseline-dir "$CANDIDATE_DIR"
} | tee "$LOG_PATH"

printf 'candidate artifacts: %s\n' "$CANDIDATE_DIR"
printf 'candidate log: %s\n' "$LOG_PATH"
