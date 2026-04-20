#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

PRESET="${1:-small-live}"
BASELINE_SUMMARY="${2:-}"
RUN_NAME="${3:-candidate-$(date '+%Y%m%d-%H%M%S')}"

if [[ -z "$BASELINE_SUMMARY" ]]; then
  printf 'baseline summary path is required\n' >&2
  exit 1
fi

ensure_report_dirs

OUTPUT_DIR="$REPORT_DIR/candidates/$RUN_NAME"
ensure_clean_dir "$OUTPUT_DIR"

GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' \
  go run ./cmd/benchmark baseline -preset "$PRESET" -output-dir "$OUTPUT_DIR" >/dev/null

CANDIDATE_SUMMARY="$(first_summary_under "$OUTPUT_DIR")"
DIFF_PATH="$OUTPUT_DIR/benchmark-diff.json"

GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' \
  go run ./cmd/benchmark compare -baseline "$BASELINE_SUMMARY" -candidate "$CANDIDATE_SUMMARY" -diff-out "$DIFF_PATH" >/dev/null

printf 'candidate_dir=%s\n' "$OUTPUT_DIR"
printf 'candidate_summary=%s\n' "$CANDIDATE_SUMMARY"
printf 'diff_path=%s\n' "$DIFF_PATH"
