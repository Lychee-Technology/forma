#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

PRESET="${1:-small-live}"
RUN_NAME="${2:-$(sanitize_name "$PRESET")}" 

ensure_report_dirs

OUTPUT_DIR="$REPORT_DIR/baseline/$RUN_NAME"
ensure_clean_dir "$OUTPUT_DIR"

GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' \
  go run ./cmd/benchmark baseline -preset "$PRESET" -output-dir "$OUTPUT_DIR" >/dev/null

SUMMARY_PATH="$(first_summary_under "$OUTPUT_DIR")"
printf 'baseline_dir=%s\n' "$OUTPUT_DIR"
printf 'baseline_summary=%s\n' "$SUMMARY_PATH"
