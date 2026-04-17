#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

ensure_report_dirs

HEAVY_LOG="$(test_log_path heavy-gate)"
rm -f "$HEAVY_LOG"

{
  printf '[1/3] go federated performance suite\n'
  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m
  printf '[2/3] build k6 bundle\n'
  (
    cd "$ROOT_DIR/tests/e2e"
    bun run build-k6
  )
  printf '[3/3] run k6 smoke\n'
  (
    cd "$ROOT_DIR/tests/e2e"
    bun run k6-smoke
  )
} | tee "$HEAVY_LOG"

printf 'heavy gate log: %s\n' "$HEAVY_LOG"
