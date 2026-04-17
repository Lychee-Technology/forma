#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

ensure_report_dirs

MEDIUM_LOG="$(test_log_path medium-gate)"
rm -f "$MEDIUM_LOG"

{
  printf '[1/2] go e2e harness smoke\n'
  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go test -v ./internal/e2e_harness/... -timeout=5m
  printf '[2/2] go federated e2e short\n'
  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go test -v ./internal/e2e_harness/federated/... -tags=e2e -short -timeout=10m
} | tee "$MEDIUM_LOG"

printf 'medium gate log: %s\n' "$MEDIUM_LOG"
