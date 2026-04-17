#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

TARGET="${1:-flusher}"

run_go_coverage "$TARGET" candidates

printf 'bdd candidate gate complete for %s\n' "$TARGET"
