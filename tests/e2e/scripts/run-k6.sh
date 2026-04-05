#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <smoke|full|perf> [extra k6 args...]" >&2
  exit 1
fi

SCENARIO="$1"
shift

case "$SCENARIO" in
  smoke|full|perf)
    ;;
  *)
    echo "unsupported scenario: $SCENARIO" >&2
    echo "expected one of: smoke, full, perf" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUNDLE_PATH="$ROOT_DIR/k6/dist/bundle.js"
REPORT_PATH="reports/k6-${SCENARIO}.json"

if [ ! -f "$BUNDLE_PATH" ]; then
  echo "k6 bundle not found: $BUNDLE_PATH" >&2
  echo "run 'bun run build-k6' in tests/e2e first." >&2
  exit 1
fi

cd "$ROOT_DIR"

if command -v k6 >/dev/null 2>&1; then
  exec k6 run --out "json=${REPORT_PATH}" -e "SCENARIO=${SCENARIO}" k6/dist/bundle.js "$@"
fi

if command -v docker >/dev/null 2>&1; then
  exec docker run --rm -i \
    -u "$(id -u):$(id -g)" \
    -v "$ROOT_DIR:/work" \
    -w /work \
    -e BASE_URL \
    -e AUTH_TOKEN \
    -e SCHEMAS \
    grafana/k6 \
    run --out "json=${REPORT_PATH}" -e "SCENARIO=${SCENARIO}" k6/dist/bundle.js "$@"
fi

echo "k6 is not installed and Docker is unavailable." >&2
echo "install k6 locally or run with Docker available." >&2
exit 127
