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

cd "$ROOT_DIR"

# Build the k6 bundle if it is missing or stale relative to the source.
if [ ! -f "$BUNDLE_PATH" ] || [ "k6/scenarios.ts" -nt "$BUNDLE_PATH" ]; then
  echo "Building k6 bundle..." >&2
  bun run build-k6
fi

# Seed data before the load test — an empty DB makes every check vacuously
# true. Also flush to S3 so warm/cold tiers exist and the DuckDB-forced
# requests have data to read (a hot-only DB reduces the "federated" load to
# Postgres). Skip with SKIP_SEED=1 when the stack is already populated.
if [ "${SKIP_SEED:-0}" != "1" ]; then
  echo "Seeding data before load test (set SKIP_SEED=1 to skip)..." >&2
  bun run register-schemas
  bun run gen-data
  echo "Flushing to S3 so warm/cold tiers exist..." >&2
  bun run cdc-flush
fi

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
