#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
E2E_DIR="$PROJECT_DIR/tests/e2e"
SERVER_SCRIPT="$PROJECT_DIR/scripts/local_server.sh"
BASE_URL="${BASE_URL:-http://localhost:8080}"
HEALTH_URL="${HEALTH_URL:-$BASE_URL/health}"
SERVER_LOG="${SERVER_LOG:-/tmp/forma-local-server.log}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-180}"
SERVER_PID=""
ACTIVE_PID=""

print_info() {
  printf '[INFO] %s\n' "$1"
}

print_error() {
  printf '[ERROR] %s\n' "$1" >&2
}

cleanup() {
  set +e

  if [ -n "$ACTIVE_PID" ] && kill -0 "$ACTIVE_PID" >/dev/null 2>&1; then
    print_info "Stopping active step (pid=$ACTIVE_PID)"
    kill -TERM -- "-$ACTIVE_PID" >/dev/null 2>&1 || kill -TERM "$ACTIVE_PID" >/dev/null 2>&1 || true
    wait "$ACTIVE_PID" >/dev/null 2>&1 || true
    ACTIVE_PID=""
  fi

  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    print_info "Stopping local Forma server (pid=$SERVER_PID)"
    kill -TERM -- "-$SERVER_PID" >/dev/null 2>&1 || kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi

  if command -v docker >/dev/null 2>&1; then
    print_info "Stopping Docker services"
    docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" down --remove-orphans >/dev/null 2>&1 || true
  fi
}

handle_signal() {
  print_error "Interrupted"
  trap - EXIT
  cleanup
  exit 130
}

run_step() {
  local label="$1"
  shift

  print_info "$label"
  "$@" &
  ACTIVE_PID=$!

  local rc=0
  set +e
  wait "$ACTIVE_PID"
  rc=$?
  set -e
  ACTIVE_PID=""

  return "$rc"
}

trap cleanup EXIT
trap handle_signal INT TERM

if ! command -v curl >/dev/null 2>&1; then
  print_error "curl is required"
  exit 1
fi

if ! command -v bun >/dev/null 2>&1; then
  print_error "bun is required"
  exit 1
fi

if [ ! -x "$SERVER_SCRIPT" ]; then
  print_error "server bootstrap script is missing or not executable: $SERVER_SCRIPT"
  exit 1
fi

print_info "Starting Forma server via $SERVER_SCRIPT"
"$SERVER_SCRIPT" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
print_info "Server logs: $SERVER_LOG"

deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))
while true; do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    print_info "Server is healthy at $HEALTH_URL"
    break
  fi

  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    print_error "Forma server exited before becoming healthy"
    tail -n 80 "$SERVER_LOG" >&2 || true
    exit 1
  fi

  if [ "$SECONDS" -ge "$deadline" ]; then
    print_error "Timed out waiting for Forma server health endpoint: $HEALTH_URL"
    tail -n 80 "$SERVER_LOG" >&2 || true
    exit 1
  fi

  sleep 2
done

run_step "Building k6 bundle" bash -lc "cd \"$E2E_DIR\" && bun run build-k6"

run_step "Running k6 full scenario against $BASE_URL" bash -lc "cd \"$E2E_DIR\" && BASE_URL=\"$BASE_URL\" bun run k6-full"

print_info "k6 full scenario completed"
