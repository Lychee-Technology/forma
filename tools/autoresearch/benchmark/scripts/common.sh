#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
COMMON_GIT_DIR="$(git rev-parse --path-format=absolute --git-common-dir)"
AR_DIR="$ROOT_DIR/tools/autoresearch/benchmark"
REPORT_DIR="$COMMON_GIT_DIR/autoresearch-benchmark"

DEFAULT_PROTECTED_WORKLOADS=(
  baseline-page-1
  hot-selective-page
  hot-low-selectivity-page
  eav-selective-page
  mixed-hot-eav-page
  mixed-tier-window
  hot-only-window
  cold-only-window
  deep-page-1000
)

ensure_report_dirs() {
  mkdir -p "$REPORT_DIR/baseline" "$REPORT_DIR/candidates" "$REPORT_DIR/decisions" "$REPORT_DIR/logs"
}

log_path() {
  local name="$1"
  ensure_report_dirs
  printf '%s/%s.log\n' "$REPORT_DIR/logs" "$name"
}

sanitize_name() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-'
}

join_by_comma() {
  local IFS=,
  printf '%s' "$*"
}

default_protected_workloads_csv() {
  join_by_comma "${DEFAULT_PROTECTED_WORKLOADS[@]}"
}

ensure_clean_dir() {
  local dir="$1"
  rm -rf "$dir"
  mkdir -p "$dir"
}

first_summary_under() {
  local dir="$1"
  local matches=("$dir"/*/benchmark-summary.json)
  if [[ ${#matches[@]} -ne 1 || ! -f "${matches[0]}" ]]; then
    printf 'expected exactly one benchmark-summary.json under %s\n' "$dir" >&2
    return 1
  fi
  printf '%s\n' "${matches[0]}"
}

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    printf '%s\n' 'docker compose'
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    printf '%s\n' 'docker-compose'
    return 0
  fi
  return 1
}

wait_for_pg() {
  local retries=30
  local attempt=1
  while [[ "$attempt" -le "$retries" ]]; do
    if docker exec forma-postgres pg_isready -U postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
  return 1
}

wait_for_rustfs() {
  local retries=30
  local attempt=1
  while [[ "$attempt" -le "$retries" ]]; do
    if curl -fsS "http://localhost:9000/health" >/dev/null 2>&1 || curl -fsS "http://localhost:9001/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
  return 1
}

ensure_local_infra() {
  local compose_bin
  local compose_file
  compose_file="$ROOT_DIR/deploy/docker-compose.yml"

  if [[ ! -f "$compose_file" ]]; then
    printf 'compose file not found: %s\n' "$compose_file" >&2
    return 1
  fi

  if ! compose_bin="$(compose_cmd)"; then
    printf 'docker compose is required but not available\n' >&2
    return 1
  fi

  if [[ "$compose_bin" == "docker compose" ]]; then
    docker compose -f "$compose_file" up -d aurora-dsql rustfs
  else
    docker-compose -f "$compose_file" up -d aurora-dsql rustfs
  fi

  wait_for_pg
  wait_for_rustfs
}
