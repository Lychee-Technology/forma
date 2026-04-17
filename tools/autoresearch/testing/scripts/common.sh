#!/usr/bin/env bash

set -euo pipefail

resolve_repo_root_dir() {
  local common_dir

  common_dir="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
  if [[ -n "$common_dir" && "$(basename "$common_dir")" == ".git" ]]; then
    dirname "$common_dir"
    return 0
  fi

  git rev-parse --show-toplevel
}

ROOT_DIR="$(resolve_repo_root_dir)"
AR_DIR="$ROOT_DIR/tools/autoresearch/testing"
REPORT_DIR="$AR_DIR/reports"

ensure_report_dirs() {
  mkdir -p "$REPORT_DIR/baseline" "$REPORT_DIR/candidates" "$REPORT_DIR/logs"
}

resolve_target_pkg() {
  case "${1:-}" in
    flusher)
      printf '%s\n' './internal/cdc'
      ;;
    postgres_duckdb_query)
      printf '%s\n' './internal'
      ;;
    entity_query_service)
      printf '%s\n' './internal'
      ;;
    postgres_repo_query)
      printf '%s\n' './internal'
      ;;
    *)
      printf 'unknown target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_focus_regex() {
  case "${1:-}" in
    flusher)
      printf '%s\n' 'processSchema|shouldFlush|executeFlush|executeBatch|executeFlushInChunks|executeFlushSingle'
      ;;
    postgres_duckdb_query)
      printf '%s\n' 'StreamDuckDBFederatedQuery|fetchAndRecordDirtyIDs|buildDuckDBQueryWithPlan|streamDuckDBRows|finalizeDuckDBExecutionPlan'
      ;;
    entity_query_service)
      printf '%s\n' 'entityQueryService|CrossSchemaSearch|validateCrossSchemaRequest|buildSchemaContexts'
      ;;
    postgres_repo_query)
      printf '%s\n' 'StreamOptimizedQuery|runOptimizedQuery|buildHybridConditions|hybridConditionBuilder'
      ;;
    *)
      printf 'unknown target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_source() {
  case "${1:-}" in
    flusher)
      printf '%s\n' 'internal/cdc/flusher.go'
      ;;
    postgres_duckdb_query)
      printf '%s\n' 'internal/postgres_duckdb_query.go'
      ;;
    entity_query_service)
      printf '%s\n' 'internal/entity_query_service.go'
      ;;
    postgres_repo_query)
      printf '%s\n' 'internal/postgres_persistent_repository_query.go'
      ;;
    *)
      printf 'unknown target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_brief() {
  case "${1:-}" in
    flusher)
      printf '%s\n' 'tools/autoresearch/testing/targets/flusher.md'
      ;;
    postgres_duckdb_query)
      printf '%s\n' 'tools/autoresearch/testing/targets/postgres_duckdb_query.md'
      ;;
    entity_query_service)
      printf '%s\n' 'tools/autoresearch/testing/targets/entity_query_service.md'
      ;;
    postgres_repo_query)
      printf '%s\n' 'tools/autoresearch/testing/targets/postgres_repo_query.md'
      ;;
    *)
      printf 'unknown target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_primary_test() {
  case "${1:-}" in
    flusher)
      printf '%s\n' 'internal/cdc/flusher_test.go'
      ;;
    postgres_duckdb_query)
      printf '%s\n' 'internal/postgres_duckdb_federated_integration_test.go'
      ;;
    entity_query_service)
      printf '%s\n' 'internal/entity_manager_test.go'
      ;;
    postgres_repo_query)
      printf '%s\n' 'internal/postgres_persistent_repository_repo_test.go'
      ;;
    *)
      printf 'unknown target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

render_prompt_template() {
  local template_path="$1"
  local target="$2"
  local max_iterations="${3:-}"
  local medium_gate_every="${4:-}"
  local rendered
  local source_file
  local brief_file
  local primary_test_file

  source_file="$(resolve_target_source "$target")"
  brief_file="$(resolve_target_brief "$target")"
  primary_test_file="$(resolve_target_primary_test "$target")"
  rendered="$(<"$template_path")"
  rendered="${rendered//\{\{TARGET\}\}/$target}"
  rendered="${rendered//\{\{SOURCE_FILE\}\}/$source_file}"
  rendered="${rendered//\{\{BRIEF_FILE\}\}/$brief_file}"
  rendered="${rendered//\{\{PRIMARY_TEST_FILE\}\}/$primary_test_file}"
  rendered="${rendered//\{\{MAX_ITERATIONS\}\}/$max_iterations}"
  rendered="${rendered//\{\{MEDIUM_GATE_EVERY\}\}/$medium_gate_every}"
  printf '%s' "$rendered"
}

render_prompt_template_with_decision() {
  local template_path="$1"
  local target="$2"
  local decision_file="$3"
  local rendered
  local source_file
  local brief_file
  local primary_test_file

  source_file="$(resolve_target_source "$target")"
  brief_file="$(resolve_target_brief "$target")"
  primary_test_file="$(resolve_target_primary_test "$target")"
  rendered="$(<"$template_path")"
  rendered="${rendered//\{\{TARGET\}\}/$target}"
  rendered="${rendered//\{\{SOURCE_FILE\}\}/$source_file}"
  rendered="${rendered//\{\{BRIEF_FILE\}\}/$brief_file}"
  rendered="${rendered//\{\{PRIMARY_TEST_FILE\}\}/$primary_test_file}"
  rendered="${rendered//\{\{DECISION_FILE\}\}/$decision_file}"
  printf '%s' "$rendered"
}

coverage_out_path() {
  local bucket="$1"
  local target="$2"
  printf '%s/%s/%s.cover.out\n' "$REPORT_DIR" "$bucket" "$target"
}

coverage_func_path() {
  local bucket="$1"
  local target="$2"
  printf '%s/%s/%s.cover.txt\n' "$REPORT_DIR" "$bucket" "$target"
}

coverage_focus_path() {
  local bucket="$1"
  local target="$2"
  printf '%s/%s/%s.focus.txt\n' "$REPORT_DIR" "$bucket" "$target"
}

coverage_summary_path() {
  local bucket="$1"
  local target="$2"
  printf '%s/%s/%s.summary.txt\n' "$REPORT_DIR" "$bucket" "$target"
}

scenario_notes_path() {
  local bucket="$1"
  local target="$2"
  printf '%s/%s/%s.scenarios.txt\n' "$REPORT_DIR" "$bucket" "$target"
}

test_log_path() {
  local name="$1"
  printf '%s/logs/%s.log\n' "$REPORT_DIR" "$name"
}

run_go_coverage() {
  local target="$1"
  local bucket="$2"

  local pkg
  local focus
  local cover_out
  local func_out
  local focus_out
  local summary_out
  local scenario_out

  pkg="$(resolve_target_pkg "$target")"
  focus="$(resolve_focus_regex "$target")"
  cover_out="$(coverage_out_path "$bucket" "$target")"
  func_out="$(coverage_func_path "$bucket" "$target")"
  focus_out="$(coverage_focus_path "$bucket" "$target")"
  summary_out="$(coverage_summary_path "$bucket" "$target")"
  scenario_out="$(scenario_notes_path "$bucket" "$target")"

  ensure_report_dirs
  rm -f "$cover_out" "$func_out" "$focus_out" "$summary_out" "$scenario_out"

  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go test "$pkg" -coverprofile="$cover_out"
  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go tool cover -func="$cover_out" | tee "$func_out" >/dev/null
  rg "$focus" "$func_out" > "$focus_out" || true
  {
    printf 'target=%s\n' "$target"
    printf 'package=%s\n' "$pkg"
    printf 'coverage_file=%s\n' "$cover_out"
    printf 'coverage_report=%s\n' "$func_out"
    printf 'focus_report=%s\n' "$focus_out"
    printf 'focus_regex=%s\n' "$focus"
    printf 'scenario_notes=%s\n' "$scenario_out"
    printf 'interpretation=Use this as supporting evidence for BDD-style scenario quality, not as the primary goal.\n'
  } > "$summary_out"
  {
    printf 'Use these reports as supporting evidence only.\n'
    printf 'Primary decision question: which meaningful behavior or regression does the candidate protect?\n'
    printf 'Target brief: %s\n' "$(resolve_target_brief "$target")"
    printf 'Primary test file: %s\n' "$(resolve_target_primary_test "$target")"
  } > "$scenario_out"

  printf 'bdd evidence summary: %s\n' "$summary_out"
  printf 'coverage report: %s\n' "$func_out"
  printf 'focus report: %s\n' "$focus_out"
  printf 'scenario notes: %s\n' "$scenario_out"
}
