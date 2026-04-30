#!/usr/bin/env bash

set -euo pipefail

DEFAULT_BENCHMARK_TARGET="postgres_duckdb_query"

resolve_repo_root_dir() {
  local top_level

  top_level="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [[ -n "$top_level" ]]; then
    printf '%s\n' "$top_level"
    return 0
  fi

  local common_dir

  common_dir="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
  if [[ -n "$common_dir" && "$(basename "$common_dir")" == ".git" ]]; then
    dirname "$common_dir"
    return 0
  fi

  git rev-parse --show-toplevel
}

ROOT_DIR="$(resolve_repo_root_dir)"
AR_DIR="$ROOT_DIR/tools/autoresearch/benchmark/federated"
REPORT_DIR="$AR_DIR/reports"

ensure_report_dirs() {
  mkdir -p "$REPORT_DIR/baseline" "$REPORT_DIR/candidates" "$REPORT_DIR/diff" "$REPORT_DIR/logs" "$REPORT_DIR/runs"
}

resolve_program_rules() {
  printf '%s\n' 'tools/autoresearch/benchmark/federated/program-perf.md'
}

default_benchmark_target() {
  printf '%s\n' "$DEFAULT_BENCHMARK_TARGET"
}

resolve_target_default_gate() {
  case "${1:-$(default_benchmark_target)}" in
    postgres_duckdb_query|federated_query_execution|entity_query_service)
      printf '%s\n' 'fast'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_gate() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  resolve_target_source "$target" >/dev/null

  gate="${2:-}"
  if [[ -z "$gate" ]]; then
    gate="$(resolve_target_default_gate "$target")"
  fi

  case "$gate" in
    fast|medium|heavy)
      printf '%s\n' "$gate"
      ;;
    *)
      printf 'unknown benchmark gate for %s: %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_source() {
  case "${1:-$(default_benchmark_target)}" in
    postgres_duckdb_query)
      printf '%s\n' 'internal/postgres_duckdb_query.go'
      ;;
    federated_query_execution)
      printf '%s\n' 'internal/e2e_harness/federated/query.go'
      ;;
    entity_query_service)
      printf '%s\n' 'internal/entity_query_service.go'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_allowed_files() {
  case "${1:-$(default_benchmark_target)}" in
    postgres_duckdb_query)
      printf '%s\n' 'internal/postgres_duckdb_query.go'
      printf '%s\n' 'internal/duckdb_template_renderer.go'
      printf '%s\n' 'internal/advanced_query_template_duckdb.go'
      ;;
    federated_query_execution)
      printf '%s\n' 'internal/e2e_harness/federated/query.go'
      ;;
    entity_query_service)
      printf '%s\n' 'internal/entity_query_service.go'
      printf '%s\n' 'internal/entity_manager_query.go'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

is_target_allowed_perf_file() {
  local target path allowed

  target="${1:-$(default_benchmark_target)}"
  path="${2:-}"

  while IFS= read -r allowed; do
    if [[ "$path" == "$allowed" ]]; then
      return 0
    fi
  done < <(resolve_target_allowed_files "$target")

  return 1
}

resolve_target_brief() {
  case "${1:-$(default_benchmark_target)}" in
    postgres_duckdb_query)
      printf '%s\n' 'tools/autoresearch/benchmark/federated/targets/postgres_duckdb_query_perf.md'
      ;;
    federated_query_execution)
      printf '%s\n' 'tools/autoresearch/benchmark/federated/targets/federated_query_execution_perf.md'
      ;;
    entity_query_service)
      printf '%s\n' 'tools/autoresearch/benchmark/federated/targets/entity_query_service_perf.md'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_target_workloads() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast)
      printf '%s\n' 'deep-page-1000'
      ;;
    postgres_duckdb_query:medium|postgres_duckdb_query:heavy)
      printf '%s\n' 'deep-page-1000,deep-page-100000'
      ;;
    federated_query_execution:fast)
      printf '%s\n' 'mixed-tier-window'
      ;;
    federated_query_execution:medium|federated_query_execution:heavy)
      printf '%s\n' 'mixed-tier-window,hot-only-window,cold-only-window'
      ;;
    entity_query_service:fast|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' 'baseline-page-1'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_protected_workloads() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium)
      printf '%s\n' 'baseline-page-1,mixed-tier-window'
      ;;
    postgres_duckdb_query:heavy)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,hot-only-window,cold-only-window'
      ;;
    federated_query_execution:fast|federated_query_execution:medium)
      printf '%s\n' 'baseline-page-1'
      ;;
    federated_query_execution:heavy)
      printf '%s\n' 'baseline-page-1,customer-region-page,security-symbol-page'
      ;;
    entity_query_service:fast|entity_query_service:medium)
      printf '%s\n' 'customer-region-page,security-symbol-page'
      ;;
    entity_query_service:heavy)
      printf '%s\n' 'customer-region-page,security-symbol-page,mixed-tier-window'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_workloads() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,deep-page-1000'
      ;;
    postgres_duckdb_query:medium)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,deep-page-1000,deep-page-100000'
      ;;
    postgres_duckdb_query:heavy)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,hot-only-window,cold-only-window,deep-page-1000,deep-page-100000'
      ;;
    federated_query_execution:fast)
      printf '%s\n' 'baseline-page-1,mixed-tier-window'
      ;;
    federated_query_execution:medium)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,hot-only-window,cold-only-window'
      ;;
    federated_query_execution:heavy)
      printf '%s\n' 'baseline-page-1,mixed-tier-window,hot-only-window,cold-only-window,customer-region-page,security-symbol-page'
      ;;
    entity_query_service:fast)
      printf '%s\n' 'baseline-page-1,customer-region-page,security-symbol-page'
      ;;
    entity_query_service:medium)
      printf '%s\n' 'baseline-page-1,customer-region-page,security-symbol-page,mixed-tier-window'
      ;;
    entity_query_service:heavy)
      printf '%s\n' 'baseline-page-1,customer-region-page,security-symbol-page,mixed-tier-window,hot-only-window,cold-only-window'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_distribution() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium|postgres_duckdb_query:heavy|federated_query_execution:fast|federated_query_execution:medium|federated_query_execution:heavy|entity_query_service:fast|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' 'hotspot-overlap'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_mode() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium|postgres_duckdb_query:heavy|federated_query_execution:fast|federated_query_execution:medium|federated_query_execution:heavy|entity_query_service:fast|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' 'live'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_scale() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium|federated_query_execution:fast|federated_query_execution:medium|entity_query_service:fast|entity_query_service:medium)
      printf '%s\n' 'small'
      ;;
    postgres_duckdb_query:heavy|federated_query_execution:heavy|entity_query_service:heavy)
      printf '%s\n' 'medium'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_tier_profile() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium|postgres_duckdb_query:heavy|federated_query_execution:fast|federated_query_execution:medium|federated_query_execution:heavy|entity_query_service:fast|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' 'balanced'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_iterations() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|federated_query_execution:fast|entity_query_service:fast)
      printf '%s\n' '1'
      ;;
    postgres_duckdb_query:medium|postgres_duckdb_query:heavy|federated_query_execution:medium|federated_query_execution:heavy|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' '2'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_seed() {
  local target gate

  target="${1:-$(default_benchmark_target)}"
  gate="$(resolve_target_gate "$target" "${2:-}")"

  case "$target:$gate" in
    postgres_duckdb_query:fast|postgres_duckdb_query:medium|postgres_duckdb_query:heavy|federated_query_execution:fast|federated_query_execution:medium|federated_query_execution:heavy|entity_query_service:fast|entity_query_service:medium|entity_query_service:heavy)
      printf '%s\n' '42'
      ;;
    *)
      printf 'unknown benchmark target/gate: %s %s\n' "$target" "$gate" >&2
      exit 1
      ;;
  esac
}

resolve_target_report_dir() {
  local phase="$1"
  local target="$2"
  local gate

  gate="$(resolve_target_gate "$target" "${3:-}")"
  printf '%s\n' "$REPORT_DIR/$phase/$target/$gate"
}

benchmark_log_path() {
  local name="$1"
  printf '%s\n' "$REPORT_DIR/logs/${name}.log"
}

benchmark_cli() {
  GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' go run ./cmd/benchmark "$@"
}

render_prompt_template_with_decision() {
  local template_path="$1"
  local target="$2"
  local gate="$3"
  local decision_file="$4"
  local worktree_dir="${5:-$ROOT_DIR}"
  local rendered
  local source_file
  local brief_file
  local rules_file
  local candidate_script
  local gate_script
  local target_workloads
  local protected_workloads

  gate="$(resolve_target_gate "$target" "$gate")"
  source_file="$worktree_dir/$(resolve_target_source "$target")"
  brief_file="$worktree_dir/$(resolve_target_brief "$target")"
  rules_file="$worktree_dir/$(resolve_program_rules)"
  candidate_script="$worktree_dir/tools/autoresearch/benchmark/federated/scripts/benchmark_candidate.sh"
  gate_script="$worktree_dir/tools/autoresearch/benchmark/federated/scripts/benchmark_gate.sh"
  target_workloads="$(resolve_target_target_workloads "$target" "$gate")"
  protected_workloads="$(resolve_target_protected_workloads "$target" "$gate")"
  rendered="$(<"$template_path")"
  rendered="${rendered//\{\{TARGET\}\}/$target}"
  rendered="${rendered//\{\{GATE\}\}/$gate}"
  rendered="${rendered//\{\{SOURCE_FILE\}\}/$source_file}"
  rendered="${rendered//\{\{BRIEF_FILE\}\}/$brief_file}"
  rendered="${rendered//\{\{RULES_FILE\}\}/$rules_file}"
  rendered="${rendered//\{\{DECISION_FILE\}\}/$decision_file}"
  rendered="${rendered//\{\{CANDIDATE_SCRIPT\}\}/$candidate_script}"
  rendered="${rendered//\{\{GATE_SCRIPT\}\}/$gate_script}"
  rendered="${rendered//\{\{WORKTREE_DIR\}\}/$worktree_dir}"
  rendered="${rendered//\{\{TARGET_WORKLOADS\}\}/$target_workloads}"
  rendered="${rendered//\{\{PROTECTED_WORKLOADS\}\}/$protected_workloads}"
  printf '%s' "$rendered"
}

configure_external_federated_env() {
  local pg_host pg_user pg_password pg_db pg_port pg_sslmode s3_endpoint s3_bucket s3_prefix s3_region s3_access_key s3_secret_key

  pg_host="${FEDERATED_BENCHMARK_PG_HOST:-}"
  pg_user="${FEDERATED_BENCHMARK_PG_USER:-postgres}"
  pg_password="${FEDERATED_BENCHMARK_PG_PASSWORD:-postgres}"
  pg_db="${FEDERATED_BENCHMARK_PG_DB:-postgres}"
  pg_port="${FEDERATED_BENCHMARK_PG_PORT:-5432}"
  pg_sslmode="${FEDERATED_BENCHMARK_PG_SSLMODE:-disable}"
  s3_endpoint="${FEDERATED_BENCHMARK_S3_ENDPOINT:-}"
  s3_bucket="${FEDERATED_BENCHMARK_S3_BUCKET:-test-bucket}"
  s3_prefix="${FEDERATED_BENCHMARK_S3_PREFIX:-test-project}"
  s3_region="${FEDERATED_BENCHMARK_S3_REGION:-us-east-1}"
  s3_access_key="${FEDERATED_BENCHMARK_S3_ACCESS_KEY:-}"
  s3_secret_key="${FEDERATED_BENCHMARK_S3_SECRET_KEY:-}"

  if [[ -z "$pg_host" && -z "$s3_endpoint" ]]; then
    return 0
  fi

  if [[ -z "$s3_endpoint" || -z "$s3_access_key" || -z "$s3_secret_key" ]]; then
    printf 'external federated benchmark mode requires S3 endpoint/access key/secret key env vars\n' >&2
    printf 'expected: FEDERATED_BENCHMARK_S3_ENDPOINT, FEDERATED_BENCHMARK_S3_ACCESS_KEY, FEDERATED_BENCHMARK_S3_SECRET_KEY\n' >&2
    exit 1
  fi

  export FEDERATED_E2E_EXTERNAL_S3_ENDPOINT="$s3_endpoint"
  export FEDERATED_E2E_EXTERNAL_S3_BUCKET="$s3_bucket"
  export FEDERATED_E2E_EXTERNAL_S3_PREFIX="$s3_prefix"
  export FEDERATED_E2E_EXTERNAL_S3_REGION="$s3_region"
  export FEDERATED_E2E_EXTERNAL_S3_ACCESS_KEY="$s3_access_key"
  export FEDERATED_E2E_EXTERNAL_S3_SECRET_KEY="$s3_secret_key"

  if [[ -n "$pg_host" ]]; then
    export FEDERATED_E2E_EXTERNAL_PG_DSN="postgres://${pg_user}:${pg_password}@${pg_host}:${pg_port}/${pg_db}?sslmode=${pg_sslmode}"
    export FEDERATED_E2E_EXTERNAL_PG_USER="$pg_user"
    export FEDERATED_E2E_EXTERNAL_PG_PASSWORD="$pg_password"
    export FEDERATED_E2E_EXTERNAL_PG_DB="$pg_db"
    export FEDERATED_E2E_EXTERNAL_PG_SSLMODE="$pg_sslmode"
  else
    unset FEDERATED_E2E_EXTERNAL_PG_DSN
    unset FEDERATED_E2E_EXTERNAL_PG_USER
    unset FEDERATED_E2E_EXTERNAL_PG_PASSWORD
    unset FEDERATED_E2E_EXTERNAL_PG_DB
    unset FEDERATED_E2E_EXTERNAL_PG_SSLMODE
  fi
}

run_or_print() {
  local dry_run="$1"
  shift
  if [[ "$dry_run" -eq 1 ]]; then
    printf 'dry-run command:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

sanitize_tsv_field() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}

print_target_context() {
  local target="$1"
  local gate

  gate="$(resolve_target_gate "$target" "${2:-}")"
  printf 'target: %s\n' "$target"
  printf 'gate: %s\n' "$gate"
  printf 'source: %s\n' "$(resolve_target_source "$target")"
  printf 'brief: %s\n' "$(resolve_target_brief "$target")"
  printf 'mode: %s\n' "$(resolve_target_mode "$target" "$gate")"
  printf 'scale: %s\n' "$(resolve_target_scale "$target" "$gate")"
  printf 'distribution override: %s\n' "$(resolve_target_distribution "$target" "$gate")"
  printf 'tier profile: %s\n' "$(resolve_target_tier_profile "$target" "$gate")"
  printf 'iterations: %s\n' "$(resolve_target_iterations "$target" "$gate")"
  printf 'seed: %s\n' "$(resolve_target_seed "$target" "$gate")"
  printf 'target workloads: %s\n' "$(resolve_target_target_workloads "$target" "$gate")"
  printf 'protected workloads: %s\n' "$(resolve_target_protected_workloads "$target" "$gate")"
  printf 'workloads: %s\n' "$(resolve_target_workloads "$target" "$gate")"
  if [[ -n "${FEDERATED_E2E_EXTERNAL_S3_ENDPOINT:-}" ]]; then
    if [[ -n "${FEDERATED_E2E_EXTERNAL_PG_DSN:-}" ]]; then
    printf 'external_pg_dsn: %s\n' "$FEDERATED_E2E_EXTERNAL_PG_DSN"
    fi
    printf 'external_s3_endpoint: %s\n' "${FEDERATED_E2E_EXTERNAL_S3_ENDPOINT:-}"
    printf 'external_s3_bucket: %s\n' "${FEDERATED_E2E_EXTERNAL_S3_BUCKET:-}"
    printf 'external_s3_prefix: %s\n' "${FEDERATED_E2E_EXTERNAL_S3_PREFIX:-}"
  fi
}
