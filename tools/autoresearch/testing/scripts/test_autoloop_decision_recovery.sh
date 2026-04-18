#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

cp "$SCRIPT_DIR/autoloop.sh" "$tmp_dir/autoloop.sh"
cp "$SCRIPT_DIR/common.sh" "$tmp_dir/common.sh"
cp "$SCRIPT_DIR/extract_decision_from_stdout.py" "$tmp_dir/extract_decision_from_stdout.py"

# shellcheck source=/dev/null
source "$tmp_dir/autoloop.sh"

WORKTREE_DIR="$tmp_dir/worktree"
mkdir -p "$WORKTREE_DIR"
TARGET="postgres_repo_query"
LOG_PREFIX="controller-test"
LOOP_LOG="$tmp_dir/controller.log"
AR_DIR="$tmp_dir/ar"
REPORT_DIR="$AR_DIR/reports"
ensure_report_dirs
DRY_RUN=0

sync_autoresearch_assets() { :; }
cleanup_synced_assets() {
  printf 'cleanup\n' >> "$tmp_dir/cleanup.log"
}
commit_candidate() {
  printf '%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >> "$tmp_dir/commit.log"
}
discard_candidate() {
  printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5" >> "$tmp_dir/discard.log"
}
record_issue_from_decision() { :; }
append_issue() {
  printf '%s|%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >> "$tmp_dir/issues.log"
}
resolve_target_pkg() {
  printf '%s\n' './internal'
}

run_single_candidate() {
  local run_index="$1"
  local decision_file="$2"
  local stdout_log

  stdout_log="$(test_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.stdout")"
  mkdir -p "$(dirname "$stdout_log")"
  rm -f "$stdout_log" "$decision_file"

  case "$TEST_CASE" in
    recover_exit_zero)
      cat > "$stdout_log" <<EOF
AUTORESEARCH_DECISION_BEGIN
status=discard
reason=stdout fallback worked
scenario=n/a
description=recovered with zero exit
evidence=n/a
AUTORESEARCH_DECISION_END
EOF
      return 0
      ;;
    recover_exit_nonzero)
      cat > "$stdout_log" <<EOF
AUTORESEARCH_DECISION_BEGIN
status=keep
reason=stdout fallback survived non-zero exit
scenario=Given stdout fallback only, when controller parses it, then keep still works
description=Uses stdout fallback after a non-zero opencode exit
evidence=n/a
AUTORESEARCH_DECISION_END
EOF
      return 17
      ;;
    missing_markers)
      printf 'plain stdout without markers\n' > "$stdout_log"
      return 0
      ;;
    missing_fields)
      cat > "$stdout_log" <<EOF
AUTORESEARCH_DECISION_BEGIN
status=discard
reason=missing fields
AUTORESEARCH_DECISION_END
EOF
      return 0
      ;;
    missing_end_marker)
      cat > "$stdout_log" <<EOF
AUTORESEARCH_DECISION_BEGIN
status=discard
reason=missing end marker
scenario=n/a
description=incomplete stdout fallback block
evidence=n/a
EOF
      return 0
      ;;
    *)
      printf 'unknown test case: %s\n' "$TEST_CASE" >&2
      return 99
      ;;
  esac
}

assert_contains() {
  local path="$1"
  local pattern="$2"

  rg -F "$pattern" "$path" >/dev/null
}

assert_not_exists() {
  local path="$1"

  [[ ! -e "$path" ]]
}

run_case() {
  local case_name="$1"
  local run_index="$2"

  TEST_CASE="$case_name"
  : > "$tmp_dir/cleanup.log"
  : > "$tmp_dir/commit.log"
  : > "$tmp_dir/discard.log"
  : > "$tmp_dir/issues.log"
  : > "$tmp_dir/controller.log"
  : > "$AR_DIR/results.tsv"
  BLOCKED_STREAK=0

  run_iteration "$run_index" || case_status=$?
  case_status=${case_status:-0}
}

run_case recover_exit_zero 1
[[ "$case_status" -eq 0 ]]
assert_contains "$tmp_dir/discard.log" '1|stdout fallback worked|n/a|recovered with zero exit|n/a'
assert_contains "$tmp_dir/cleanup.log" 'cleanup'
assert_not_exists "$WORKTREE_DIR/.autoresearch-decision-1.txt"

run_case recover_exit_nonzero 2
[[ "$case_status" -eq 0 ]]
assert_contains "$tmp_dir/commit.log" '2|Given stdout fallback only, when controller parses it, then keep still works|Uses stdout fallback after a non-zero opencode exit|n/a'
assert_contains "$tmp_dir/controller.log" 'exited with 17 after producing a usable decision artifact'
assert_contains "$tmp_dir/controller.log" 'using stdout-recovered decision artifact'
assert_contains "$tmp_dir/cleanup.log" 'cleanup'
assert_not_exists "$WORKTREE_DIR/.autoresearch-decision-2.txt"

run_case missing_markers 3
[[ "$case_status" -eq 1 ]]
assert_contains "$tmp_dir/discard.log" '3|no_decision_file|n/a|n/a|no_decision_file'
assert_contains "$tmp_dir/issues.log" 'harness|postgres_repo_query|tools/autoresearch/testing/scripts/autoloop.sh|Autoresearch run produced no usable decision artifact|run 3 produced no decision file; stdout fallback block missing begin marker|Inspect the stdout log and tighten decision-block emission or fallback parsing'
assert_contains "$tmp_dir/cleanup.log" 'cleanup'

run_case missing_fields 4
[[ "$case_status" -eq 1 ]]
assert_contains "$tmp_dir/discard.log" '4|no_decision_file|n/a|n/a|no_decision_file'
assert_contains "$tmp_dir/issues.log" 'harness|postgres_repo_query|tools/autoresearch/testing/scripts/autoloop.sh|Autoresearch run produced no usable decision artifact|run 4 produced no decision file; stdout fallback block missing one or more required decision fields|Inspect the stdout log and tighten decision-block emission or fallback parsing'
assert_contains "$tmp_dir/cleanup.log" 'cleanup'

run_case missing_end_marker 5
[[ "$case_status" -eq 1 ]]
assert_contains "$tmp_dir/discard.log" '5|no_decision_file|n/a|n/a|no_decision_file'
assert_contains "$tmp_dir/issues.log" 'harness|postgres_repo_query|tools/autoresearch/testing/scripts/autoloop.sh|Autoresearch run produced no usable decision artifact|run 5 produced no decision file; stdout fallback block missing end marker|Inspect the stdout log and tighten decision-block emission or fallback parsing'
assert_contains "$tmp_dir/cleanup.log" 'cleanup'

printf 'autoloop decision recovery controller test passed\n'
