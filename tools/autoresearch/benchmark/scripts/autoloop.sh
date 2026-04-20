#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./tools/autoresearch/benchmark/scripts/autoloop.sh [options]

Options:
  -m, --model MODEL            OpenCode model in provider/model form
      --agent AGENT            Optional OpenCode agent name
      --preset NAME            Benchmark preset (default: small-live)
      --goal TEXT              Optimization goal text
      --goal-file PATH         File containing optimization goal text
      --protected-workloads CSV
                               Comma-separated protected workloads
      --iterations N           Number of iterations (default: 5)
      --baseline               Capture a fresh baseline before the loop starts
      --skip-local-infra       Do not auto-start Postgres and RustFS
      --attach URL             Attach to an existing OpenCode server
      --session ID             Continue a specific OpenCode session across runs
  -c, --continue               Continue the last OpenCode session across runs
      --fork                   Fork when continuing a session
      --dangerously-skip-permissions
                               Pass through to OpenCode run
      --sleep-seconds N        Sleep between runs (default: 5)
      --dry-run                Show commands without executing
      --force                  Skip branch and cleanliness checks
  -h, --help                   Show this help
EOF
}

MODEL="github-copilot/gpt-5-mini"
AGENT=""
PRESET="small-live"
GOAL_TEXT=""
GOAL_FILE=""
PROTECTED_WORKLOADS="$(default_protected_workloads_csv)"
ITERATIONS=5
RUN_BASELINE=0
SKIP_LOCAL_INFRA=0
ATTACH_URL=""
SESSION_ID=""
CONTINUE_LAST=0
FORK_SESSION=0
SKIP_PERMISSIONS=0
SLEEP_SECONDS=5
DRY_RUN=0
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--model)
      MODEL="${2:-}"
      shift 2
      ;;
    --agent)
      AGENT="${2:-}"
      shift 2
      ;;
    --preset)
      PRESET="${2:-}"
      shift 2
      ;;
    --goal)
      GOAL_TEXT="${2:-}"
      shift 2
      ;;
    --goal-file)
      GOAL_FILE="${2:-}"
      shift 2
      ;;
    --protected-workloads)
      PROTECTED_WORKLOADS="${2:-}"
      shift 2
      ;;
    --iterations)
      ITERATIONS="${2:-}"
      shift 2
      ;;
    --baseline)
      RUN_BASELINE=1
      shift
      ;;
    --skip-local-infra)
      SKIP_LOCAL_INFRA=1
      shift
      ;;
    --attach)
      ATTACH_URL="${2:-}"
      shift 2
      ;;
    --session)
      SESSION_ID="${2:-}"
      shift 2
      ;;
    -c|--continue)
      CONTINUE_LAST=1
      shift
      ;;
    --fork)
      FORK_SESSION=1
      shift
      ;;
    --dangerously-skip-permissions)
      SKIP_PERMISSIONS=1
      shift
      ;;
    --sleep-seconds)
      SLEEP_SECONDS="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "$GOAL_FILE" ]]; then
  GOAL_TEXT="$(<"$GOAL_FILE")"
fi

if [[ -z "${GOAL_TEXT// }" ]]; then
  printf 'an optimization goal is required via --goal or --goal-file\n' >&2
  exit 1
fi

if [[ -n "$SESSION_ID" && "$CONTINUE_LAST" -eq 1 ]]; then
  printf 'use either --continue or --session, not both\n' >&2
  exit 1
fi

for value_name in ITERATIONS SLEEP_SECONDS; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt 0 ]]; then
    printf '%s must be a non-negative integer\n' "$value_name" >&2
    exit 1
  fi
done

PRESET_SAFE="$(sanitize_name "$PRESET")"
BRANCH_DATE="$(date '+%Y%m%d')"
RESEARCH_BRANCH="autoresearch/benchmark-${PRESET_SAFE}-${BRANCH_DATE}"
WORKTREE_DIR="$ROOT_DIR/.worktrees/autoresearch-benchmark-${PRESET_SAFE}"
LOOP_LOG="$(log_path "benchmark-${PRESET_SAFE}")"
BASELINE_RUN_NAME="${PRESET_SAFE}-baseline"
BASELINE_DIR="$REPORT_DIR/baseline/$BASELINE_RUN_NAME"
BASELINE_SUMMARY="$BASELINE_DIR"/*/benchmark-summary.json

append_loop_log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" | tee -a "$LOOP_LOG"
}

is_registered_worktree() {
  git -C "$ROOT_DIR" worktree list --porcelain | rg -Fxq "worktree $WORKTREE_DIR"
}

ensure_main_clean() {
  local status
  status="$(git -C "$ROOT_DIR" status --porcelain)"
  if [[ -n "$status" && "$FORCE" -ne 1 ]]; then
    printf 'main branch has uncommitted changes; use --force to override\n' >&2
    git -C "$ROOT_DIR" status --short >&2
    return 1
  fi
}

init_worktree() {
  if [[ -d "$WORKTREE_DIR" ]]; then
    if ! is_registered_worktree; then
      git -C "$ROOT_DIR" worktree prune >/dev/null 2>&1 || true
    fi
    if ! is_registered_worktree; then
      printf 'worktree directory %s exists but is not registered\n' "$WORKTREE_DIR" >&2
      return 1
    fi
    return 0
  fi
  mkdir -p "$(dirname "$WORKTREE_DIR")"
  if ! git -C "$ROOT_DIR" rev-parse --verify "$RESEARCH_BRANCH" >/dev/null 2>&1; then
    git -C "$ROOT_DIR" branch "$RESEARCH_BRANCH" origin/main
  fi
  git -C "$ROOT_DIR" worktree add "$WORKTREE_DIR" "$RESEARCH_BRANCH"
}

ensure_worktree_clean() {
  local status
  status="$(git -C "$WORKTREE_DIR" status --porcelain)"
  if [[ -n "$status" ]]; then
    printf 'worktree %s is not clean\n' "$WORKTREE_DIR" >&2
    git -C "$WORKTREE_DIR" status --short >&2
    return 1
  fi
}

build_opencode_command() {
  local -n out_ref=$1
  local prompt
  local prompt_cmd=("$SCRIPT_DIR/opencode_autoresearch.sh" --model "$MODEL" --preset "$PRESET" --goal "$GOAL_TEXT" --protected-workloads "$PROTECTED_WORKLOADS" --print-prompt)
  if [[ -n "$AGENT" ]]; then
    prompt_cmd+=(--agent "$AGENT")
  fi
  prompt="$(WORKTREE_DIR="$WORKTREE_DIR" "${prompt_cmd[@]}" 2>/dev/null)"

  out_ref=(opencode run)
  if [[ -n "$MODEL" ]]; then
    out_ref+=(--model "$MODEL")
  fi
  if [[ -n "$AGENT" ]]; then
    out_ref+=(--agent "$AGENT")
  fi
  if [[ -n "$ATTACH_URL" ]]; then
    out_ref+=(--attach "$ATTACH_URL")
  fi
  if [[ "$SKIP_PERMISSIONS" -eq 1 ]]; then
    out_ref+=(--dangerously-skip-permissions)
  fi
  if [[ -n "$SESSION_ID" ]]; then
    out_ref+=(--session "$SESSION_ID")
  elif [[ "$CONTINUE_LAST" -eq 1 ]]; then
    out_ref+=(--continue)
  fi
  if [[ "$FORK_SESSION" -eq 1 ]]; then
    out_ref+=(--fork)
  fi
  out_ref+=(--title "autoresearch-benchmark:${PRESET}" --dir "$WORKTREE_DIR" -- "$prompt")
}

commit_candidate() {
  local run_index="$1"
  git -C "$WORKTREE_DIR" add -A
  local changes
  changes="$(git -C "$WORKTREE_DIR" diff --cached --name-only)"
  if [[ -z "$changes" ]]; then
    append_loop_log "iteration $run_index produced no staged changes"
    return 0
  fi
  git -C "$WORKTREE_DIR" commit -m "perf(autoresearch): ${PRESET} run ${run_index}" -m "$GOAL_TEXT"
}

discard_candidate() {
  git -C "$WORKTREE_DIR" restore --worktree .
  git -C "$WORKTREE_DIR" clean -fd
}

capture_baseline() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'baseline command: (cd %q && ./tools/autoresearch/benchmark/scripts/baseline.sh %q %q)\n' "$WORKTREE_DIR" "$PRESET" "$BASELINE_RUN_NAME"
    return 0
  fi
  (
    cd "$WORKTREE_DIR"
    ./tools/autoresearch/benchmark/scripts/baseline.sh "$PRESET" "$BASELINE_RUN_NAME"
  )
}

run_iteration() {
  local run_index="$1"
  local run_name="${PRESET_SAFE}-run-${run_index}"
  local decision_file="$REPORT_DIR/decisions/${run_name}.json"
  local stdout_log
  local cmd=()
  stdout_log="$(log_path "benchmark-${PRESET_SAFE}-run-${run_index}")"

  build_opencode_command cmd

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'run %d command:' "$run_index"
    printf ' %q' "${cmd[@]}"
    printf '\n'
    printf 'candidate gate: (cd %q && ./tools/autoresearch/benchmark/scripts/run_candidate.sh %q %q %q)\n' "$WORKTREE_DIR" "$PRESET" "$BASELINE_SUMMARY_PATH" "$run_name"
    printf 'decision evaluator: python3 %q --baseline-summary %q --candidate-summary <candidate> --diff <diff> --decision-out %q --protected-workloads %q\n' "$SCRIPT_DIR/evaluate_benchmark_run.py" "$BASELINE_SUMMARY_PATH" "$decision_file" "$PROTECTED_WORKLOADS"
    append_loop_log "iteration $run_index dry-run complete"
    return 0
  fi

  rm -f "$stdout_log"
  {
    printf '=== benchmark iteration %d start ===\n' "$run_index"
    printf 'command:'
    printf ' %q' "${cmd[@]}"
    printf '\n'
    "${cmd[@]}"
  } 2>&1 | tee "$stdout_log"

  local candidate_output candidate_summary diff_path
  candidate_output="$(cd "$WORKTREE_DIR" && ./tools/autoresearch/benchmark/scripts/run_candidate.sh "$PRESET" "$BASELINE_SUMMARY_PATH" "$run_name")"
  candidate_summary="$(printf '%s\n' "$candidate_output" | rg '^candidate_summary=' | sed 's/^candidate_summary=//')"
  diff_path="$(printf '%s\n' "$candidate_output" | rg '^diff_path=' | sed 's/^diff_path=//')"

  python3 "$SCRIPT_DIR/evaluate_benchmark_run.py" \
    --baseline-summary "$BASELINE_SUMMARY_PATH" \
    --candidate-summary "$candidate_summary" \
    --diff "$diff_path" \
    --decision-out "$decision_file" \
    --protected-workloads "$PROTECTED_WORKLOADS"

  local status
  status="$(python3 - <<'PY' "$decision_file"
import json, sys
print(json.loads(open(sys.argv[1], encoding='utf-8').read())['status'])
PY
)"
  if [[ "$status" == "keep" ]]; then
    commit_candidate "$run_index"
    append_loop_log "iteration $run_index kept"
  else
    discard_candidate
    append_loop_log "iteration $run_index discarded"
  fi
}

autoloop_main() {
  cd "$ROOT_DIR"
  ensure_report_dirs
  append_loop_log "preset=$PRESET research_branch=$RESEARCH_BRANCH worktree=$WORKTREE_DIR model=${MODEL:-default}"
  if [[ "$DRY_RUN" -ne 1 ]]; then
    ensure_main_clean
    if [[ "$SKIP_LOCAL_INFRA" -ne 1 ]]; then
      ensure_local_infra
    fi
    init_worktree
    ensure_worktree_clean
  fi

  BASELINE_SUMMARY_PATH=""
  if [[ "$RUN_BASELINE" -eq 1 ]]; then
    append_loop_log "capturing baseline"
    capture_baseline
  else
    if ! BASELINE_SUMMARY_PATH="$(first_summary_under "$BASELINE_DIR" 2>/dev/null)"; then
      append_loop_log "capturing baseline"
      capture_baseline
      BASELINE_SUMMARY_PATH=""
    fi
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    if [[ -z "$BASELINE_SUMMARY_PATH" ]]; then
      BASELINE_SUMMARY_PATH="$BASELINE_DIR/<preset-dir>/benchmark-summary.json"
    fi
  else
    BASELINE_SUMMARY_PATH="$(first_summary_under "$BASELINE_DIR")"
  fi

  local run_index
  for ((run_index = 1; run_index <= ITERATIONS; run_index++)); do
    if [[ "$DRY_RUN" -ne 1 ]]; then
      ensure_worktree_clean
    fi
    append_loop_log "=== iteration $run_index of $ITERATIONS ==="
    run_iteration "$run_index"
    if [[ "$run_index" -lt "$ITERATIONS" && "$SLEEP_SECONDS" -gt 0 ]]; then
      if [[ "$DRY_RUN" -eq 1 ]]; then
        printf 'sleep %s\n' "$SLEEP_SECONDS"
      else
        sleep "$SLEEP_SECONDS"
      fi
    fi
  done
  append_loop_log "benchmark autoloop finished: preset=$PRESET iterations=$ITERATIONS"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  autoloop_main "$@"
fi
