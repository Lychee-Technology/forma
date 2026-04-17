#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./tools/autoresearch/testing/scripts/autoloop.sh [options]

Options:
  -m, --model MODEL            OpenCode model in provider/model form
  -t, --target TARGET          Target key: flusher | postgres_duckdb_query | entity_query_service | postgres_repo_query
      --agent AGENT            Optional OpenCode agent name
      --variant NAME           Optional OpenCode model variant
      --iterations N           Number of outer-loop runs (default: 20)
      --baseline               Run baseline before the loop starts
      --skip-local-infra       Do not auto-start Postgres and RustFS via deploy/docker-compose.yml
      --attach URL             Attach each run to an existing OpenCode server
      --session ID             Continue a specific OpenCode session across all runs
  -c, --continue               Continue the last OpenCode session across all runs
      --fork                   Fork when continuing a session
      --dangerously-skip-permissions
                               Pass through to OpenCode run
      --sleep-seconds N        Sleep between runs (default: 5)
      --log-prefix NAME        Prefix for loop log files (default: autoloop)
      --print-prompt           Print the generated prompt for the current target and exit
      --dry-run                Print the commands without executing them
      --force                  Skip branch/safety checks
  -h, --help                   Show this help

Examples:
  ./tools/autoresearch/testing/scripts/autoloop.sh --model openai/gpt-5 --target flusher --baseline
  ./tools/autoresearch/testing/scripts/autoloop.sh --model anthropic/claude-sonnet-4-5 --target flusher --iterations 24
EOF
}

TARGET="flusher"
MODEL=""
AGENT=""
VARIANT=""
ITERATIONS=20
RUN_BASELINE=0
SKIP_LOCAL_INFRA=0
ATTACH_URL=""
SESSION_ID=""
CONTINUE_LAST=0
FORK_SESSION=0
SKIP_PERMISSIONS=0
SLEEP_SECONDS=5
LOG_PREFIX="autoloop"
PRINT_PROMPT=0
DRY_RUN=0
FORCE=0
CONSECUTIVE_BLOCKED_LIMIT=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--model)
      MODEL="${2:-}"
      shift 2
      ;;
    -t|--target)
      TARGET="${2:-}"
      shift 2
      ;;
    --agent)
      AGENT="${2:-}"
      shift 2
      ;;
    --variant)
      VARIANT="${2:-}"
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
    --log-prefix)
      LOG_PREFIX="${2:-}"
      shift 2
      ;;
    --print-prompt)
      PRINT_PROMPT=1
      shift
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
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

resolve_target_pkg "$TARGET" >/dev/null

for value_name in ITERATIONS SLEEP_SECONDS; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt 0 ]]; then
    printf '%s must be a non-negative integer\n' "$value_name" >&2
    exit 1
  fi
done

if [[ "$ITERATIONS" -lt 1 ]]; then
  printf '--iterations must be at least 1\n' >&2
  exit 1
fi

BLOCKED_STREAK=0

if [[ -n "$SESSION_ID" && "$CONTINUE_LAST" -eq 1 ]]; then
  printf 'use either --continue or --session, not both\n' >&2
  exit 1
fi

if ! command -v opencode >/dev/null 2>&1; then
  printf 'opencode not found in PATH\n' >&2
  exit 1
fi

WORKTREE_DIR="$ROOT_DIR/.worktrees/autoresearch-$TARGET"
BRANCH_DATE="$(date '+%Y%m%d')"
RESEARCH_BRANCH="autoresearch/${TARGET}-${BRANCH_DATE}"
MAIN_BRANCH="main"

is_registered_worktree() {
  git -C "$ROOT_DIR" worktree list --porcelain | rg -Fxq "worktree $WORKTREE_DIR"
}

is_on_research_branch() {
  local current_branch
  current_branch="$(git symbolic-ref --short HEAD 2>/dev/null || git rev-parse --short HEAD 2>/dev/null)"
  [[ "$current_branch" == "$RESEARCH_BRANCH" ]]
}

init_worktree() {
  if [[ -d "$WORKTREE_DIR" ]]; then
    if ! is_registered_worktree; then
      git -C "$ROOT_DIR" worktree prune >/dev/null 2>&1 || true
    fi
    if ! is_registered_worktree; then
      printf 'worktree directory %s exists but is not registered as a git worktree.\n' "$WORKTREE_DIR" >&2
      printf 'Remove the directory or run `git worktree prune` and retry.\n' >&2
      return 1
    fi
    printf 'using existing worktree: %s\n' "$WORKTREE_DIR"
    return 0
  fi

  printf 'creating new worktree: %s\n' "$WORKTREE_DIR"
  mkdir -p "$(dirname "$WORKTREE_DIR")"

  if git -C "$ROOT_DIR" rev-parse --verify "$RESEARCH_BRANCH" >/dev/null 2>&1; then
    printf 'branch %s already exists, using it.\n' "$RESEARCH_BRANCH"
  else
    printf 'creating new branch: %s\n' "$RESEARCH_BRANCH"
    git -C "$ROOT_DIR" branch "$RESEARCH_BRANCH"
  fi

  git -C "$ROOT_DIR" worktree add "$WORKTREE_DIR" "$RESEARCH_BRANCH"
}

ensure_worktree_clean() {
  local status
  status="$(git -C "$WORKTREE_DIR" status --porcelain)"
  if [[ -n "$status" ]]; then
    printf 'worktree %s is not clean. Commit or discard changes first.\n' "$WORKTREE_DIR" >&2
    git -C "$WORKTREE_DIR" status --short >&2
    return 1
  fi
}

ensure_worktree_on_research() {
  local current_branch
  current_branch="$(git -C "$WORKTREE_DIR" symbolic-ref --short HEAD 2>/dev/null || git -C "$WORKTREE_DIR" rev-parse --short HEAD 2>/dev/null)"
  if [[ "$current_branch" != "$RESEARCH_BRANCH" ]]; then
    if [[ "$FORCE" -eq 1 ]]; then
      printf 'worktree is on %s, but --force is set. Switching to %s.\n' "$current_branch" "$RESEARCH_BRANCH"
      git -C "$WORKTREE_DIR" checkout "$RESEARCH_BRANCH"
    else
      printf 'ERROR: worktree must be on research branch %s.\n' "$RESEARCH_BRANCH" >&2
      printf 'Currently on: %s\n' "$current_branch" >&2
      printf 'Use --force to override.\n' >&2
      exit 1
    fi
  fi
}

ensure_main_clean() {
  local status
  status="$(git -C "$ROOT_DIR" status --porcelain)"
  if [[ -n "$status" ]]; then
    printf 'WARNING: main branch has uncommitted changes. This may interfere with worktree operations.\n' >&2
    git -C "$ROOT_DIR" status --short >&2
    if [[ "$FORCE" -ne 1 ]]; then
      printf 'Commit or discard main changes before running autoloop, or use --force.\n' >&2
      return 1
    fi
  fi
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

  if [[ "$SKIP_LOCAL_INFRA" -eq 1 ]]; then
    append_loop_log "skipping local infra startup by request"
    return 0
  fi

  if [[ ! -f "$compose_file" ]]; then
    printf 'compose file not found: %s\n' "$compose_file" >&2
    return 1
  fi

  if ! compose_bin="$(compose_cmd)"; then
    printf 'docker compose is required but not available\n' >&2
    return 1
  fi

  append_loop_log "starting local infra from deploy/docker-compose.yml"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'local infra command: %s -f %q up -d aurora-dsql rustfs\n' "$compose_bin" "$compose_file"
    printf 'local infra health checks: postgres=pg_isready, rustfs=http://localhost:19000 or :19001\n'
    return 0
  fi

  if [[ "$compose_bin" == "docker compose" ]]; then
    docker compose -f "$compose_file" up -d aurora-dsql rustfs
  else
    docker-compose -f "$compose_file" up -d aurora-dsql rustfs
  fi

  if ! wait_for_pg; then
    printf 'postgres did not become healthy in time\n' >&2
    return 1
  fi
  if ! wait_for_rustfs; then
    printf 'rustfs did not become reachable in time\n' >&2
    return 1
  fi

  append_loop_log "local infra is ready: postgres + rustfs"
}

print_prompt() {
  "$SCRIPT_DIR/opencode_autoresearch.sh" \
    --target "$TARGET" \
    --single-candidate \
    --print-prompt
}

build_opencode_command() {
  local run_index="$1"
  local decision_file="$2"
  local -n out_ref=$3

  out_ref=(opencode run)

  if [[ -n "$MODEL" ]]; then
    out_ref+=(--model "$MODEL")
  fi
  if [[ -n "$AGENT" ]]; then
    out_ref+=(--agent "$AGENT")
  fi
  if [[ -n "$VARIANT" ]]; then
    out_ref+=(--variant "$VARIANT")
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

  local prompt
  prompt="$("$SCRIPT_DIR/opencode_autoresearch.sh" \
    --target "$TARGET" \
    --single-candidate \
    --decision-file "$decision_file" \
    --print-prompt 2>/dev/null)"

  out_ref+=(
    --title "autoresearch:${TARGET}:${run_index}"
    --dir "$WORKTREE_DIR"
    --
    "$prompt"
  )
}

run_single_candidate() {
  local run_index="$1"
  local decision_file="$2"
  local cmd=()
  local stdout_log
  build_opencode_command "$run_index" "$decision_file" cmd
  stdout_log="$(test_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.stdout")"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'run %d command:' "$run_index"
    printf ' %q' "${cmd[@]}"
    printf '\n'
    return 0
  fi

  rm -f "$stdout_log"

  {
    printf '=== run %d start ===\n' "$run_index"
    printf 'command:'
    printf ' %q' "${cmd[@]}"
    printf '\n'
    "${cmd[@]}"
  } 2>&1 | tee "$stdout_log"
  return $?
}

read_decision() {
  local decision_file="$1"
  if [[ ! -f "$decision_file" ]]; then
    printf 'decision file not found: %s\n' "$decision_file" >&2
    printf 'status=error\nreason=missing_decision_file\n' >&2
    return 1
  fi
  cat "$decision_file"
}

get_decision_field() {
  local decision_file="$1"
  local key="$2"
  local line

  line="$(rg "^${key}=" "$decision_file" | head -1 || true)"
  if [[ -z "$line" ]]; then
    return 0
  fi

  printf '%s' "${line#*=}" | tr -d '\r'
}

recover_decision_from_stdout() {
  local run_index="$1"
  local decision_file="$2"
  local stdout_log

  stdout_log="$(test_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.stdout")"
  [[ -f "$stdout_log" ]] || return 1

  python3 "$SCRIPT_DIR/extract_decision_from_stdout.py" "$stdout_log" "$decision_file"
}

sanitize_tsv_field() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}

ensure_issue_file() {
  if [[ ! -f "$AR_DIR/issues.tsv" ]]; then
    printf 'id\tcategory\ttarget\tfile\ttitle\tevidence\tsuggested_fix\tstatus\trun_date\n' > "$AR_DIR/issues.tsv"
  fi
}

issue_exists() {
  local category="$1"
  local target="$2"
  local file="$3"
  local title="$4"
  local id existing_category existing_target existing_file existing_title rest

  if [[ ! -f "$AR_DIR/issues.tsv" ]]; then
    return 1
  fi

  while IFS=$'\t' read -r id existing_category existing_target existing_file existing_title rest; do
    [[ "$id" == "id" ]] && continue
    if [[ "$existing_category" == "$category" && "$existing_target" == "$target" && "$existing_file" == "$file" && "$existing_title" == "$title" ]]; then
      return 0
    fi
  done < "$AR_DIR/issues.tsv"

  return 1
}

next_issue_id() {
  local max_id=0
  local id rest

  ensure_issue_file
  while IFS=$'\t' read -r id rest; do
    [[ "$id" == "id" ]] && continue
    if [[ "$id" =~ ^AR-([0-9]+)$ ]]; then
      if (( 10#${BASH_REMATCH[1]} > max_id )); then
        max_id=$((10#${BASH_REMATCH[1]}))
      fi
    fi
  done < "$AR_DIR/issues.tsv"

  printf 'AR-%03d\n' "$((max_id + 1))"
}

append_issue() {
  local category="$1"
  local target="$2"
  local file="$3"
  local title="$4"
  local evidence="$5"
  local suggested_fix="$6"
  local status="${7:-open}"
  local run_date="${8:-$(date '+%Y-%m-%d')}"
  local issue_id

  [[ -z "$category" || -z "$title" ]] && return 0

  category="$(sanitize_tsv_field "$category")"
  target="$(sanitize_tsv_field "$target")"
  file="$(sanitize_tsv_field "${file:-n/a}")"
  title="$(sanitize_tsv_field "$title")"
  evidence="$(sanitize_tsv_field "${evidence:-n/a}")"
  suggested_fix="$(sanitize_tsv_field "${suggested_fix:-n/a}")"
  status="$(sanitize_tsv_field "$status")"
  run_date="$(sanitize_tsv_field "$run_date")"

  ensure_issue_file
  if issue_exists "$category" "$target" "$file" "$title"; then
    return 0
  fi

  issue_id="$(next_issue_id)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$issue_id" "$category" "$target" "$file" "$title" "$evidence" "$suggested_fix" "$status" "$run_date" >> "$AR_DIR/issues.tsv"
}

record_issue_from_decision() {
  local decision_file="$1"
  local category file title evidence suggested_fix

  category="$(get_decision_field "$decision_file" issue_category)"
  file="$(get_decision_field "$decision_file" issue_file)"
  title="$(get_decision_field "$decision_file" issue_title)"
  evidence="$(get_decision_field "$decision_file" issue_evidence)"
  suggested_fix="$(get_decision_field "$decision_file" issue_suggested_fix)"

  append_issue "$category" "$TARGET" "$file" "$title" "$evidence" "$suggested_fix"
}

commit_candidate() {
  local run_index="$1"
  local scenario="$2"
  local description="$3"
  local evidence="$4"
  local status_line
  local path
  local -a test_files=()

  local commit_msg="test(autoresearch): ${TARGET} - ${scenario}"
  if [[ -n "$description" ]]; then
    commit_msg+=" | ${description}"
  fi

  while IFS= read -r status_line; do
    path="${status_line:3}"
    case "$path" in
      internal/*_test.go|tests/e2e/*)
        test_files+=("$path")
        ;;
    esac
  done < <(git -C "$WORKTREE_DIR" status --porcelain --untracked-files=all)

  if [[ "${#test_files[@]}" -gt 0 ]]; then
    git -C "$WORKTREE_DIR" add -- "${test_files[@]}"
  fi

  local changes
  changes="$(git -C "$WORKTREE_DIR" diff --cached --name-only)"
  if [[ -z "$changes" ]]; then
    printf 'nothing to commit for run %d\n' "$run_index"
    return 0
  fi

  git -C "$WORKTREE_DIR" commit -m "$commit_msg"
  local hash
  hash="$(git -C "$WORKTREE_DIR" rev-parse --short HEAD)"
  printf 'committed run %d as %s: %s\n' "$run_index" "$hash" "$commit_msg"
  printf '%s\t%s\t%s\tkeep\t%s\t%s\t%s\n' "$hash" "$TARGET" "$(resolve_target_pkg "$TARGET")" "$scenario" "$evidence" "$description" >> "$AR_DIR/results.tsv"
}

discard_candidate() {
  local run_index="$1"
  local reason="$2"
  local scenario="${3:-n/a}"
  local description="${4:-n/a}"
  local evidence="${5:-n/a}"

  git -C "$WORKTREE_DIR" restore --worktree .
  git -C "$WORKTREE_DIR" clean -fd
  printf 'discarded run %d: %s\n' "$run_index" "$reason"
  printf 'n/a\t%s\t%s\tdiscard\t%s\t%s\t%s\n' "$TARGET" "$(resolve_target_pkg "$TARGET")" "$scenario" "${evidence:-$reason}" "$description" >> "$AR_DIR/results.tsv"
}

run_iteration() {
  local run_index="$1"
  local decision_file="$WORKTREE_DIR/.autoresearch-decision-$run_index.txt"

  rm -f "$decision_file"

  run_single_candidate "$run_index" "$decision_file"
  local run_status=$?

  if [[ "$DRY_RUN" -eq 1 ]]; then
    append_loop_log "run $run_index dry-run complete"
    return 0
  fi

  if [[ "$run_status" -ne 0 ]]; then
    append_loop_log "run $run_index failed with exit $run_status"
    printf 'n/a\t%s\t%s\terror\tn/a\trun %d failed with exit %d\tn/a\n' "$TARGET" "$(resolve_target_pkg "$TARGET")" "$run_index" "$run_status" >> "$AR_DIR/results.tsv"
    append_issue "harness" "$TARGET" "tools/autoresearch/testing/scripts/autoloop.sh" "Autoresearch run exited before writing a decision" "run ${run_index} failed with exit ${run_status}" "Inspect OpenCode run logs and harden controller or prompt error handling"
    return 1
  fi

  if [[ ! -f "$decision_file" ]]; then
    if recover_decision_from_stdout "$run_index" "$decision_file"; then
      append_loop_log "run $run_index recovered decision file from stdout fallback"
    else
      append_loop_log "run $run_index produced no decision file"
      append_issue "harness" "$TARGET" "tools/autoresearch/testing/scripts/autoloop.sh" "Autoresearch run produced no decision artifact" "run ${run_index} produced no decision file" "Tighten prompt compliance or add controller-side fallback capture for interrupted runs"
      discard_candidate "$run_index" "no_decision_file" "n/a" "n/a" "no_decision_file"
      return 1
    fi
  fi

  local status reason scenario description evidence
  status="$(get_decision_field "$decision_file" status)"
  reason="$(get_decision_field "$decision_file" reason)"
  scenario="$(get_decision_field "$decision_file" scenario)"
  description="$(get_decision_field "$decision_file" description)"
  evidence="$(get_decision_field "$decision_file" evidence)"

  record_issue_from_decision "$decision_file"

  append_loop_log "run $run_index decision: status=$status reason=$reason"

  case "$status" in
    keep)
      BLOCKED_STREAK=0
      commit_candidate "$run_index" "$scenario" "$description" "$evidence"
      ;;
    discard)
      if [[ "$reason" == *blocked* || "$evidence" == *blocked* ]]; then
        BLOCKED_STREAK=$((BLOCKED_STREAK + 1))
      else
        BLOCKED_STREAK=0
      fi
      discard_candidate "$run_index" "$reason" "$scenario" "$description" "$evidence"
      ;;
    *)
      BLOCKED_STREAK=0
      append_loop_log "unknown status '$status' from decision file, treating as discard"
      discard_candidate "$run_index" "unknown_status" "$scenario" "$description" "$evidence"
      ;;
  esac

  rm -f "$decision_file"
}

append_loop_log() {
  local line="$1"
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$line" | tee -a "$LOOP_LOG"
}

cd "$ROOT_DIR"

if [[ "$PRINT_PROMPT" -eq 1 ]]; then
  print_prompt
  exit 0
fi

BRANCH_DATE="$(date '+%Y%m%d')"
RESEARCH_BRANCH="autoresearch/${TARGET}-${BRANCH_DATE}"
WORKTREE_DIR="$ROOT_DIR/.worktrees/autoresearch-$TARGET"
ensure_report_dirs
LOOP_LOG="$(test_log_path "$LOG_PREFIX-$TARGET")"

append_loop_log "target=$TARGET research_branch=$RESEARCH_BRANCH worktree=$WORKTREE_DIR model=${MODEL:-default}"

if ! ensure_main_clean; then
  if [[ "$FORCE" -ne 1 ]]; then
    exit 1
  fi
fi
ensure_local_infra
init_worktree
ensure_worktree_clean
ensure_worktree_on_research

if [[ "$RUN_BASELINE" -eq 1 ]]; then
  append_loop_log "running baseline"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'baseline would run in worktree: %s\n' "$WORKTREE_DIR"
  else
    GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' \
      go test ./internal/cdc -coverprofile="$REPORT_DIR/baseline/${TARGET}.cover.out"
    GOCACHE="$ROOT_DIR/.gocache" GOFLAGS='-buildvcs=false' \
      go tool cover -func="$REPORT_DIR/baseline/${TARGET}.cover.out" \
      > "$REPORT_DIR/baseline/${TARGET}.cover.txt"
    append_loop_log "baseline complete"
  fi
fi

for ((run_index = 1; run_index <= ITERATIONS; run_index++)); do
  append_loop_log "=== iteration $run_index of $ITERATIONS ==="
  run_iteration "$run_index"
  iter_status=$?

  if [[ "$iter_status" -ne 0 ]]; then
    append_loop_log "iteration $run_index did not complete successfully"
  fi

  if [[ "$BLOCKED_STREAK" -ge "$CONSECUTIVE_BLOCKED_LIMIT" ]]; then
    append_loop_log "stopping early after $BLOCKED_STREAK consecutive blocked discards; target guidance likely too restrictive"
    break
  fi

  if [[ "$run_index" -lt "$ITERATIONS" && "$SLEEP_SECONDS" -gt 0 ]]; then
    if [[ "$DRY_RUN" -eq 1 ]]; then
      printf 'sleep %s\n' "$SLEEP_SECONDS"
    else
      sleep "$SLEEP_SECONDS"
    fi
  fi
done

append_loop_log "autoloop finished: $ITERATIONS iterations completed for target=$TARGET"
