#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./tools/autoresearch/benchmark/scripts/opencode_autoresearch.sh [options]

Options:
  -m, --model MODEL           OpenCode model in provider/model form
      --mode MODE             Launch mode: tui | run (default: run)
      --agent AGENT           Optional OpenCode agent name
      --preset NAME           Benchmark preset (default: small-live)
      --goal TEXT             Optimization goal text
      --goal-file PATH        File containing optimization goal text
      --protected-workloads CSV
                              Comma-separated protected workloads
      --print-prompt          Print rendered prompt and exit
      --dry-run               Print the OpenCode command and exit
  -c, --continue              Continue the last OpenCode session
  -s, --session ID            Continue a specific OpenCode session
      --fork                  Fork the session when continuing
  -h, --help                  Show this help
EOF
}

MODE="run"
MODEL="github-copilot/gpt-5-mini"
AGENT=""
PRESET="small-live"
GOAL_TEXT=""
GOAL_FILE=""
PROTECTED_WORKLOADS="$(default_protected_workloads_csv)"
CONTINUE_LAST=0
SESSION_ID=""
FORK_SESSION=0
PRINT_PROMPT=0
DRY_RUN=0
WORKTREE_DIR_OVERRIDE="${WORKTREE_DIR:-$ROOT_DIR}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--model)
      MODEL="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
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
    -c|--continue)
      CONTINUE_LAST=1
      shift
      ;;
    -s|--session)
      SESSION_ID="${2:-}"
      shift 2
      ;;
    --fork)
      FORK_SESSION=1
      shift
      ;;
    --print-prompt)
      PRINT_PROMPT=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
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

PROMPT="$(<"$AR_DIR/prompts/opencode-single-candidate.md")"
PROMPT="${PROMPT//\{\{PRESET\}\}/$PRESET}"
PROMPT="${PROMPT//\{\{PROTECTED_WORKLOADS\}\}/$PROTECTED_WORKLOADS}"
PROMPT="${PROMPT//\{\{GOAL\}\}/$GOAL_TEXT}"
PROMPT="${PROMPT//\{\{WORKTREE_DIR\}\}/$WORKTREE_DIR_OVERRIDE}"

if [[ "$PRINT_PROMPT" -eq 1 ]]; then
  printf '%s\n' "$PROMPT"
  exit 0
fi

COMMAND=(opencode)
if [[ "$MODE" == "run" ]]; then
  COMMAND+=(run)
fi
if [[ -n "$MODEL" ]]; then
  COMMAND+=(--model "$MODEL")
fi
if [[ -n "$AGENT" ]]; then
  COMMAND+=(--agent "$AGENT")
fi
if [[ "$CONTINUE_LAST" -eq 1 ]]; then
  COMMAND+=(--continue)
fi
if [[ -n "$SESSION_ID" ]]; then
  COMMAND+=(--session "$SESSION_ID")
fi
if [[ "$FORK_SESSION" -eq 1 ]]; then
  COMMAND+=(--fork)
fi
if [[ "$MODE" == "tui" ]]; then
  COMMAND+=(--prompt "$PROMPT" "$ROOT_DIR")
else
  COMMAND+=(--title "autoresearch-benchmark:${PRESET}" -- "$PROMPT")
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  printf 'working directory: %s\n' "$ROOT_DIR"
  printf 'command:'
  printf ' %q' "${COMMAND[@]}"
  printf '\n'
  exit 0
fi

cd "$ROOT_DIR"
exec "${COMMAND[@]}"
