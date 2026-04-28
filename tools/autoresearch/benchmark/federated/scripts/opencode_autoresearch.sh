#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/benchmark/federated/common.sh
source "$SCRIPT_DIR/../common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./tools/autoresearch/benchmark/federated/scripts/opencode_autoresearch.sh [options]

Options:
  -m, --model MODEL        OpenCode model in provider/model form (default: github-copilot/gpt-5-mini)
  -t, --target TARGET      Benchmark target key (default: postgres_duckdb_query)
  -g, --gate GATE          Benchmark gate key (default: target default)
      --agent AGENT        Optional OpenCode agent name
      --single-candidate   Generate a single-candidate prompt
      --decision-file PATH Decision artifact path (required with --single-candidate)
  -c, --continue           Continue the last OpenCode session
  -s, --session ID         Continue a specific OpenCode session
      --fork               Fork the session when continuing
      --print-prompt       Print the generated prompt and exit
      --dry-run            Print the OpenCode command and exit
  -h, --help               Show this help
EOF
}

TARGET="$(default_benchmark_target)"
GATE=""
MODEL="github-copilot/gpt-5-mini"
AGENT=""
SINGLE_CANDIDATE=0
DECISION_FILE=""
WORKTREE_DIR_OVERRIDE="${WORKTREE_DIR:-$ROOT_DIR}"
CONTINUE_LAST=0
SESSION_ID=""
FORK_SESSION=0
PRINT_PROMPT=0
DRY_RUN=0

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
    -g|--gate)
      GATE="${2:-}"
      shift 2
      ;;
    --agent)
      AGENT="${2:-}"
      shift 2
      ;;
    --single-candidate)
      SINGLE_CANDIDATE=1
      shift
      ;;
    --decision-file)
      DECISION_FILE="${2:-}"
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
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

resolve_target_source "$TARGET" >/dev/null
GATE="$(resolve_target_gate "$TARGET" "$GATE")"

if [[ "$SINGLE_CANDIDATE" -eq 1 && -z "$DECISION_FILE" ]]; then
  printf '--decision-file is required with --single-candidate\n' >&2
  exit 1
fi

if [[ -n "$SESSION_ID" && "$CONTINUE_LAST" -eq 1 ]]; then
  printf 'use either --continue or --session, not both\n' >&2
  exit 1
fi

if [[ "$SINGLE_CANDIDATE" -ne 1 ]]; then
  printf 'only --single-candidate mode is supported in benchmark autoresearch\n' >&2
  exit 1
fi

TEMPLATE_PATH="$AR_DIR/prompts/opencode-single-candidate.md"
PROMPT="$(render_prompt_template_with_decision "$TEMPLATE_PATH" "$TARGET" "$GATE" "$DECISION_FILE" "$WORKTREE_DIR_OVERRIDE")"

if [[ "$PRINT_PROMPT" -eq 1 ]]; then
  printf '%s\n' "$PROMPT"
  exit 0
fi

COMMAND=(opencode run)

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
COMMAND+=(--title "benchmark-autoresearch:${TARGET}:${GATE}" -- "$PROMPT")

if [[ "$DRY_RUN" -eq 1 ]]; then
  printf 'working directory: %s\n' "$ROOT_DIR"
  printf 'command:'
  printf ' %q' "${COMMAND[@]}"
  printf '\n'
  exit 0
fi

if ! command -v opencode >/dev/null 2>&1; then
  printf 'opencode not found in PATH\n' >&2
  exit 1
fi

cd "$ROOT_DIR"
exec "${COMMAND[@]}"
