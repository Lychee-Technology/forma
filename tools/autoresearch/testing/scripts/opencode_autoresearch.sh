#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./tools/autoresearch/testing/scripts/opencode_autoresearch.sh [options]

Options:
  -m, --model MODEL           OpenCode model in provider/model form
  -t, --target TARGET         Target key: flusher | postgres_duckdb_query | entity_query_service | postgres_repo_query
      --mode MODE             Launch mode: tui | run (default: tui)
      --agent AGENT           Optional OpenCode agent name
      --single-candidate       Generate a single-candidate prompt (controller mode)
      --decision-file PATH     Path where the decision artifact will be written
  -c, --continue              Continue the last OpenCode session
  -s, --session ID            Continue a specific OpenCode session
      --fork                  Fork the session when continuing
      --print-prompt          Print the generated prompt and exit
      --dry-run               Print the OpenCode command and exit
  -h, --help                  Show this help

Examples:
  ./tools/autoresearch/testing/scripts/opencode_autoresearch.sh --model openai/gpt-5 --target flusher
  ./tools/autoresearch/testing/scripts/opencode_autoresearch.sh --mode run --model anthropic/claude-sonnet-4-5 --target flusher --single-candidate --decision-file /tmp/decision.txt
  ./tools/autoresearch/testing/scripts/opencode_autoresearch.sh --print-prompt --target flusher

This launcher generates a BDD-first prompt for the selected target.
EOF
}

TARGET="flusher"
MODE="tui"
MODEL=""
AGENT=""
SINGLE_CANDIDATE=0
DECISION_FILE=""
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
    --mode)
      MODE="${2:-}"
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

resolve_target_pkg "$TARGET" >/dev/null

if [[ "$SINGLE_CANDIDATE" -eq 1 && -z "$DECISION_FILE" ]]; then
  printf '--decision-file is required when using --single-candidate\n' >&2
  exit 1
fi

case "$MODE" in
  tui|run)
    ;;
  *)
    printf 'invalid mode: %s\n' "$MODE" >&2
    exit 1
    ;;
esac

if [[ -n "$SESSION_ID" && "$CONTINUE_LAST" -eq 1 ]]; then
  printf 'use either --continue or --session, not both\n' >&2
  exit 1
fi

if [[ "$SINGLE_CANDIDATE" -eq 1 ]]; then
  TEMPLATE_PATH="$AR_DIR/prompts/opencode-single-candidate.md"
  if [[ -n "$DECISION_FILE" ]]; then
    PROMPT="$(render_prompt_template_with_decision "$TEMPLATE_PATH" "$TARGET" "$DECISION_FILE")"
  else
    PROMPT="$(render_prompt_template "$TEMPLATE_PATH" "$TARGET" "" "")"
  fi
else
  TEMPLATE_PATH="$AR_DIR/prompts/opencode-autoloop.md"
  PROMPT="$(render_prompt_template "$TEMPLATE_PATH" "$TARGET" "" "")"
fi

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
  COMMAND+=(--title "autoresearch:${TARGET}" -- "$PROMPT")
fi

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
