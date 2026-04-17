#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/autoresearch/testing/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

TARGET="postgres_repo_query"
LOG_PREFIX="fallback-test"
stdout_log="$(test_log_path "${LOG_PREFIX}-${TARGET}-run-1.stdout")"
decision_file="$REPORT_DIR/logs/recovered-decision-test.txt"

mkdir -p "$(dirname "$stdout_log")"
rm -f "$stdout_log" "$decision_file"

cat > "$stdout_log" <<'EOF'
noise before
AUTORESEARCH_DECISION_BEGIN
status=discard
reason=stdout fallback worked
scenario=n/a
description=recovered from stdout only
evidence=n/a
issue_category=harness
issue_file=tools/autoresearch/testing/scripts/autoloop.sh
issue_title=Recovered from stdout fallback
issue_evidence=synthetic test
issue_suggested_fix=n/a
AUTORESEARCH_DECISION_END
noise after
EOF

python3 "$SCRIPT_DIR/extract_decision_from_stdout.py" "$stdout_log" "$decision_file"

rg '^status=discard$' "$decision_file" >/dev/null
rg '^reason=stdout fallback worked$' "$decision_file" >/dev/null
rg '^issue_title=Recovered from stdout fallback$' "$decision_file" >/dev/null

rm -f "$stdout_log" "$decision_file"
printf 'stdout fallback recovery test passed\n'
