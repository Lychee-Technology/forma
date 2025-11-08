#!/usr/bin/env bash

set -euo pipefail

: "${BASE_URL:=http://localhost:8080}"

echo ">> Using base URL: ${BASE_URL}"

activity_id="${ACTIVITY_ID:-}"
if [[ -z "${activity_id}" ]]; then
  if command -v uuidgen >/dev/null 2>&1; then
    activity_id=$(uuidgen)
  else
    activity_id=$(python - <<'PY'
import uuid
print(uuid.uuid4())
PY
)
  fi
fi

shift_days_iso() {
  local days="$1"
  local offset="${days}"
  [[ "${days}" == -* ]] || offset="+${days}"
  date -u -v"${offset}"d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
    || date -u -d "${days} days" +"%Y-%m-%dT%H:%M:%SZ"
}

now_ts=$(shift_days_iso 0)
follow_up_ts=$(shift_days_iso 2)
updated_follow_up_ts=$(shift_days_iso 5)
yesterday_ts=$(shift_days_iso -1)

create_payload=$(
  cat <<JSON
{
  "id": "${activity_id}",
  "type": "call",
  "direction": "outbound",
  "at": "${now_ts}",
  "userId": "user-cli-001",
  "summary": "Initial outbound call logged from CLI script",
  "nextFollowUpAt": "${follow_up_ts}"
}
JSON
)

echo ">> Creating activity (id: ${activity_id})..."
create_response=$(curl -v -sS -X POST "${BASE_URL}/api/v1/activity" \
  -H "Content-Type: application/json" \
  -d "${create_payload}")

echo "${create_response}" | jq .

row_id=$(echo "${create_response}" | jq -r '.row_id')
if [[ -z "${row_id}" || "${row_id}" == "null" ]]; then
  echo "!! Failed to extract row_id from create response" >&2
  exit 1
fi

echo ">> Stored row_id: ${row_id}"

echo ">> Listing activities..."
curl -v -sS "${BASE_URL}/api/v1/activity?page=1&items_per_page=10" | jq .

echo ">> Fetching single activity..."
curl -v -sS "${BASE_URL}/api/v1/activity/${row_id}" | jq .

update_payload=$(
  cat <<JSON
{
  "summary": "Inbound follow-up recorded; customer rescheduled",
  "direction": "inbound",
  "type": "note",
  "nextFollowUpAt": "${updated_follow_up_ts}"
}
JSON
)

echo ">> Updating activity..."
curl -v -sS -X PUT "${BASE_URL}/api/v1/activity/${row_id}" \
  -H "Content-Type: application/json" \
  -d "${update_payload}" | jq .

echo ">> Testing Advanced Query..."

echo ">> 1. Query by direction (inbound)..."
query_direction_payload=$(
  cat <<JSON
{
  "schema_name": "activity",
  "page": 1,
  "items_per_page": 10,
  "condition": {
    "a": "direction",
    "v": "inbound"
  }
}
JSON
)
curl -v -sS -X POST "${BASE_URL}/api/v1/advanced_query" \
  -H "Content-Type: application/json" \
  -d "${query_direction_payload}" | jq .

echo ">> 2. Query recent notes (type=note AND at>yesterday)..."
query_recent_payload=$(
  cat <<JSON
{
  "schema_name": "activity",
  "page": 1,
  "items_per_page": 5,
  "condition": {
    "l": "and",
    "c": [
      {
        "a": "type",
        "v": "note"
      },
      {
        "a": "at",
        "v": "gt:${yesterday_ts}"
      }
    ]
  }
}
JSON
)
curl -v -sS -X POST "${BASE_URL}/api/v1/advanced_query" \
  -H "Content-Type: application/json" \
  -d "${query_recent_payload}" | jq .

if [[ "${SKIP_DELETE:-false}" != "true" ]]; then
  echo ">> Deleting activity..."
  curl -v -sS -X DELETE "${BASE_URL}/api/v1/activity/${row_id}" | jq .
else
  echo ">> Skipping deletion (set SKIP_DELETE=false to enable)."
fi

echo ">> Listing activities after delete/skip..."
curl -v -sS "${BASE_URL}/api/v1/activity?page=1&items_per_page=10" | jq .

echo ">> Activity CRUD flow completed."
