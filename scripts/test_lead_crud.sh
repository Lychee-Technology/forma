#!/usr/bin/env bash

set -euo pipefail

: "${BASE_URL:=http://localhost:8080}"

echo ">> Using base URL: ${BASE_URL}"

create_payload=$(
  cat <<'JSON'
{
  "id": "lead-cli-001",
  "status": "hot",
  "personalInfo": {
    "name": {
      "display": "CLI Lead",
      "given": "CLI",
      "family": "Lead"
    },
    "age": 29,
    "maritalStatus": "single"
  },
  "contactInfo": {
    "email": "cli.lead@example.com",
    "phone": "+81-90-0000-0000",
    "preferredContactMethod": "email"
  },
  "currentAddress": {
    "city": "Tokyo",
    "prefecture": "Tokyo",
    "fullAddress": "1-2-3 Shibuya"
  },
  "propertyRequirements": {
    "budget": {
      "min": 60000000,
      "max": 90000000,
      "currency": "JPY"
    },
    "desiredAreas": ["Shibuya", "Meguro"],
    "propertyType": "condo",
    "bedrooms": {
      "min": 2,
      "max": 3
    },
    "maxStationWalkMinutes": 12,
    "preferences": ["pet-friendly", "south-facing"]
  },
  "metadata": {
    "createdBy": "cli-test",
    "source": "web"
  }
}
JSON
)

echo ">> Creating lead..."
create_response=$(curl -v -sS -X POST "${BASE_URL}/api/v1/lead" \
  -H "Content-Type: application/json" \
  -d "${create_payload}")

echo "${create_response}" | jq .

row_id=$(echo "${create_response}" | jq -r '.row_id')
if [[ -z "${row_id}" || "${row_id}" == "null" ]]; then
  echo "!! Failed to extract row_id from create response" >&2
  exit 1
fi

echo ">> Stored row_id: ${row_id}"

echo ">> Listing leads..."
curl  -v -sS "${BASE_URL}/api/v1/lead?page=1&items_per_page=10" | jq .

echo ">> Fetching single lead..."
curl  -v -sS "${BASE_URL}/api/v1/lead/${row_id}" | jq .

update_payload=$(
  cat <<JSON
{
  "id": "lead-cli-001",
  "status": "warm",
  "personalInfo": {
    "name": {
      "display": "CLI Lead",
      "given": "CLI",
      "family": "Lead"
    },
    "age": 30,
    "maritalStatus": "single"
  },
  "contactInfo": {
    "email": "cli.lead@example.com",
    "phone": "+81-80-9999-8888",
    "preferredContactMethod": "line"
  },
  "currentAddress": {
    "city": "Tokyo",
    "prefecture": "Tokyo",
    "fullAddress": "1-2-3 Shibuya"
  },
  "propertyRequirements": {
    "budget": {
      "min": 70000000,
      "max": 100000000,
      "currency": "JPY"
    },
    "desiredAreas": ["Shibuya", "Meguro"],
    "propertyType": "condo",
    "bedrooms": {
      "min": 2,
      "max": 3
    },
    "maxStationWalkMinutes": 10,
    "preferences": ["pet-friendly", "south-facing", "gym"]
  },
  "metadata": {
    "createdBy": "cli-test",
    "source": "web",
    "updatedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  }
}
JSON
)

echo ">> Updating lead..."
curl -v -sS -X PUT "${BASE_URL}/api/v1/lead/${row_id}" \
  -H "Content-Type: application/json" \
  -d "${update_payload}" | jq .

# echo ">> Deleting lead..."
# curl -v -sS -X DELETE "${BASE_URL}/api/v1/lead/${row_id}"
# echo

echo ">> Listing leads after deletion..."
curl  -v -sS "${BASE_URL}/api/v1/lead?page=1&items_per_page=10" | jq .

echo ">> CRUD flow completed."
