#!/usr/bin/env bash
# Default value for APP_ID
APP_ID="${1:-1234567890}"

set -euo pipefail

API_ID="1234567890"  # Replace with your actual API ID

go run ../cmd/tools/main.go init-db \
  --db-host localhost \
  --db-port 5432 \
  --db-name ltbase \
  --db-user postgres \
  --db-password postgres \
  --db-ssl-mode disable \
  --schema-table "schema_registry_$API_ID" \
  --eav-table "eav_data_$API_ID" \
  --entity-main-table "entity_main_$API_ID"