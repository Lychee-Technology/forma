#!/usr/bin/env bash

set -euo pipefail

API_ID="${1:-1234567890}"
SCHEMA_DIR="${2:-cmd/server/schemas}"

echo "Initializing database for API ID: $API_ID"
echo "Schema directory: $SCHEMA_DIR"

./build/tools init-db \
  --db-host localhost \
  --db-port 5432 \
  --db-name ltbase \
  --db-user postgres \
  --db-password postgres \
  --db-ssl-mode disable \
  --schema-table "schema_registry_$API_ID" \
  --eav-table "eav_data_$API_ID" \
  --entity-main-table "entity_main_$API_ID" \
  --change-log-table "change_log_$API_ID" \
  --schema-dir "$SCHEMA_DIR"
