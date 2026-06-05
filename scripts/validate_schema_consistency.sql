\if :{?schema_table}
\else
\set schema_table schema_registry_dev
\endif

\if :{?eav_table}
\else
\set eav_table eav_data_dev
\endif

\if :{?entity_main_table}
\else
\set entity_main_table entity_main_dev
\endif

\echo '=== Forma schema consistency validation ==='
\echo ''
\echo 'Tables:'
\echo '  schema_table      = ' :schema_table
\echo '  eav_table         = ' :eav_table
\echo '  entity_main_table = ' :entity_main_table
\echo ''
\echo 'Notes:'
\echo '- This script checks DB-level consistency only.'
\echo '- Run `make validate-schema-consistency` for metadata-aware checks against *_attributes.json files.'
\echo ''

\echo '1. Duplicate schema IDs'
SELECT
  schema_id,
  array_agg(schema_name ORDER BY schema_name) AS conflicting_schemas,
  COUNT(*) AS duplicate_count
FROM :schema_table
GROUP BY schema_id
HAVING COUNT(*) > 1
ORDER BY schema_id;

\echo ''
\echo '2. Duplicate schema names'
SELECT
  schema_name,
  array_agg(schema_id ORDER BY schema_id) AS conflicting_schema_ids,
  COUNT(*) AS duplicate_count
FROM :schema_table
GROUP BY schema_name
HAVING COUNT(*) > 1
ORDER BY schema_name;

\echo ''
\echo '3. EAV schema IDs missing from registry'
SELECT
  e.schema_id,
  COUNT(*) AS orphan_rows
FROM :eav_table AS e
LEFT JOIN :schema_table AS s ON s.schema_id = e.schema_id
WHERE s.schema_id IS NULL
GROUP BY e.schema_id
ORDER BY e.schema_id;

\echo ''
\echo '4. entity_main schema IDs missing from registry'
SELECT
  m.ltbase_schema_id AS schema_id,
  COUNT(*) AS orphan_rows
FROM :entity_main_table AS m
LEFT JOIN :schema_table AS s ON s.schema_id = m.ltbase_schema_id
WHERE s.schema_id IS NULL
GROUP BY m.ltbase_schema_id
ORDER BY m.ltbase_schema_id;

\echo ''
\echo '5. change-log style entity references missing from entity_main primary key'
\echo '   Skipped here because change_log table name is deployment-specific and not provided to this script.'

\echo ''
\echo '6. EAV rows with both value columns populated'
SELECT
  schema_id,
  attr_id,
  COUNT(*) AS affected_rows
FROM :eav_table
WHERE value_text IS NOT NULL
  AND value_numeric IS NOT NULL
GROUP BY schema_id, attr_id
ORDER BY schema_id, attr_id;

\echo ''
\echo '7. EAV rows with neither value column populated'
SELECT
  schema_id,
  attr_id,
  COUNT(*) AS affected_rows
FROM :eav_table
WHERE value_text IS NULL
  AND value_numeric IS NULL
GROUP BY schema_id, attr_id
ORDER BY schema_id, attr_id;

\echo ''
\echo '8. Duplicate entity_main primary keys (should be impossible unless constraints were bypassed)'
SELECT
  ltbase_schema_id,
  ltbase_row_id,
  COUNT(*) AS duplicate_count
FROM :entity_main_table
GROUP BY ltbase_schema_id, ltbase_row_id
HAVING COUNT(*) > 1
ORDER BY ltbase_schema_id, ltbase_row_id;

\echo ''
\echo '9. Duplicate EAV primary keys (should be impossible unless constraints were bypassed)'
SELECT
  schema_id,
  row_id,
  attr_id,
  array_indices,
  COUNT(*) AS duplicate_count
FROM :eav_table
GROUP BY schema_id, row_id, attr_id, array_indices
HAVING COUNT(*) > 1
ORDER BY schema_id, row_id, attr_id, array_indices;

\echo ''
\echo '=== Validation complete ==='
