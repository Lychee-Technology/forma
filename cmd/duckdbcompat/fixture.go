package main

import (
	"strconv"
	"strings"
)

// fixtureRows spans two row groups at DuckDB's default ROW_GROUP_SIZE of
// 122880, so a reader also has to stitch groups together.
const fixtureRows = 150000

// fixtureSQL builds the fixture deterministically from range(), with only
// functions whose results do not depend on the engine release (md5, integer
// and float arithmetic, CASE). No lambdas: their syntax differs between
// DuckDB 1.x and 2.0, and the same SQL must run on both.
//
// Columns follow the e2e_wide export layout: the system columns, then one
// attribute per parquet type the exporters produce. i % 10 = 0 rows are
// tombstones (deleted_at > 0) with every attribute NULL, as the delta
// exporter writes a hard-deleted row.
var fixtureSQL = strings.ReplaceAll(fixtureTemplate, "{rows}", strconv.Itoa(fixtureRows))

const fixtureTemplate = `CREATE OR REPLACE TABLE fixture AS
WITH src AS (
  SELECT i, md5(CAST(i AS VARCHAR)) AS h, md5('ref' || CAST(i AS VARCHAR)) AS r, i % 10 = 0 AS dead
  FROM range({rows}) t(i)
)
SELECT
  CAST(20 AS SMALLINT) AS schema_id,
  CAST(substr(h, 1, 8) || '-' || substr(h, 9, 4) || '-' || substr(h, 13, 4) || '-' || substr(h, 17, 4) || '-' || substr(h, 21, 12) AS UUID) AS row_id,
  CAST(1700000000000 + i * 1000 AS BIGINT) AS changed_at,
  CAST(CASE WHEN dead THEN 1700000000000 + i * 1000 ELSE 0 END AS BIGINT) AS deleted_at,
  CAST(CASE WHEN dead THEN NULL ELSE 1690000000000 + i END AS BIGINT) AS ltbase_created_at,
  CAST(CASE WHEN dead THEN NULL ELSE 1690000000000 + 2 * i END AS BIGINT) AS ltbase_updated_at,
  CAST(NULL AS BIGINT) AS ltbase_deleted_at,
  CAST(CASE WHEN dead THEN NULL WHEN i % 7 = 1 THEN NULL WHEN i % 7 = 2 THEN ''
            WHEN i % 7 = 3 THEN 'żółw 🐢 ''quoted'' "double"' ELSE 'title-' || CAST(i % 50 AS VARCHAR) END AS VARCHAR) AS title,
  CAST(CASE WHEN dead THEN NULL ELSE h END AS VARCHAR) AS note,
  CAST(CASE WHEN dead OR i % 9 = 1 THEN NULL WHEN i % 9 = 2 THEN -32768 WHEN i % 9 = 3 THEN 32767 ELSE i % 1000 END AS SMALLINT) AS rank,
  CAST(CASE WHEN dead OR i % 9 = 1 THEN NULL WHEN i % 9 = 2 THEN -2147483648 WHEN i % 9 = 3 THEN 2147483647 ELSE i * 7 END AS INTEGER) AS count,
  CAST(CASE WHEN dead OR i % 9 = 1 THEN NULL WHEN i % 9 = 2 THEN -9223372036854775808 WHEN i % 9 = 3 THEN 9223372036854775807
            ELSE i * 1000003 END AS BIGINT) AS amount,
  CAST(CASE WHEN dead OR i % 9 = 1 THEN NULL WHEN i % 9 = 2 THEN 1.7976931348623157e308 WHEN i % 9 = 3 THEN 4.9e-324
            WHEN i % 9 = 4 THEN -123456.789 ELSE i * 0.25 + 0.1 END AS DOUBLE) AS score,
  CAST(CASE WHEN dead OR i % 3 = 1 THEN NULL
            ELSE substr(r, 1, 8) || '-' || substr(r, 9, 4) || '-' || substr(r, 13, 4) || '-' || substr(r, 17, 4) || '-' || substr(r, 21, 12) END AS UUID) AS ref,
  CAST(CASE WHEN dead OR i % 3 = 1 THEN NULL ELSE substr(r, 1, 8) || '-' || substr(r, 9, 4) || '-' || substr(r, 13, 4) || '-' || substr(r, 17, 4) || '-' || substr(r, 21, 12) END AS VARCHAR) AS token,
  CAST(CASE WHEN dead OR i % 3 = 1 THEN NULL ELSE i % 3 = 0 END AS BOOLEAN) AS active,
  CAST(CASE WHEN dead OR i % 4 = 1 THEN NULL ELSE -62135596800000 + i * 86400000 END AS BIGINT) AS born,
  CAST(CASE WHEN dead OR i % 5 = 1 THEN NULL WHEN i % 5 = 2 THEN 9007199254740993 ELSE i * 0.5 END AS DOUBLE) AS level,
  CAST(CASE WHEN dead OR i % 4 = 1 THEN NULL WHEN i % 4 = 2 THEN [] WHEN i % 4 = 3 THEN ['only']
            ELSE ['a-' || CAST(i AS VARCHAR), '', 'żółw', 'c'] END AS VARCHAR[]) AS tags,
  CAST(CASE WHEN dead OR i % 4 = 1 THEN NULL WHEN i % 4 = 2 THEN [] ELSE [i * 0.5, -1.5, 1.7976931348623157e308] END AS DOUBLE[]) AS scores,
  CAST(CASE WHEN dead OR i % 4 = 1 THEN NULL WHEN i % 4 = 2 THEN [] ELSE [i, -9223372036854775808, 9223372036854775807] END AS BIGINT[]) AS stamps,
  CAST(CASE WHEN dead OR i % 4 = 1 THEN NULL WHEN i % 4 = 2 THEN [] ELSE [true, false, i % 2 = 0] END AS BOOLEAN[]) AS flags
FROM src
ORDER BY i`
