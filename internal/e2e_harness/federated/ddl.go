package federated

import (
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// federatedDDL is the table DDL the federated harness creates. It is a
// hand-maintained copy of the init-db DDL (#440); ddl_test.go pins its
// entity_main statement to the runtime column set so it cannot drift from
// what the writer, the read projection and the CDC column order are built
// for (#585). The seed row for schema_id=1 is what the federated fixtures
// expect. The unique-key tables (#651), the row-guard table and the
// schema-lease table (#633) come from uniquekey, rowguard and schemalease,
// the sources init-db uses, rather than from another copy.
var federatedDDL = append([]string{
	`CREATE TABLE IF NOT EXISTS schema_registry (
		schema_id SMALLINT PRIMARY KEY,
		schema_name TEXT COLLATE "C" NOT NULL UNIQUE,
		created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
	)`,
	`CREATE TABLE IF NOT EXISTS entity_main (
		ltbase_schema_id SMALLINT NOT NULL,
		ltbase_row_id UUID NOT NULL,
		text_01 TEXT COLLATE "C",
		text_02 TEXT COLLATE "C",
		text_03 TEXT COLLATE "C",
		text_04 TEXT COLLATE "C",
		text_05 TEXT COLLATE "C",
		text_06 TEXT COLLATE "C",
		text_07 TEXT COLLATE "C",
		text_08 TEXT COLLATE "C",
		text_09 TEXT COLLATE "C",
		text_10 TEXT COLLATE "C",
		smallint_01 SMALLINT,
		smallint_02 SMALLINT,
		smallint_03 SMALLINT,
		integer_01 INTEGER,
		integer_02 INTEGER,
		integer_03 INTEGER,
		bigint_01 BIGINT,
		bigint_02 BIGINT,
		bigint_03 BIGINT,
		double_01 DOUBLE PRECISION,
		double_02 DOUBLE PRECISION,
		double_03 DOUBLE PRECISION,
		uuid_01 UUID,
		uuid_02 UUID,
		ltbase_created_at BIGINT NOT NULL,
		ltbase_updated_at BIGINT NOT NULL,
		ltbase_deleted_at BIGINT,
		ltbase_created_by TEXT COLLATE "C",
		ltbase_updated_by TEXT COLLATE "C",
		ltbase_deleted_by TEXT COLLATE "C",
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`,
	`CREATE TABLE IF NOT EXISTS eav_data (
		schema_id SMALLINT NOT NULL,
		row_id UUID NOT NULL,
		attr_id SMALLINT NOT NULL,
		array_indices TEXT COLLATE "C" NOT NULL DEFAULT '',
		value_text TEXT COLLATE "C",
		value_numeric DOUBLE PRECISION,
		PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
	)`,
	`CREATE TABLE IF NOT EXISTS change_log (
		schema_id SMALLINT NOT NULL,
		row_id UUID NOT NULL,
		changed_at BIGINT NOT NULL,
		deleted_at BIGINT DEFAULT 0,
		flushed_at BIGINT DEFAULT 0,
		PRIMARY KEY (schema_id, row_id, flushed_at)
	)`,
	`INSERT INTO schema_registry (schema_id, schema_name) VALUES (1, 'test_entity') ON CONFLICT DO NOTHING`,
}, append(uniquekey.StatementsFor("entity_main"), rowguard.StatementFor("entity_main"),
	schemalease.StatementFor("entity_main"))...)
