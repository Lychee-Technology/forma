package federated

// federatedDDL is the table DDL the federated harness creates. It is a
// hand-maintained copy of the init-db DDL (#440); ddl_test.go pins its
// entity_main statement to the runtime column set so it cannot drift from
// what the writer, the read projection and the CDC column order are built
// for (#585). The seed row for schema_id=1 is what the federated fixtures
// expect.
var federatedDDL = []string{
	`CREATE TABLE IF NOT EXISTS schema_registry (
		schema_id SMALLINT PRIMARY KEY,
		schema_name TEXT NOT NULL UNIQUE,
		created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
	)`,
	`CREATE TABLE IF NOT EXISTS entity_main (
		ltbase_schema_id SMALLINT NOT NULL,
		ltbase_row_id UUID NOT NULL,
		text_01 TEXT,
		text_02 TEXT,
		text_03 TEXT,
		text_04 TEXT,
		text_05 TEXT,
		text_06 TEXT,
		text_07 TEXT,
		text_08 TEXT,
		text_09 TEXT,
		text_10 TEXT,
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
		ltbase_created_by TEXT,
		ltbase_updated_by TEXT,
		ltbase_deleted_by TEXT,
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`,
	`CREATE TABLE IF NOT EXISTS eav_data (
		schema_id SMALLINT NOT NULL,
		row_id UUID NOT NULL,
		attr_id SMALLINT NOT NULL,
		array_indices TEXT NOT NULL DEFAULT '',
		value_text TEXT,
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
}
