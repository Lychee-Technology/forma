-- Regression fixture for the DSQL lint gate (#740): every statement here is
-- PostgreSQL that Aurora DSQL rejects. The gate requires dsql-lint to report
-- exactly the rules named by the expect comments, so a tool upgrade that
-- stops catching one of them fails the gate instead of passing silently.
--
-- expect: multi_ddl_transaction
-- expect: index_async
-- expect: index_concurrently
-- expect: set_transaction
-- expect: truncate
-- expect: serial_type

-- DSQL allows one DDL statement per transaction (init-db's #690 shape).
BEGIN;
CREATE TABLE IF NOT EXISTS fixture_a (id UUID PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS fixture_b (id UUID PRIMARY KEY, v TEXT);
COMMIT;

-- DSQL builds secondary indexes only asynchronously.
CREATE INDEX IF NOT EXISTS fixture_a_v_idx ON fixture_a (v);
CREATE INDEX CONCURRENTLY IF NOT EXISTS fixture_b_v_idx ON fixture_b (v);

-- DSQL runs at a fixed REPEATABLE READ and has no SET TRANSACTION.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

TRUNCATE fixture_a;

CREATE TABLE fixture_c (id SERIAL PRIMARY KEY);
