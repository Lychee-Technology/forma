-- Regression fixture for the DSQL lint gate (#740): DSQL-compatible SQL in
-- the shapes Forma uses. dsql-lint must report nothing here.

BEGIN;
CREATE TABLE IF NOT EXISTS fixture_guard (
    ltbase_schema_id SMALLINT NOT NULL,
    ltbase_row_id    UUID     NOT NULL,
    generation       BIGINT   NOT NULL,
    PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
);
COMMIT;

CREATE INDEX ASYNC IF NOT EXISTS fixture_guard_generation_idx ON fixture_guard (generation);

INSERT INTO fixture_guard AS g (ltbase_schema_id, ltbase_row_id, generation) VALUES ($1, $2, 1)
    ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET generation = g.generation + 1;

SELECT generation FROM fixture_guard WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2;
