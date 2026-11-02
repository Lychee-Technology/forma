package cdc

import (
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// buildExportSQL renders the production delta export plan as the SQL tests
// read: sql is the staging setup followed by the COPY, and the queries are
// the PostgreSQL reads of the change_log, entity_main and eav_data stages.
func buildExportSQL(s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, string, error) {
	plan, err := buildDeltaExportPlan(stagedTransport, s3TmpPath, cfg, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return "", "", "", "", err
	}
	return plan.setupSQL + plan.copySQL, stageQuery(plan, "cl"), stageQuery(plan, "m"), stageQuery(plan, "e_rows"), nil
}

// buildBaseExportSQL is buildExportSQL for the base export, which reads no
// change_log.
func buildBaseExportSQL(s3TmpPath string, cfg CDCConfig, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, error) {
	plan, err := buildBaseExportPlan(stagedTransport, s3TmpPath, cfg, schemaID, rowIDs, attrCache)
	if err != nil {
		return "", "", "", err
	}
	return plan.setupSQL + plan.copySQL, stageQuery(plan, "m"), stageQuery(plan, "e_rows"), nil
}

func stageQuery(plan exportSQLPlan, alias string) string {
	for _, stage := range plan.stages {
		if stage.alias == alias {
			return stage.query
		}
	}
	return ""
}
