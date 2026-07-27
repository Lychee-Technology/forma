package cdc

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/duckdbinit"
)

// buildExporterInitSteps assembles the PRAGMA/INSTALL/LOAD/SET statements
// every pooled exporter connection must run on open, preserving the exact
// statement set and order NewDuckExporter previously issued through the pool
// (#285). Credential validation happens here, so construction fails fast
// before any connection is opened.
func buildExporterInitSteps(cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	if cfg.DuckMemLimit != "" {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA memory_limit='%s';", cfg.DuckMemLimit), "set memory_limit"))
	}
	if cfg.DuckThreads > 0 {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA threads=%d;", cfg.DuckThreads), "set threads"))
	}
	// postgres_scanner first for postgres_query
	for _, ext := range []string{"postgres_scanner", "httpfs", "parquet"} {
		steps = append(steps, duckdbinit.ExtensionStep(ext))
	}
	s3Steps, err := buildExporterS3Steps(cfg, s3AccessKey, s3Secret, s3SessionToken)
	if err != nil {
		return nil, fmt.Errorf("build cdc duckdb s3 statements: %w", err)
	}
	return append(steps, s3Steps...), nil
}

func buildExporterS3Steps(cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	credentials := []struct{ name, value string }{
		{"s3_access_key_id", s3AccessKey},
		{"s3_secret_access_key", s3Secret},
		// Temporary credentials (STS/assumed roles) are a key+secret+token
		// triple; without the token httpfs signs requests the store rejects
		// even though the SDK client on the same credentials works. The token
		// arrives from the caller alongside the pair, so it can only come from
		// the source that supplied that pair (#329) — resolving it here
		// independently is exactly the cross-source mix that fails opaquely.
		// See ResolveStaticS3Credentials.
		{"s3_session_token", s3SessionToken},
		{"s3_region", cfg.S3Region},
	}
	for _, c := range credentials {
		if c.value == "" {
			continue
		}
		if err := duckdbinit.ValidateS3Credential(c.name, c.value); err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET %s='%s';", c.name, c.value), "set "+c.name))
	}
	if cfg.S3Endpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		if err := duckdbinit.ValidateS3Credential("s3_endpoint", ep); err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_endpoint='%s';", ep), "set s3_endpoint"))
	}
	sslVal := "true"
	if !cfg.S3UseSSL {
		sslVal = "false"
	}
	steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_use_ssl=%s;", sslVal), "set s3_use_ssl"))
	if cfg.S3UsePath {
		steps = append(steps, duckdbinit.SingleStmtStep("SET s3_url_style='path';", "set s3_url_style"))
	}
	return steps, nil
}
