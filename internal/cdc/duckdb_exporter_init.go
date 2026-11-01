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
//
// httpfs, parquet and the S3 session settings are required: every engine
// built here reads or writes S3 parquet, and each required extension is
// verified loaded after its INSTALL and LOAD. postgres_scanner is required only
// when requirePostgres is set, for the CDC exports that ATTACH PostgreSQL;
// the S3-only engines (compactor, baseline statistics, manifest audit) keep
// it optional, so a missing scanner never stops a tool that does not use it
// (#752). The resource PRAGMAs stay optional.
func buildExporterInitSteps(cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, requirePostgres bool) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	if cfg.DuckMemLimit != "" {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA memory_limit='%s';", cfg.DuckMemLimit), "set memory_limit"))
	}
	if cfg.DuckThreads > 0 {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA threads=%d;", cfg.DuckThreads), "set threads"))
	}
	// postgres_scanner first for postgres_query. The required form verifies
	// that DuckDB reports the scanner loaded: INSTALL and LOAD succeeding is
	// not proof of the capability, and a failed INSTALL of a scanner that is
	// already loaded is no reason to fail the connection.
	pgStep := duckdbinit.ExtensionStep("postgres_scanner")
	if requirePostgres {
		pgStep = duckdbinit.RequiredExtensionStep("postgres_scanner")
	}
	steps = append(steps, pgStep,
		duckdbinit.RequiredExtensionStep("httpfs"),
		duckdbinit.RequiredExtensionStep("parquet"))
	s3Steps, err := buildExporterS3Steps(cfg, s3AccessKey, s3Secret, s3SessionToken)
	if err != nil {
		return nil, fmt.Errorf("build cdc duckdb s3 statements: %w", err)
	}
	return append(steps, s3Steps...), nil
}

func buildExporterS3Steps(cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	credentials := []struct {
		name, value string
		secret      bool
	}{
		{"s3_access_key_id", s3AccessKey, true},
		{"s3_secret_access_key", s3Secret, true},
		// Temporary credentials (STS/assumed roles) are a key+secret+token
		// triple; without the token httpfs signs requests the store rejects
		// even though the SDK client on the same credentials works. The token
		// arrives from the caller alongside the pair, so it can only come from
		// the source that supplied that pair (#329) — resolving it here
		// independently is exactly the cross-source mix that fails opaquely.
		// See ResolveStaticS3Credentials.
		{"s3_session_token", s3SessionToken, true},
		{"s3_region", cfg.S3Region, false},
	}
	for _, c := range credentials {
		if c.value == "" {
			continue
		}
		step, err := duckdbinit.S3SettingStep(c.name, c.value, c.secret)
		if err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, step.AsRequired())
	}
	if cfg.S3Endpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		step, err := duckdbinit.S3SettingStep("s3_endpoint", ep, false)
		if err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, step.AsRequired())
	}
	sslVal := "true"
	if !cfg.S3UseSSL {
		sslVal = "false"
	}
	steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_use_ssl=%s;", sslVal), "set s3_use_ssl").AsRequired())
	if cfg.S3UsePath {
		steps = append(steps, duckdbinit.SingleStmtStep("SET s3_url_style='path';", "set s3_url_style").AsRequired())
	}
	return steps, nil
}
