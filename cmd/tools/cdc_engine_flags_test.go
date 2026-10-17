package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// TestPostgresFlagsCDCEngine pins how the CDC tools resolve their engine,
// which picks their schema lock (#633): --pg-use-iam reaches only Aurora
// DSQL, so it implies aurora-dsql and refuses any other engine beside it.
func TestPostgresFlagsCDCEngine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flags   postgresFlags
		want    forma.DatabaseEngine
		wantErr string
	}{
		{name: "default is postgresql", want: forma.EnginePostgreSQL},
		{name: "explicit postgresql", flags: postgresFlags{engine: "postgresql"}, want: forma.EnginePostgreSQL},
		{name: "explicit aurora-dsql", flags: postgresFlags{engine: "aurora-dsql"}, want: forma.EngineAuroraDSQL},
		{name: "iam implies aurora-dsql", flags: postgresFlags{useIAM: true}, want: forma.EngineAuroraDSQL},
		{name: "iam with aurora-dsql", flags: postgresFlags{useIAM: true, engine: "aurora-dsql"}, want: forma.EngineAuroraDSQL},
		{name: "iam with postgresql", flags: postgresFlags{useIAM: true, engine: "postgresql"},
			wantErr: `--pg-use-iam authenticates to Aurora DSQL, but --pg-engine is "postgresql"`},
		{name: "unknown engine", flags: postgresFlags{engine: "dsql"}, wantErr: "validate --pg-engine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.flags.cdcEngine("pg-engine")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("cdcEngine: %v", err)
			}
			if got != tc.want {
				t.Errorf("engine = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCDCToolsWirePGEngine keeps --pg-engine wired to each holder of the
// schema lock: a tool left on the default would lock differently from the
// others on DSQL and exclude none of them.
func TestCDCToolsWirePGEngine(t *testing.T) {
	flush, err := parseCDCFlushFlags([]string{"-s3-bucket=b", "-pg-engine=aurora-dsql"})
	if err != nil {
		t.Fatalf("parse cdc-flush flags: %v", err)
	}
	if flush.cfg.Engine != forma.EngineAuroraDSQL {
		t.Errorf("cdc-flush Engine = %q, want aurora-dsql", flush.cfg.Engine)
	}
	iam, err := parseCDCFlushFlags([]string{"-s3-bucket=b", "-pg-use-iam"})
	if err != nil {
		t.Fatalf("parse cdc-flush flags: %v", err)
	}
	if iam.cfg.Engine != forma.EngineAuroraDSQL {
		t.Errorf("cdc-flush --pg-use-iam Engine = %q, want aurora-dsql", iam.cfg.Engine)
	}

	initOpts, err := parseCDCInitFlags([]string{"-s3-bucket=b", "-schema-registry-table=t", "-schema-dir=/d", "-pg-engine=aurora-dsql"})
	if err != nil {
		t.Fatalf("parse cdc-init flags: %v", err)
	}
	if initOpts.cfg.Engine != forma.EngineAuroraDSQL {
		t.Errorf("cdc-init Engine = %q, want aurora-dsql", initOpts.cfg.Engine)
	}

	reconcile, err := parseReconcileFlags([]string{"--s3-bucket", "b", "--schema-registry-table", "t", "--pg-engine", "aurora-dsql"})
	if err != nil {
		t.Fatalf("parse manifest-reconcile flags: %v", err)
	}
	if reconcile.engine != forma.EngineAuroraDSQL {
		t.Errorf("manifest-reconcile engine = %q, want aurora-dsql", reconcile.engine)
	}

	_, err = parseCDCFlushFlags([]string{"-s3-bucket=b", "-pg-engine=oracle"})
	var cfgErr *forma.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Errorf("an unknown engine must fail as a ConfigError, got %v", err)
	}
}
