package duckdbinit_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// recordingExecer records every attempted statement and fails the one
// matching failOn.
type recordingExecer struct {
	executed []string
	failOn   string
}

func (r *recordingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	r.executed = append(r.executed, query)
	if query == r.failOn {
		return nil, errors.New("injected init failure")
	}
	return driver.RowsAffected(0), nil
}

func TestMakeConnInit_FailedStmtSkipsRestOfStepOnly(t *testing.T) {
	steps := []duckdbinit.Step{
		duckdbinit.ExtensionStep("bad_ext"),
		duckdbinit.SingleStmtStep("SET s3_region='us-test-1';", "set s3_region"),
	}
	execer := &recordingExecer{failOn: "INSTALL bad_ext;"}
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar())(execer))

	require.Contains(t, execer.executed, "INSTALL bad_ext;")
	require.NotContains(t, execer.executed, "LOAD bad_ext;", "failed INSTALL must skip the LOAD in the same step")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';", "later steps must still run")
}

func TestValidateS3Credential(t *testing.T) {
	for _, bad := range []string{"a'b", `a"b`, "a;b", `a\b`, "a b"} {
		require.Error(t, duckdbinit.ValidateS3Credential("s3_region", bad), "value %q must be rejected", bad)
	}
	require.NoError(t, duckdbinit.ValidateS3Credential("s3_region", "us-east-1"))
}
