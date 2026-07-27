package duckdbinit_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

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
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(execer))

	require.Contains(t, execer.executed, "INSTALL bad_ext;")
	require.NotContains(t, execer.executed, "LOAD bad_ext;", "failed INSTALL must skip the LOAD in the same step")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';", "later steps must still run")
}

// blockingExecer blocks the statement matching blockOn until its context is
// canceled, then returns the context error — the shape of an INSTALL stalling
// on a dead extension repository.
type blockingExecer struct {
	executed    []string
	blockOn     string
	sawDeadline bool
}

func (b *blockingExecer) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	b.executed = append(b.executed, query)
	if query == b.blockOn {
		_, b.sawDeadline = ctx.Deadline()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return driver.RowsAffected(0), nil
}

// Driver cancellation rides on the context passed to ExecContext, so init
// statements must run under a bounded context — otherwise a stalled INSTALL
// blocks Connect (and thereby constructors) indefinitely (PR #328 review).
func TestMakeConnInit_BlockedStmtObservesCancellation(t *testing.T) {
	steps := []duckdbinit.Step{
		duckdbinit.SingleStmtStep("INSTALL httpfs;", "install httpfs"),
		duckdbinit.SingleStmtStep("SET s3_region='us-test-1';", "set s3_region"),
	}
	execer := &blockingExecer{blockOn: "INSTALL httpfs;"}

	done := make(chan error, 1)
	go func() {
		done <- duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), 50*time.Millisecond)(execer)
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "a timed-out init statement must be skipped, not block the connection")
	case <-time.After(5 * time.Second):
		t.Fatal("connection init hook did not observe cancellation of a blocked statement")
	}

	require.True(t, execer.sawDeadline, "init statements must run under a deadline-bound context")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';", "later steps must still be attempted after a timed-out step")
}

func TestValidateS3Credential(t *testing.T) {
	for _, bad := range []string{"a'b", `a"b`, "a;b", `a\b`, "a b"} {
		require.Error(t, duckdbinit.ValidateS3Credential("s3_region", bad), "value %q must be rejected", bad)
	}
	require.NoError(t, duckdbinit.ValidateS3Credential("s3_region", "us-east-1"))
}
