package duckdbinit_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

// stepBudgetExecer blocks the statement matching blockOn until its context
// expires and records the context error every statement observes on entry.
type stepBudgetExecer struct {
	blockOn string
	ctxErrs map[string]error
}

func (e *stepBudgetExecer) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	e.ctxErrs[query] = ctx.Err()
	if query == e.blockOn {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return driver.RowsAffected(0), nil
}

// Issue #487: a single slow INSTALL must not starve the steps after it. Under
// one shared deadline, an INSTALL that eats the whole budget hands an
// already-expired context to every later step — including the network-free
// SET s3_* statements — so a slow extension download silently zeroes the S3
// session config. Each step must get its own deadline instead.
func TestMakeConnInit_SlowStepDoesNotStarveLaterSteps(t *testing.T) {
	steps := []duckdbinit.Step{
		duckdbinit.ExtensionStep("httpfs"),
		duckdbinit.SingleStmtStep("SET s3_region='us-test-1';", "set s3_region"),
	}
	execer := &stepBudgetExecer{blockOn: "INSTALL httpfs;", ctxErrs: make(map[string]error)}
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), 50*time.Millisecond)(execer))

	require.Contains(t, execer.ctxErrs, "SET s3_region='us-test-1';", "later steps must still run after a timed-out step")
	require.NoError(t, execer.ctxErrs["SET s3_region='us-test-1';"],
		"a step after a timed-out step must run under its own live deadline, not the expired remnant of a shared one (#487)")
	require.NotContains(t, execer.ctxErrs, "LOAD httpfs;", "a timed-out INSTALL must still skip its own LOAD")
}

func TestValidateS3Credential(t *testing.T) {
	for _, bad := range []string{"a'b", `a"b`, "a;b", `a\b`, "a b"} {
		require.Error(t, duckdbinit.ValidateS3Credential("s3_region", bad), "value %q must be rejected", bad)
	}
	require.NoError(t, duckdbinit.ValidateS3Credential("s3_region", "us-east-1"))
}

// A failed Required step fails the hook, so the driver discards the
// connection and database/sql reports the failure, instead of logging it and
// handing out a connection that lacks the capability (#752). The steps after
// it do not run: the connection is closed anyway.
func TestMakeConnInit_RequiredStepFailureFailsTheConnection(t *testing.T) {
	steps := []duckdbinit.Step{
		duckdbinit.SingleStmtStep("PRAGMA threads=2;", "set threads"),
		duckdbinit.SingleStmtStep("SET s3_endpoint='127.0.0.1:9000';", "set s3_endpoint").AsRequired(),
		duckdbinit.SingleStmtStep("SET s3_region='us-test-1';", "set s3_region"),
	}
	execer := &multiFailExecer{failOn: map[string]bool{"SET s3_endpoint='127.0.0.1:9000';": true}}
	err := duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(execer)

	var stepErr *duckdbinit.RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "set s3_endpoint", stepErr.Label)
	require.ErrorContains(t, err, "injected init failure")
	require.NotContains(t, execer.executed, "SET s3_region='us-test-1';",
		"no step runs after a failed required step")
}

// multiFailExecer records every attempted statement and fails each one in
// failOn; a statement starting with "SELECT CASE WHEN EXISTS" is a
// RequiredExtensionStep's Verify, which fails when failVerify is set.
type multiFailExecer struct {
	executed   []string
	failOn     map[string]bool
	failVerify bool
	verifyCtx  error
}

func (m *multiFailExecer) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	m.executed = append(m.executed, query)
	if strings.HasPrefix(query, "SELECT CASE WHEN EXISTS") {
		m.verifyCtx = ctx.Err()
		if m.failVerify {
			return nil, errors.New("Invalid Input Error: extension is not loaded")
		}
		return driver.RowsAffected(0), nil
	}
	if m.failOn[query] {
		return nil, errors.New("injected init failure")
	}
	return driver.RowsAffected(0), nil
}

// A required extension is judged by whether DuckDB reports it loaded, not by
// its INSTALL: parquet is statically linked in DuckDB 2.0, yet INSTALL parquet
// downloads it, so that INSTALL failing on a cold cache must not fail the
// connection while the extension works (#752).
func TestMakeConnInit_RequiredExtensionPassesWhenLoadedDespiteFailedInstall(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	execer := &multiFailExecer{failOn: map[string]bool{"INSTALL parquet;": true}}
	err := duckdbinit.MakeConnInit([]duckdbinit.Step{duckdbinit.RequiredExtensionStep("parquet")},
		zap.New(core).Sugar(), duckdbinit.DefaultInitTimeout)(execer)
	require.NoError(t, err)
	require.NotContains(t, execer.executed, "LOAD parquet;", "a failed INSTALL still skips its LOAD")
	require.Equal(t, 1, logs.FilterMessageSnippet("present anyway").Len(), "the failed INSTALL is still logged")
}

// A required extension that is not loaded after INSTALL and LOAD fails the
// connection, and the error names the INSTALL failure that caused it.
func TestMakeConnInit_RequiredExtensionNotLoadedFailsTheConnection(t *testing.T) {
	execer := &multiFailExecer{failOn: map[string]bool{"INSTALL httpfs;": true}, failVerify: true}
	err := duckdbinit.MakeConnInit([]duckdbinit.Step{duckdbinit.RequiredExtensionStep("httpfs")},
		zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(execer)

	var stepErr *duckdbinit.RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "verify httpfs loaded", stepErr.Label)
	require.ErrorContains(t, err, "not loaded")
	require.ErrorContains(t, err, "install httpfs failed: injected init failure")
}

// The Verify of a required extension whose INSTALL used up the step deadline
// runs under a live deadline of its own, so the outcome reflects whether the
// extension is loaded, not the expired INSTALL budget.
func TestMakeConnInit_RequiredExtensionVerifiesAfterInstallTimeout(t *testing.T) {
	steps := []duckdbinit.Step{duckdbinit.RequiredExtensionStep("httpfs")}
	execer := &stepBudgetExecer{blockOn: "INSTALL httpfs;", ctxErrs: make(map[string]error)}
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), 50*time.Millisecond)(execer))
	var verify string
	for q := range execer.ctxErrs {
		if strings.HasPrefix(q, "SELECT CASE WHEN EXISTS") {
			verify = q
		}
	}
	require.NotEmpty(t, verify, "the verify statement must run after a timed-out INSTALL")
	require.NoError(t, execer.ctxErrs[verify], "the verify must run under its own live deadline")
	require.NotContains(t, execer.ctxErrs, "LOAD httpfs;")
}

// The verify statement really fails on DuckDB when the extension is absent
// and passes when it is loaded.
func TestRequiredExtensionStep_VerifyAgainstRealDuckDB(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	parquet := duckdbinit.RequiredExtensionStep("parquet")
	_, err = db.ExecContext(ctx, parquet.Verify.SQL)
	require.NoError(t, err, "parquet is statically linked and loaded")

	missing := duckdbinit.RequiredExtensionStep("forma_missing_ext_752")
	_, err = db.ExecContext(ctx, missing.Verify.SQL)
	require.ErrorContains(t, err, "extension forma_missing_ext_752 is not loaded")
}

// echoingExecer fails every statement with an error that quotes it back,
// the way DuckDB's parser and binder errors do.
type echoingExecer struct{}

func (echoingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("Parser Error: near " + query)
}

// An S3 credential never reaches the returned error or the log, even when
// DuckDB's error quotes the SET statement that embeds it (#752).
func TestMakeConnInit_FailedSecretSettingIsScrubbed(t *testing.T) {
	const secret = "s3cr3tvalue752"
	step, err := duckdbinit.S3SettingStep("s3_secret_access_key", secret, true)
	require.NoError(t, err)

	core, logs := observer.New(zap.DebugLevel)
	optionalErr := duckdbinit.MakeConnInit([]duckdbinit.Step{step}, zap.New(core).Sugar(), duckdbinit.DefaultInitTimeout)(echoingExecer{})
	require.NoError(t, optionalErr, "an optional setting is logged and skipped")
	require.Equal(t, 1, logs.Len())
	require.NotContains(t, logs.All()[0].ContextMap()["err"], secret)

	requiredErr := duckdbinit.MakeConnInit([]duckdbinit.Step{step.AsRequired()}, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(echoingExecer{})
	require.Error(t, requiredErr)
	require.NotContains(t, requiredErr.Error(), secret)
	require.Contains(t, requiredErr.Error(), "[REDACTED]")

	// The region is not a secret: an operator needs it in the error.
	region, err := duckdbinit.S3SettingStep("s3_region", "us-test-1", false)
	require.NoError(t, err)
	regionErr := duckdbinit.MakeConnInit([]duckdbinit.Step{region.AsRequired()}, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(echoingExecer{})
	require.ErrorContains(t, regionErr, "us-test-1")
}

func TestS3SettingStep_RejectsForbiddenCharacters(t *testing.T) {
	_, err := duckdbinit.S3SettingStep("s3_secret_access_key", "bad'key", true)
	require.ErrorContains(t, err, "forbidden character")
}

type fatalEchoingExecer struct{}

func (fatalEchoingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	return nil, &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: database has been invalidated near " + query}
}

// Scrubbing a secret out of a DuckDB error keeps the error's type, so the
// recovering connector still sees that a required step failed FATAL.
func TestMakeConnInit_ScrubbedRequiredStepKeepsFatalType(t *testing.T) {
	const secret = "s3cr3tvalue752"
	step, err := duckdbinit.S3SettingStep("s3_secret_access_key", secret, true)
	require.NoError(t, err)

	requiredErr := duckdbinit.MakeConnInit([]duckdbinit.Step{step.AsRequired()}, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(fatalEchoingExecer{})
	require.NotContains(t, requiredErr.Error(), secret)
	var duckErr *duckdb.Error
	require.ErrorAs(t, requiredErr, &duckErr)
	require.Equal(t, duckdb.ErrorTypeFatal, duckErr.Type)
	require.NotContains(t, duckErr.Msg, secret)
}
