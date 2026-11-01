// Package duckdbinit builds and applies the session-scoped initialization
// (INSTALL/LOAD/SET/PRAGMA) that every pooled DuckDB connection must run on
// open. Session-scoped statements issued through the pool reach only one
// arbitrary connection (issues #245, #285); the connector init hook returned
// by MakeConnInit runs them for each new physical connection instead. A step
// is optional (logged and skipped when it fails) or required (its failure
// fails the connection, #752).
//
// It also holds RecoveringConnector, which reopens a DuckDB database that
// DuckDB invalidated after a FATAL or INTERNAL error, running the same init
// hook on the reopened database's connections (#702, #717).
package duckdbinit

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

// DefaultInitTimeout bounds each init step of a new physical connection.
// It matches the 5s ping bound both constructors use, and the pre-#285
// exporter's construction-time init deadline. The bound is per step, not per
// hook run: under one shared deadline a single slow INSTALL (a network
// download on a cold cache) starved every later step, so the network-free
// SET s3_* statements were skipped and connections opened with half their
// session config silently missing (#487).
const DefaultInitTimeout = 5 * time.Second

// Stmt is a single statement executed on every new physical DuckDB connection.
// Secret, when set, is a value the SQL embeds (an S3 credential) that must
// never reach a log line or a returned error: DuckDB's error prose may quote
// the statement it failed on.
type Stmt struct {
	SQL    string
	Label  string
	Secret string
}

// Step groups statements that depend on each other: when one fails, the
// step's remaining statements are skipped (an extension whose INSTALL fails
// must not be LOADed), while later steps still run.
//
// A Required step is a capability the connection is useless without, such as
// the httpfs extension of an engine that reads S3. Its failure fails the
// connection instead of being logged and skipped, so database/sql reports it
// to whoever asked for the connection: the constructor's ping, or the query
// that made the pool open a new physical connection. A ping that succeeds on
// a connection missing httpfs proves nothing about S3 (#752).
//
// Verify, when set, is what decides a step's outcome: it runs after the
// statements whether or not they failed, and the step fails only if Verify
// does. A failed statement of a step whose Verify passes is logged like an
// optional step's. RequiredExtensionStep uses it, so a failed INSTALL of an
// extension that is statically linked or already installed is not mistaken
// for a missing capability, and a "successful" INSTALL never stands in for a
// check that the extension is actually loaded.
type Step struct {
	Stmts    []Stmt
	Required bool
	Verify   *Stmt
}

// SingleStmtStep wraps one independent statement in its own step.
func SingleStmtStep(sql, label string) Step {
	return Step{Stmts: []Stmt{{SQL: sql, Label: label}}}
}

// ExtensionStep pairs an extension's INSTALL and LOAD into one step, so a
// failed INSTALL skips that extension's LOAD.
func ExtensionStep(ext string) Step {
	return Step{Stmts: []Stmt{
		{SQL: fmt.Sprintf("INSTALL %s;", ext), Label: "install " + ext},
		{SQL: fmt.Sprintf("LOAD %s;", ext), Label: "load " + ext},
	}}
}

// RequiredExtensionStep is ExtensionStep for an extension the connection
// cannot do without: after INSTALL and LOAD it verifies that DuckDB reports
// the extension loaded, and fails the connection when it does not.
// DuckDB 2.0 links parquet statically, yet INSTALL parquet still downloads
// it, so a cold extension cache on a slow network can fail that INSTALL on
// a connection where parquet works.
func RequiredExtensionStep(ext string) Step {
	step := ExtensionStep(ext)
	step.Required = true
	step.Verify = &Stmt{
		SQL: fmt.Sprintf(
			"SELECT CASE WHEN EXISTS (SELECT 1 FROM duckdb_extensions() WHERE extension_name = '%[1]s' AND loaded) "+
				"THEN 1 ELSE error('extension %[1]s is not loaded') END;", strings.ReplaceAll(ext, "'", "''")),
		Label: "verify " + ext + " loaded",
	}
	return step
}

// AsRequired returns a copy of the step marked Required.
func (s Step) AsRequired() Step {
	s.Required = true
	return s
}

// S3SettingStep validates value with ValidateS3Credential and builds the
// SET name='value' step for it. secret marks a value that must never be
// logged (key id, secret key, session token), as opposed to the region or
// endpoint an operator needs to see in an error.
func S3SettingStep(name, value string, secret bool) (Step, error) {
	if err := ValidateS3Credential(name, value); err != nil {
		return Step{}, err
	}
	stmt := Stmt{SQL: fmt.Sprintf("SET %s='%s';", name, value), Label: "set " + name}
	if secret {
		stmt.Secret = value
	}
	return Step{Stmts: []Stmt{stmt}}, nil
}

// RequiredStepError reports a Required init step that failed on a new
// physical connection. Its message never carries the step's Secret.
type RequiredStepError struct {
	Label string
	Err   error
}

func (e *RequiredStepError) Error() string {
	return fmt.Sprintf("duckdb: required connection init step %q failed: %v", e.Label, e.Err)
}

func (e *RequiredStepError) Unwrap() error { return e.Err }

// ValidateS3Credential checks that an S3 credential value is safe to embed in
// a DuckDB SET statement. DuckDB's PRAGMA/SET does not support parameterized
// queries, so the value is checked against a denylist of characters instead.
// Rejected characters: single-quote ('), double-quote ("), semicolon (;),
// backslash (\), and space.
func ValidateS3Credential(name, value string) error {
	const forbidden = `'";\ `
	for _, ch := range forbidden {
		if strings.ContainsRune(value, ch) {
			return fmt.Errorf("S3 credential %q contains forbidden character %q; DuckDB SET does not support parameterized queries", name, string(ch))
		}
	}
	return nil
}

// MakeConnInit returns the connector init hook the driver runs for every new
// physical connection. A failed optional step is logged and skipped, so a
// degraded optional init never blocks the connection. A failed Required step
// fails the hook with a *RequiredStepError, which makes the driver close the
// connection and database/sql return the error; the steps after it do not
// run, since the connection is discarded anyway.
//
// Each step runs under its own stepTimeout deadline: driver cancellation
// rides on the context passed to ExecContext, so an unbounded context would
// let a stalled INSTALL/LOAD block connection establishment (and thereby
// constructors) indefinitely. The deadline is deliberately not shared across
// steps — one slow INSTALL must degrade only its own step, never starve the
// later, network-free SET statements into being skipped (#487). A step's
// Verify gets a deadline of its own for the same reason. The hook run stays
// bounded at 2 * len(steps) * stepTimeout. A non-positive stepTimeout
// disables the bound.
func MakeConnInit(steps []Step, log *zap.SugaredLogger, stepTimeout time.Duration) func(driver.ExecerContext) error {
	return func(execer driver.ExecerContext) error {
		for _, step := range steps {
			label, err := runStmts(execer, step.Stmts, stepTimeout)
			if step.Verify != nil {
				if _, verr := runStmts(execer, []Stmt{*step.Verify}, stepTimeout); verr != nil {
					label, err = step.Verify.Label, verifyError(verr, label, err)
				} else if err != nil {
					log.Warnw("duckdb: connection init step failed; the capability it provides is present anyway",
						"step", label, "err", err)
					continue
				}
			}
			if err == nil {
				continue
			}
			if step.Required {
				return &RequiredStepError{Label: label, Err: err}
			}
			log.Warnw("duckdb: connection init step failed", "step", label, "err", err)
		}
		return nil
	}
}

// verifyError reports a failed Verify, carrying the statement failure that
// most likely caused it (a failed INSTALL names the download error).
func verifyError(verr error, stmtLabel string, stmtErr error) error {
	if stmtErr == nil {
		return verr
	}
	return fmt.Errorf("%w (after %s failed: %v)", verr, stmtLabel, stmtErr)
}

// runStmts executes stmts under a fresh stepTimeout deadline and stops at the
// first failure, returning the failed statement's label and its error with
// the statement's secret scrubbed out.
func runStmts(execer driver.ExecerContext, stmts []Stmt, stepTimeout time.Duration) (string, error) {
	ctx := context.Background()
	if stepTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stepTimeout)
		defer cancel()
	}
	for _, s := range stmts {
		// Statements are literal SQL, no NamedValue args.
		if _, err := execer.ExecContext(ctx, s.SQL, nil); err != nil {
			return s.Label, scrubSecret(err, s.Secret)
		}
	}
	return "", nil
}

// scrubSecret returns err unchanged unless its message quotes secret, in
// which case it returns an error with every occurrence replaced. The
// replacement drops the wrapped chain on purpose: a caller unwrapping it
// would reach the unscrubbed message again. A DuckDB error keeps its type,
// so the recovering connector still recognizes a FATAL one.
func scrubSecret(err error, secret string) error {
	if secret == "" || !strings.Contains(err.Error(), secret) {
		return err
	}
	msg := strings.ReplaceAll(err.Error(), secret, "[REDACTED]")
	var duckErr *duckdb.Error
	if errors.As(err, &duckErr) {
		return &duckdb.Error{Type: duckErr.Type, Msg: msg}
	}
	return errors.New(msg)
}
