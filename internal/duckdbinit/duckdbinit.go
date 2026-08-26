// Package duckdbinit builds and applies the session-scoped initialization
// (INSTALL/LOAD/SET/PRAGMA) that every pooled DuckDB connection must run on
// open. Session-scoped statements issued through the pool reach only one
// arbitrary connection (issues #245, #285); the connector init hook returned
// by MakeConnInit runs them for each new physical connection instead.
package duckdbinit

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

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
type Stmt struct {
	SQL   string
	Label string
}

// Step groups statements that depend on each other: when one fails, the
// step's remaining statements are skipped (an extension whose INSTALL fails
// must not be LOADed), while later steps still run.
type Step struct {
	Stmts []Stmt
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
// physical connection. Failed statements are logged and skipped so a degraded
// init never blocks the connection; construction-time errors are limited to
// the credential validation done by the statement builders.
//
// Each step runs under its own stepTimeout deadline: driver cancellation
// rides on the context passed to ExecContext, so an unbounded context would
// let a stalled INSTALL/LOAD block connection establishment (and thereby
// constructors) indefinitely. The deadline is deliberately not shared across
// steps — one slow INSTALL must degrade only its own step, never starve the
// later, network-free SET statements into being skipped (#487). The hook run
// stays bounded at len(steps) * stepTimeout. A non-positive stepTimeout
// disables the bound.
func MakeConnInit(steps []Step, log *zap.SugaredLogger, stepTimeout time.Duration) func(driver.ExecerContext) error {
	return func(execer driver.ExecerContext) error {
		for _, step := range steps {
			runStep(execer, step, log, stepTimeout)
		}
		return nil
	}
}

// runStep executes one step's statements under a fresh stepTimeout deadline,
// logging and skipping the step's remainder on the first failure.
func runStep(execer driver.ExecerContext, step Step, log *zap.SugaredLogger, stepTimeout time.Duration) {
	ctx := context.Background()
	if stepTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stepTimeout)
		defer cancel()
	}
	for _, s := range step.Stmts {
		// Statements are literal SQL, no NamedValue args.
		if _, err := execer.ExecContext(ctx, s.SQL, nil); err != nil {
			log.Warnw("duckdb: connection init step failed", "step", s.Label, "err", err)
			return
		}
	}
}
