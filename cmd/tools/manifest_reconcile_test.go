package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/reconcile"
)

var reconcileBase = []string{"--s3-bucket", "bkt", "--schema-registry-table", "schema_registry"}

func reconcileArgs(extra ...string) []string {
	return append(append([]string{}, reconcileBase...), extra...)
}

func TestParseReconcileFlags_Defaults(t *testing.T) {
	opts, err := parseReconcileFlags(reconcileBase)
	require.NoError(t, err)
	require.Equal(t, "data", opts.dataPrefix)
	require.Equal(t, "entity_main", opts.entityMain)
	require.Zero(t, opts.schemaID, "0 visits every schema")
	require.Equal(t, reconcile.Options{
		RetentionGrace: 15 * time.Minute, ReaderDeadline: federated.DefaultPublishedPassDeadline,
		ClockMargin: time.Minute, OrphanAge: 2 * time.Hour,
	}, opts.run, "a run is the read-only diagnosis unless --gc is given")
	require.NoError(t, opts.run.Validate(), "the defaults keep a reader safe")
	require.GreaterOrEqual(t, opts.run.OrphanAge, cdcv2.DefaultStagingBudget+opts.run.ClockMargin)
}

func TestParseReconcileFlags_RequiredAndModes(t *testing.T) {
	_, err := parseReconcileFlags([]string{"--schema-registry-table", "t"})
	require.Error(t, err, "missing --s3-bucket")
	_, err = parseReconcileFlags([]string{"--s3-bucket", "bkt"})
	require.ErrorContains(t, err, "--schema-registry-table is required")

	opts, err := parseReconcileFlags(reconcileArgs("--schema-id", "7", "--gc", "--verify-checksums",
		"--retention-grace", "5m", "--clock-margin", "30s", "--orphan-age", "3h",
		"--entity-main-table", "em", "--data-prefix", "lake/", "--pg-engine", "aurora-dsql"))
	require.NoError(t, err)
	require.Equal(t, int16(7), opts.schemaID)
	require.True(t, opts.run.GC)
	require.True(t, opts.run.VerifyChecksums)
	require.Equal(t, [3]time.Duration{5 * time.Minute, 30 * time.Second, 3 * time.Hour},
		[3]time.Duration{opts.run.RetentionGrace, opts.run.ClockMargin, opts.run.OrphanAge})
	require.Equal(t, "em", opts.entityMain)
	require.Equal(t, "lake/", opts.dataPrefix)
	require.Equal(t, forma.EngineAuroraDSQL, opts.db.Engine)

	opts, err = parseReconcileFlags([]string{"--help"})
	require.NoError(t, err)
	require.Nil(t, opts, "help answers no options")
}

// TestParseReconcileFlags_RefusesUnsafeTimes: a grace that does not cover
// the reader's pass deadline plus the clock margin could delete an object a
// reader still scans; a zero clock margin trusts two clocks to agree; an
// orphan age below the writers' staging budget fences writers inside it.
func TestParseReconcileFlags_RefusesUnsafeTimes(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--retention-grace", "60s"}, "retention grace"},
		{[]string{"--retention-grace", "2m", "--clock-margin", "61s"}, "retention grace"},
		{[]string{"--clock-margin", "0s"}, "--clock-margin"},
		{[]string{"--orphan-age", "1h"}, "--orphan-age"},
		{[]string{"--schema-id", "40000"}, "--schema-id"},
	} {
		_, err := parseReconcileFlags(reconcileArgs(tc.args...))
		require.ErrorContains(t, err, tc.want, "%v", tc.args)
	}
	_, err := parseReconcileFlags(reconcileArgs("--retention-grace", "60s"))
	require.ErrorIs(t, err, reconcile.ErrRetentionUnsafe)
}

// TestManifestReconcileRefusesOverflowingTimesBeforeConnecting: a floor
// whose sum wraps a Duration would let a zero grace or orphan age pass. The
// flags are refused before any database connection opens.
func TestManifestReconcileRefusesOverflowingTimesBeforeConnecting(t *testing.T) {
	old := openReconcileDBFn
	defer func() { openReconcileDBFn = old }()
	openReconcileDBFn = func(context.Context, cdc.CDCConfig, *zap.Logger) (*sql.DB, error) {
		t.Fatal("the flags were accepted and a database connection was opened")
		return nil, nil
	}
	maxNS := fmt.Sprintf("%dns", int64(math.MaxInt64))
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--gc", "--retention-grace=0s", "--clock-margin=" + maxNS, "--orphan-age=1ns"}, "overflows"},
		// R_max + C fits, the staging budget + C does not.
		{[]string{"--gc", "--retention-grace=" + maxNS, "--clock-margin=" + fmt.Sprintf("%dns", int64(math.MaxInt64-2*time.Minute)), "--orphan-age=1ns"},
			"--clock-margin"},
	} {
		err := runManifestReconcile(context.Background(), reconcileArgs(tc.args...))
		require.ErrorContains(t, err, tc.want, "%v", tc.args)
	}
	_, err := parseReconcileFlags(reconcileArgs("--retention-grace=0s", "--clock-margin="+maxNS, "--orphan-age=1ns"))
	require.ErrorIs(t, err, reconcile.ErrRetentionUnsafe)
}

// TestParseReconcileFlags_RefusesRepair: under V2 there is nothing to adopt,
// and the recovery of a lost generation is a re-init.
func TestParseReconcileFlags_RefusesRepair(t *testing.T) {
	_, err := parseReconcileFlags(reconcileArgs("--repair"))
	require.ErrorIs(t, err, errRepairRemoved)
	require.ErrorContains(t, err, "cdc-init")
	for _, removed := range []string{"--manifest-template", "--manifest-prefix", "--allow-empty-manifest-schema", "--gc-grace", "--etag-retries"} {
		_, err := parseReconcileFlags(reconcileArgs(removed, "x"))
		require.ErrorContains(t, err, "flag provided but not defined", removed)
	}
	_, err = parseReconcileFlags(reconcileArgs("--verify-stamps"))
	require.ErrorContains(t, err, "flag provided but not defined")
}

// TestNewReconciler_WiresTheFlags pins the flag-to-Reconciler wiring: the
// tables and the lock named after --entity-main-table, the layout of
// --data-prefix, the schema filter and the options.
func TestNewReconciler_WiresTheFlags(t *testing.T) {
	opts, err := parseReconcileFlags(reconcileArgs("--schema-id", "9", "--gc", "--entity-main-table", "em", "--data-prefix", "lake"))
	require.NoError(t, err)
	db := &sql.DB{}
	objects := &reconcile.S3Objects{Bucket: "bkt"}
	r, err := newReconciler(opts, objects, db, zap.NewNop())
	require.NoError(t, err)
	require.Same(t, db, r.DB)
	require.Equal(t, publication.TablesFor("em"), r.Tables)
	require.Equal(t, manifestv2.Layout{Prefix: "lake"}, r.Layout)
	require.Same(t, objects, r.Objects)
	require.NotNil(t, r.Locker)
	require.Equal(t, &reconcile.Schemas{DB: db, Registry: "schema_registry", Tables: publication.TablesFor("em"), Only: 9}, r.Schemas)
	require.Equal(t, opts.run, r.Opts)
	require.True(t, r.Opts.GC)

	opts, err = parseReconcileFlags(reconcileArgs("--pg-engine", "aurora-dsql"))
	require.NoError(t, err)
	r, err = newReconciler(opts, objects, db, zap.NewNop())
	require.NoError(t, err)
	require.Nil(t, r.Locker, "the diagnosis takes no lock: on Aurora DSQL it is a lease row write")
}

func TestRunToolMain_ManifestReconcileExitCodes(t *testing.T) {
	old := runManifestReconcileFn
	defer func() { runManifestReconcileFn = old }()
	for _, tt := range []struct {
		name     string
		err      error
		wantCode int
	}{
		{"clean run", nil, 0},
		{"discrepancies found", &discrepancyError{count: 3}, 2},
		{"tool failure", errors.New("s3 unavailable"), 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runManifestReconcileFn = func(ctx context.Context, args []string) error { return tt.err }
			var out bytes.Buffer
			code := runToolMain(context.Background(), []string{"manifest-reconcile"}, &out)
			require.Equal(t, tt.wantCode, code, out.String())
			if tt.wantCode == 2 {
				require.NotContains(t, out.String(), "manifest-reconcile:", "the report is already rendered")
			}
		})
	}
}

// TestReconcileExitError_ToolFailureBeatsDiscrepancy: a per-schema tool
// failure (S3 outage, lock error) exits 1, never folded into the exit-2
// signal monitoring reads as an inconsistency.
func TestReconcileExitError_ToolFailureBeatsDiscrepancy(t *testing.T) {
	finding := []reconcile.Finding{{Kind: reconcile.LostObject}}
	err := reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{
		{SchemaID: 1, Err: errors.New("s3 unavailable")},
		{SchemaID: 2, Findings: finding},
	}})
	require.Error(t, err)
	var ec interface{ ExitCode() int }
	require.False(t, errors.As(err, &ec), "a tool failure is a plain error")
	require.True(t, strings.Contains(err.Error(), "schema 1"))

	err = reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{
		{SchemaID: 1, Findings: finding}, {SchemaID: 2, Skipped: true},
	}})
	require.True(t, errors.As(err, &ec))
	require.Equal(t, 2, ec.ExitCode())

	// A diagnosis that left findings out because their generation expired
	// beside it established neither loss nor collection: it is not clean.
	inconclusive := reconcile.SchemaReport{SchemaID: 1, ConcurrentlyExpired: 2, Refused: "run the diagnosis again"}
	err = reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{inconclusive}})
	require.True(t, errors.As(err, &ec), "an inconclusive diagnosis is a discrepancy, not a tool failure")
	require.Equal(t, 2, ec.ExitCode())

	// A refused collection without findings (e.g. no-verified-manifest guard or
	// active recovery fence) must also exit 2 rather than appearing clean.
	refused := reconcile.SchemaReport{SchemaID: 1, Refused: "no manifest of schema 1's generation history verified in the bucket"}
	err = reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{refused}})
	require.True(t, errors.As(err, &ec), "a refused collection without findings is a discrepancy, not clean")
	require.Equal(t, 2, ec.ExitCode())

	clean := reconcile.SchemaReport{SchemaID: 1, Plan: publication.Plan{Objects: []string{"garbage"}}}
	require.NoError(t, reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{clean}}),
		"garbage awaiting collection is not a discrepancy")
}
