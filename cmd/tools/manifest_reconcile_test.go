package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/reconcile"
)

// TestNewReconciler_WiresVerificationPasses pins the flag → Reconciler wiring
// the flag parser alone cannot prove: --verify-checksums must reach Opts AND
// the byte reader must be attached, or the scrub answers a configuration
// error at run time instead of scrubbing.
func TestNewReconciler_WiresVerificationPasses(t *testing.T) {
	client := &s3.Client{}
	logger := zap.NewNop()

	off := newReconciler(&reconcileOptions{}, client, nil, logger)
	if off.Opts.VerifyChecksums || off.Opts.VerifyStamps {
		t.Fatalf("verification must stay off when the flags are not set: %+v", off.Opts)
	}

	on := newReconciler(&reconcileOptions{
		verifyChecksums: true,
		verifyStamps:    true,
		etagRetries:     3,
	}, client, nil, logger)
	if !on.Opts.VerifyChecksums {
		t.Fatal("--verify-checksums must reach reconcile.Options")
	}
	if !on.Opts.VerifyStamps {
		t.Fatal("--verify-stamps must reach reconcile.Options")
	}
	if on.Objects == nil {
		t.Fatal("the scrub's byte reader must be wired, else verifyChecksums fails as misconfigured")
	}
}

func TestRunToolMain_ManifestReconcileExitCodes(t *testing.T) {
	old := runManifestReconcileFn
	defer func() { runManifestReconcileFn = old }()

	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"clean run", nil, 0},
		{"discrepancies found", &discrepancyError{count: 3}, 2},
		{"tool failure", errors.New("s3 unavailable"), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runManifestReconcileFn = func(ctx context.Context, args []string) error {
				return tt.err
			}
			var out bytes.Buffer
			code := runToolMain(context.Background(), []string{"manifest-reconcile"}, &out)
			if code != tt.wantCode {
				t.Fatalf("exit code = %d, want %d (output %q)", code, tt.wantCode, out.String())
			}
		})
	}
}

func TestRunToolMain_DiscrepancyExitPrintsNoErrorLine(t *testing.T) {
	old := runManifestReconcileFn
	defer func() { runManifestReconcileFn = old }()
	runManifestReconcileFn = func(ctx context.Context, args []string) error {
		return &discrepancyError{count: 2}
	}

	var out bytes.Buffer
	code := runToolMain(context.Background(), []string{"manifest-reconcile"}, &out)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if strings.Contains(out.String(), "manifest-reconcile:") {
		t.Fatalf("discrepancy exit must not print an error line (report already rendered), got %q", out.String())
	}
}

func TestReconcileExitError_ToolFailureBeatsDiscrepancy(t *testing.T) {
	// Per-schema tool failures (S3 outage, lock error) must exit 1, not be
	// folded into the exit-2 "discrepancies found" signal monitoring treats
	// as data inconsistency.
	failed := reconcile.Report{Schemas: []reconcile.SchemaReport{
		{SchemaID: 1, Err: errors.New("s3 unavailable")},
		{SchemaID: 2, Dangling: []string{"data/2/x.parquet"}},
	}}
	err := reconcileExitError(failed)
	if err == nil {
		t.Fatal("schema errors must surface as a tool failure")
	}
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		t.Fatalf("tool failure must be a plain error (exit 1), got exit code %d", ec.ExitCode())
	}

	residual := reconcile.Report{Schemas: []reconcile.SchemaReport{
		{SchemaID: 1, Dangling: []string{"data/1/x.parquet"}},
		{SchemaID: 2, Skipped: true},
	}}
	err = reconcileExitError(residual)
	if !errors.As(err, &ec) || ec.ExitCode() != 2 {
		t.Fatalf("residual discrepancies must exit 2, got %v", err)
	}

	if err := reconcileExitError(reconcile.Report{Schemas: []reconcile.SchemaReport{{SchemaID: 1}}}); err != nil {
		t.Fatalf("clean report must exit 0, got %v", err)
	}
}

func TestParseReconcileFlags_Defaults(t *testing.T) {
	opts, err := parseReconcileFlags([]string{
		"--s3-bucket", "bkt",
		"--schema-registry-table", "schema_registry",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.dataPrefix != "data" {
		t.Fatalf("dataPrefix = %q, want data", opts.dataPrefix)
	}
	if opts.manifest.PathTemplate != "manifest/{{.SchemaID}}.json" {
		t.Fatalf("manifest template = %q", opts.manifest.PathTemplate)
	}
	if opts.manifest.Bucket != "bkt" {
		t.Fatalf("manifest bucket = %q, want bkt", opts.manifest.Bucket)
	}
	if opts.gcGrace != 15*time.Minute {
		t.Fatalf("gcGrace = %v, want 15m", opts.gcGrace)
	}
	if opts.etagRetries != 3 {
		t.Fatalf("etagRetries = %d, want 3", opts.etagRetries)
	}
	if opts.repair || opts.gc || opts.verifyStamps || opts.verifyChecksums {
		t.Fatalf("repair/gc/verify-stamps/verify-checksums must default to false")
	}
	if opts.schemaID != 0 {
		t.Fatalf("schemaID = %d, want 0 (all schemas)", opts.schemaID)
	}
}

func TestParseReconcileFlags_RequiredAndModes(t *testing.T) {
	if _, err := parseReconcileFlags([]string{"--schema-registry-table", "t"}); err == nil {
		t.Fatal("missing --s3-bucket must error")
	}
	if _, err := parseReconcileFlags([]string{"--s3-bucket", "bkt"}); err == nil {
		t.Fatal("missing --schema-registry-table must error")
	}

	opts, err := parseReconcileFlags([]string{
		"--s3-bucket", "bkt",
		"--schema-registry-table", "t",
		"--schema-id", "7",
		"--repair", "--gc", "--gc-grace", "1m", "--verify-stamps", "--verify-checksums",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !opts.repair || !opts.gc || opts.gcGrace != time.Minute || opts.schemaID != 7 {
		t.Fatalf("mode flags not parsed: %+v", opts)
	}
	if !opts.verifyStamps {
		t.Fatalf("--verify-stamps not parsed: %+v", opts)
	}
	if !opts.verifyChecksums {
		t.Fatalf("--verify-checksums not parsed: %+v", opts)
	}

	opts, err = parseReconcileFlags([]string{"--help"})
	if err != nil {
		t.Fatalf("help must not error, got %v", err)
	}
	if opts != nil {
		t.Fatal("help must return nil options")
	}
}
