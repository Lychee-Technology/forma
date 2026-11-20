package main

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"go.uber.org/zap"
)

// stubToolAWSChainLoader replaces the shared AWS config loader seam with a
// recorder. The stub returns a *usable* static provider rather than a bare
// aws.Config: a nil Credentials provider would make any caller that reaches
// Retrieve panic, which would mask the assertion these tests actually make.
func stubToolAWSChainLoader(t *testing.T, called *bool) {
	t.Helper()
	old := toolLoadAWSConfigFn
	t.Cleanup(func() { toolLoadAWSConfigFn = old })
	toolLoadAWSConfigFn = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		*called = true
		return aws.Config{
			Region:      "us-east-1",
			Credentials: awscreds.NewStaticCredentialsProvider("chain-key", "chain-secret", "chain-token"),
		}, nil
	}
}

// setStaticS3Env supplies the environment triple that
// cdc.ResolveStaticS3Credentials is expected to read, so the engines have a
// credential source that does not involve the default chain.
func setStaticS3Env(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
}

// TestParseCompactorFlags_InputChecksumVerify pins the flag→config plumbing of
// the #347 escape hatch. The gate is fail-closed by default, so the default
// case is the load-bearing half: deleting the SkipInputChecksumVerify field
// from the CompactionConfig literal in parseCompactorFlags turns the opt-in
// case red, and flipping the flag's default to true turns the default case red.
func TestParseCompactorFlags_InputChecksumVerify(t *testing.T) {
	base := []string{"--schema-id", "7", "--s3-bucket", "bkt"}

	opts, err := parseCompactorFlags(base)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.compact.SkipInputChecksumVerify {
		t.Fatal("SkipInputChecksumVerify must default to false: the #347 pre-merge gate verifies unless opted out")
	}

	opts, err = parseCompactorFlags(append(append([]string{}, base...), "--skip-input-checksum-verify"))
	if err != nil {
		t.Fatalf("parse with --skip-input-checksum-verify: %v", err)
	}
	if !opts.compact.SkipInputChecksumVerify {
		t.Fatal("--skip-input-checksum-verify must reach CompactionConfig.SkipInputChecksumVerify")
	}
	// WithDefaults() runs on that literal; a future zero-value normalization of
	// the bool would silently re-arm the gate an operator asked to bypass.
	if opts.compact.MaxRetries == 0 {
		t.Fatal("WithDefaults did not run; the assertion above no longer covers the post-defaults value")
	}
}

// TestOpenMergeEngine_DoesNotConsultCredentialChain pins the deliberate
// narrowing of #329: the merge engine resolves credentials through the shared
// cdc.ResolveStaticS3Credentials rule (explicit config, else the environment
// pair) and never through the AWS default credential chain. Chain-only
// sources — shared profiles, assumed roles, web identity, IMDS — are
// intentionally disconnected from the DuckDB httpfs session; see the WARNING
// on openMergeEngine.
func TestOpenMergeEngine_DoesNotConsultCredentialChain(t *testing.T) {
	called := false
	stubToolAWSChainLoader(t, &called)
	setStaticS3Env(t)

	opts := &compactorOptions{s3: s3Flags{region: "us-east-1"}}
	exporter, err := openMergeEngine(context.Background(), opts, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	if called {
		t.Fatal("merge engine must not load the AWS default credential chain (#329)")
	}
}

// TestOpenMergeEngine_EnvCredentialsReachEngineValidation is the positive
// counterpart to the chain assertions above: those alone stay green if the
// cdc.ResolveStaticS3Credentials call is deleted and empty strings are passed
// through. Here the environment pair carries an access key containing a space,
// which duckdbinit.ValidateS3Credential rejects while building the httpfs SET
// statements — so a non-nil construction error is only possible if the
// resolved environment credentials genuinely reach that builder.
func TestOpenMergeEngine_EnvCredentialsReachEngineValidation(t *testing.T) {
	called := false
	stubToolAWSChainLoader(t, &called)
	t.Setenv("AWS_ACCESS_KEY_ID", "bad key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	opts := &compactorOptions{s3: s3Flags{region: "us-east-1"}}
	exporter, err := openMergeEngine(context.Background(), opts, zap.NewNop())
	if err == nil {
		_ = exporter.DB.Close()
		t.Fatal("expected the env access key to reach httpfs credential validation and be rejected (#329)")
	}
	if exporter != nil {
		t.Fatal("expected a nil exporter alongside the construction error")
	}
	if called {
		t.Fatal("merge engine must not load the AWS default credential chain (#329)")
	}
}

// The V1 compactor's manifest and backoff flags are gone with no
// compatibility path (#890): the compactor reads the schema's publication
// record and publishes an immutable manifest under the -data-prefix layout,
// and a lost compare-and-set rebases or restarts from the head rather than
// backing off. The parser refuses each by name, so none is silently ignored.
func TestParseCompactorFlagsRefusesTheRemovedV1Flags(t *testing.T) {
	for _, arg := range []string{"-manifest-template=manifest/{{.SchemaID}}.json", "-manifest-prefix=m", "-base-backoff-ms=1", "-max-backoff-ms=1"} {
		_, err := parseCompactorFlags([]string{"--schema-id", "7", "--s3-bucket", "bkt", arg})
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Errorf("compactor %s: err = %v, want the parser's refusal of an undefined flag", arg, err)
		}
	}
}

// The compactor publishes through the schema's record, so it takes the
// database flags cdc-flush takes, and its engine rule.
func TestParseCompactorFlagsMapsThePublicationDatabase(t *testing.T) {
	opts, err := parseCompactorFlags([]string{"--schema-id", "7", "--s3-bucket", "bkt", "--pg-host", "db", "--pg-port", "6543",
		"--pg-user", "u", "--pg-db", "d", "--pg-ssl-mode", "disable", "--pg-engine", "aurora-dsql", "--entity-main-table", "em", "--data-prefix", "lake"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.db.PGHost != "db" || opts.db.PGPort != 6543 || opts.db.PGUser != "u" || opts.db.PGDB != "d" || opts.db.PGSSLMode != "disable" ||
		opts.db.Engine != "aurora-dsql" || opts.entityMain != "em" || opts.dataPrefix != "lake" {
		// Only the checked fields: the database config also holds the
		// resolved password, which the parser may inherit from PGPASSWORD.
		t.Fatalf("parsed host %q port %d user %q db %q ssl mode %q engine %q, entity main %q, data prefix %q",
			opts.db.PGHost, opts.db.PGPort, opts.db.PGUser, opts.db.PGDB, opts.db.PGSSLMode, opts.db.Engine, opts.entityMain, opts.dataPrefix)
	}
	if _, err := parseCompactorFlags([]string{"--schema-id", "7", "--s3-bucket", "bkt", "--pg-engine", "oracle"}); err == nil {
		t.Fatal("an unknown --pg-engine must be refused")
	}
}
