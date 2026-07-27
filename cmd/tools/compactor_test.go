package main

import (
	"context"
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

// TestOpenReconcileStatsEngine_DoesNotConsultCredentialChain holds the
// reconcile stats engine to the same rule as the merge engine, so the two
// DuckDB constructors in cmd/tools cannot drift apart again.
func TestOpenReconcileStatsEngine_DoesNotConsultCredentialChain(t *testing.T) {
	called := false
	stubToolAWSChainLoader(t, &called)
	setStaticS3Env(t)

	opts := &reconcileOptions{s3: s3Flags{region: "us-east-1"}}
	exporter, err := openReconcileStatsEngine(context.Background(), opts, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	if called {
		t.Fatal("reconcile stats engine must not load the AWS default credential chain (#329)")
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
