package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
)

// stubLoadAWSConfig swaps the package-level loadAWSConfig seam for the duration
// of a single test. Tests using it must NOT call t.Parallel(): the seam is
// process-global state.
func stubLoadAWSConfig(
	t *testing.T,
	fn func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error),
) {
	t.Helper()
	previous := loadAWSConfig
	loadAWSConfig = fn
	t.Cleanup(func() { loadAWSConfig = previous })
}

// stubBaseConfig returns a stub that yields a fixed base config with no
// credentials, so credential assertions can only pass via NewS3Client's own
// precedence logic.
func stubBaseConfig(t *testing.T, region string) {
	t.Helper()
	stubLoadAWSConfig(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: region}, nil
	})
}

func TestNewS3Client_LoadError(t *testing.T) {
	sentinel := errors.New("boom: no shared config profile")
	stubLoadAWSConfig(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, sentinel
	})

	client, err := NewS3Client(context.Background(), S3Options{Region: "us-east-1"})
	if err == nil {
		t.Fatalf("expected error, got client %v", client)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error wrapping the load failure with %%w, got %v", err)
	}
	if err.Error() == sentinel.Error() {
		t.Fatalf("expected wrapped error to add context, got bare %v", err)
	}
	if client != nil {
		t.Fatalf("expected nil client on error, got %v", client)
	}
}

func TestNewS3Client_StaticCredentials(t *testing.T) {
	stubBaseConfig(t, "us-east-1")
	// Env vars must lose to explicit static credentials.
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	client, err := NewS3Client(context.Background(), S3Options{
		Region:    "us-east-1",
		AccessKey: "static-key",
		SecretKey: "static-secret",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	creds := retrieveCredentials(t, client.Options().Credentials)
	if creds.AccessKeyID != "static-key" || creds.SecretAccessKey != "static-secret" {
		t.Fatalf("expected static credentials from S3Options, got %+v", creds)
	}
}

func TestNewS3Client_EnvCredentialsFallback(t *testing.T) {
	stubBaseConfig(t, "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	client, err := NewS3Client(context.Background(), S3Options{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	creds := retrieveCredentials(t, client.Options().Credentials)
	if creds.AccessKeyID != "env-key" || creds.SecretAccessKey != "env-secret" {
		t.Fatalf("expected env credentials fallback, got %+v", creds)
	}
}

func TestNewS3Client_DefaultChainWhenNoStaticOrEnvCredentials(t *testing.T) {
	chainCreds := awscreds.NewStaticCredentialsProvider("chain-key", "chain-secret", "")
	stubLoadAWSConfig(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1", Credentials: chainCreds}, nil
	})
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	client, err := NewS3Client(context.Background(), S3Options{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	creds := retrieveCredentials(t, client.Options().Credentials)
	if creds.AccessKeyID != "chain-key" || creds.SecretAccessKey != "chain-secret" {
		t.Fatalf("expected default-chain credentials to be preserved, got %+v", creds)
	}
}

func TestNewS3Client_EndpointForcesPathStyle(t *testing.T) {
	stubBaseConfig(t, "us-east-1")

	withEndpoint, err := NewS3Client(context.Background(), S3Options{
		Region:   "us-east-1",
		Endpoint: "http://127.0.0.1:9000",
	})
	if err != nil {
		t.Fatalf("NewS3Client with endpoint: %v", err)
	}
	opts := withEndpoint.Options()
	if !opts.UsePathStyle {
		t.Errorf("expected UsePathStyle=true when an endpoint is configured (DuckDB httpfs parity)")
	}
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://127.0.0.1:9000" {
		t.Errorf("expected BaseEndpoint to be the configured endpoint, got %v", opts.BaseEndpoint)
	}

	noEndpoint, err := NewS3Client(context.Background(), S3Options{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewS3Client without endpoint: %v", err)
	}
	opts = noEndpoint.Options()
	if opts.UsePathStyle {
		t.Errorf("expected UsePathStyle=false when no endpoint is configured (real AWS uses virtual-host addressing)")
	}
	if opts.BaseEndpoint != nil {
		t.Errorf("expected nil BaseEndpoint when no endpoint is configured, got %q", *opts.BaseEndpoint)
	}
}

func TestNewS3Client_RegionOverride(t *testing.T) {
	stubBaseConfig(t, "us-east-1")

	overridden, err := NewS3Client(context.Background(), S3Options{Region: "ap-northeast-1"})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	if got := overridden.Options().Region; got != "ap-northeast-1" {
		t.Errorf("expected S3Options.Region to override the loaded region, got %q", got)
	}

	inherited, err := NewS3Client(context.Background(), S3Options{})
	if err != nil {
		t.Fatalf("NewS3Client with empty region: %v", err)
	}
	if got := inherited.Options().Region; got != "us-east-1" {
		t.Errorf("expected loaded region to be preserved when S3Options.Region is empty, got %q", got)
	}
}

func retrieveCredentials(t *testing.T, provider aws.CredentialsProvider) aws.Credentials {
	t.Helper()
	if provider == nil {
		t.Fatal("expected a credentials provider on the s3 client, got nil")
	}
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve credentials: %v", err)
	}
	if strings.TrimSpace(creds.AccessKeyID) == "" {
		t.Fatal("expected a non-empty access key id")
	}
	return creds
}
