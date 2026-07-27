package cdc

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
)

// stubLoadAWSConfig swaps the package seam for one test. No t.Parallel:
// the seam is process-global (same pattern as internal/bootstrap).
func stubLoadAWSConfig(
	t *testing.T,
	fn func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error),
) {
	t.Helper()
	previous := loadAWSConfig
	loadAWSConfig = fn
	t.Cleanup(func() { loadAWSConfig = previous })
}

func retrieveCreds(t *testing.T, p aws.CredentialsProvider) aws.Credentials {
	t.Helper()
	if p == nil {
		t.Fatal("expected a credentials provider, got nil")
	}
	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve credentials: %v", err)
	}
	return creds
}

// TestSetupAWSClient_EnvHalfPairPreservesDefaultChain mirrors the
// internal/bootstrap test of the same intent: both sites must keep parity
// (#302, #250 review T3/T4).
func TestSetupAWSClient_EnvHalfPairPreservesDefaultChain(t *testing.T) {
	chain := awsCreds.NewStaticCredentialsProvider("chain-key", "chain-secret", "")
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1", Credentials: chain}, nil
	})
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	_, credProvider, _, err := setupAWSClient(context.Background(), CDCConfig{})
	if err != nil {
		t.Fatalf("setupAWSClient: %v", err)
	}
	creds := retrieveCreds(t, credProvider)
	if creds.AccessKeyID != "chain-key" || creds.SecretAccessKey != "chain-secret" {
		t.Fatalf("half-set env pair must preserve the default chain, got %+v", creds)
	}
}

// TestSetupAWSClient_RegionPassedAtLoad pins WithRegion at load, mirroring
// internal/bootstrap.
func TestSetupAWSClient_RegionPassedAtLoad(t *testing.T) {
	var loadedRegion string
	stubLoadAWSConfig(t, func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (aws.Config, error) {
		lo := &config.LoadOptions{}
		for _, fn := range optFns {
			if err := fn(lo); err != nil {
				return aws.Config{}, err
			}
		}
		loadedRegion = lo.Region
		return aws.Config{Region: lo.Region}, nil
	})

	region, _, _, err := setupAWSClient(context.Background(), CDCConfig{S3Region: "eu-central-1"})
	if err != nil {
		t.Fatalf("setupAWSClient: %v", err)
	}
	if loadedRegion != "eu-central-1" || region != "eu-central-1" {
		t.Fatalf("expected region via load option, got loaded=%q returned=%q", loadedRegion, region)
	}
}

// TestSetupAWSClient_ConfigCredentialsStillWin pins that explicit CDCConfig
// credentials keep precedence over env, unchanged by #302.
func TestSetupAWSClient_ConfigCredentialsStillWin(t *testing.T) {
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1"}, nil
	})
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	_, credProvider, _, err := setupAWSClient(context.Background(), CDCConfig{
		S3AccessKeyID: "cfg-key", S3SecretAccessKey: "cfg-secret",
	})
	if err != nil {
		t.Fatalf("setupAWSClient: %v", err)
	}
	creds := retrieveCreds(t, credProvider)
	if creds.AccessKeyID != "cfg-key" || creds.SecretAccessKey != "cfg-secret" {
		t.Fatalf("config credentials must win over env, got %+v", creds)
	}
}
