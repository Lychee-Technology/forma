package cdc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// countingS3 is an S3 client that counts every call and answers none: a
// refusal before any I/O leaves it at zero.
type countingS3 struct{ calls atomic.Int32 }

func (c *countingS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.calls.Add(1)
	return nil, errors.New("countingS3 answers no call")
}

func (c *countingS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.calls.Add(1)
	return nil, errors.New("countingS3 answers no call")
}

// An embedded caller hands the writers a cdc.Config directly, without the
// tools' -s3-prefix check. A prefix holding a glob metacharacter would let
// the writer stage and commit a generation, and mark its rows flushed, that
// the federated reader refuses on every read (ErrPublishedPathNotConcrete).
// Both flush entry points and init refuse it as a *forma.ConfigError before
// any S3, PostgreSQL or DuckDB I/O: the client sees no call, and the
// unreachable PostgreSQL host is never dialled.
func TestWritersRefuseAGlobDataPrefixBeforeAnyIO(t *testing.T) {
	entrypoints := map[string]func(CDCConfig, S3Client) error{
		"RunOnce": func(cfg CDCConfig, client S3Client) error {
			_, err := RunOnce(context.Background(), cfg, client, false, zap.NewNop(), &stubSchemaRegistry{})
			return err
		},
		"Runner.RunOnce": func(cfg CDCConfig, client S3Client) error {
			r := NewRunner(zap.NewNop())
			defer func() { _ = r.Close() }()
			_, err := r.RunOnce(context.Background(), cfg, client, false, &stubSchemaRegistry{})
			return err
		},
		"RunInit": func(cfg CDCConfig, client S3Client) error {
			_, err := RunInit(context.Background(), InitOptions{Config: cfg, S3Client: client, SchemaRegistry: &stubSchemaRegistry{}})
			return err
		},
	}
	for _, prefix := range []string{"da*ta", "data?", "d[a]ta"} {
		for entry, run := range entrypoints {
			t.Run(entry+"/"+prefix, func(t *testing.T) {
				client := &countingS3{}
				cfg := CDCConfig{S3Bucket: "bucket", S3Prefix: prefix, PGHost: "unreachable.invalid", PGPort: 1}
				err := run(cfg, client)
				var cfgErr *forma.ConfigError
				if !errors.As(err, &cfgErr) || cfgErr.Field != "S3Prefix" {
					t.Fatalf("%s with prefix %q: want a *forma.ConfigError on S3Prefix, got %v", entry, prefix, err)
				}
				if n := client.calls.Load(); n != 0 {
					t.Fatalf("%s made %d S3 calls before refusing the prefix", entry, n)
				}
			})
		}
	}
	if err := (CDCConfig{S3Prefix: "data/v2"}).ValidateInit(); err != nil {
		t.Fatalf("a literal prefix is a valid init configuration: %v", err)
	}
	if err := (CDCConfig{S3Prefix: "data/v2"}).ValidateFlush(); err != nil {
		t.Fatalf("a literal prefix is a valid flush configuration: %v", err)
	}
}
