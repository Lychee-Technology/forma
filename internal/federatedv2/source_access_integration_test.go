package federatedv2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
)

// fixtureBucket answers GetObject and HeadObject over the fixture's objects
// as S3 answers a principal with s3:GetObject: a key it holds is served, and
// a missing key answers 404 (NoSuchKey, NotFound) when the principal also
// has s3:ListBucket on the bucket, and 403 AccessDenied when it has not.
type fixtureBucket struct {
	f          *fixture
	listBucket bool
}

func (b *fixtureBucket) missing() error {
	if b.listBucket {
		return nil
	}
	return &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}
}

func (b *fixtureBucket) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	raw, err := b.f.store.Get(ctx, aws.ToString(in.Key))
	if errors.Is(err, manifestv2.ErrObjectNotFound) {
		if denied := b.missing(); denied != nil {
			return nil, denied
		}
		return nil, &types.NoSuchKey{}
	}
	if err != nil {
		return nil, err
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(raw))}, nil
}

func (b *fixtureBucket) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("a reader writes nothing")
}

func (b *fixtureBucket) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	b.f.mu.Lock()
	b.f.probes++
	b.f.mu.Unlock()
	_, err := os.Stat(aws.ToString(in.Key))
	if errors.Is(err, os.ErrNotExist) {
		if denied := b.missing(); denied != nil {
			return nil, denied
		}
		return nil, &types.NotFound{}
	}
	if err != nil {
		return nil, err
	}
	return &s3.HeadObjectOutput{}, nil
}

// s3Source is the fixture's reader with its manifests fetched and its
// objects probed through manifestv2's S3 store and probe, over a bucket that
// answers a missing key as S3 does with or without s3:ListBucket.
func (f *fixture) s3Source(t *testing.T, listBucket bool) *Source {
	t.Helper()
	bucket := &fixtureBucket{f: f, listBucket: listBucket}
	cache, err := manifestv2.NewCache(&manifestv2.S3Store{Client: bucket, Bucket: "b"}, 16)
	require.NoError(t, err)
	src, err := New(Config{
		EntityMain: f.tables.EntityMain, Manifests: cache, Epochs: map[int16]int64{schemaID: 1},
		ObjectURI: func(key string) string { return key }, Exists: manifestv2.S3Exists(bucket, "b"),
	})
	require.NoError(t, err)
	return src
}

// A lost manifest or a lost listed object fails the read closed with the
// typed error, with or without s3:ListBucket, and is never answered from
// PostgreSQL nor charged to the breaker. With s3:ListBucket the store says
// 404 and the loss is reported as such: ErrManifestMissing, or the missing
// keys. Without it the store says 403 for the same missing key, which proves
// nothing: the error is forma.ErrPublishedObjectUnverifiable, and it names no
// missing manifest and no missing key.
func TestALossBehindAccessDeniedFailsClosedWithoutClaimingLoss(t *testing.T) {
	for _, lose := range []string{"manifest", "parquet"} {
		for _, listBucket := range []bool{true, false} {
			name := lose + "/without s3:ListBucket"
			if listBucket {
				name = lose + "/with s3:ListBucket"
			}
			t.Run(name, func(t *testing.T) {
				f := newFixture(t)
				f.initialize(t, publication.Unpublished)
				a := map[row]int64{allRows["a"]: 100}
				f.write(t, allRows["a"], 100)
				file := f.deltaFile(t, 1, a)
				g := f.flush(t, a, file)
				if lose == "manifest" {
					f.store.lost = map[string]bool{g.ManifestKey: true}
				} else {
					require.NoError(t, os.Remove(file.Key))
				}
				breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
				e := f.engine(t, f.s3Source(t, listBucket), nil, breaker)

				_, err := e.Query(f.ctx, f.tables, query(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
				require.ErrorIs(t, err, federated.ErrPublicationInconsistent, "fails closed, never degraded")
				require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)
				var typed *federated.PublicationConsistencyError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, [3]int64{int64(schemaID), 1, g.Generation}, [3]int64{int64(typed.SchemaID), typed.Epoch, typed.Generation})
				require.False(t, breaker.IsOpen(), "neither a loss nor a refused probe is DuckDB's failure")
				switch {
				case listBucket && lose == "manifest":
					require.ErrorIs(t, err, manifestv2.ErrManifestMissing)
				case listBucket:
					require.Equal(t, []string{file.Key}, typed.MissingKeys)
				default:
					require.ErrorIs(t, err, forma.ErrPublishedObjectUnverifiable)
					require.NotErrorIs(t, err, manifestv2.ErrManifestMissing)
					require.Empty(t, typed.MissingKeys, "a 403 is not a confirmed loss")
				}
				if listBucket {
					require.NotErrorIs(t, err, forma.ErrPublishedObjectUnverifiable)
				}
			})
		}
	}
}
