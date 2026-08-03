package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// fakeProbeClient is an S3ProbeClient whose HeadObject is programmable,
// mirroring the in-memory fake style of memStore (manifest_test.go). Only
// the probe path is exercised here; the manifest object IO is covered by
// the Store tests.
type fakeProbeClient struct {
	head        func(key string) (*s3.HeadObjectOutput, error)
	headKeys    []string
	headBuckets []string
}

func (f *fakeProbeClient) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, errors.New("GetObject not implemented by fakeProbeClient")
}

func (f *fakeProbeClient) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("PutObject not implemented by fakeProbeClient")
}

func (f *fakeProbeClient) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headBuckets = append(f.headBuckets, aws.ToString(params.Bucket))
	f.headKeys = append(f.headKeys, aws.ToString(params.Key))
	if f.head == nil {
		return &s3.HeadObjectOutput{}, nil
	}
	return f.head(aws.ToString(params.Key))
}

// httpResponseError builds the shape the AWS SDK returns for an HTTP error
// response whose body carries no recognizable S3 error code — the case the
// *types.NotFound assertion alone misses.
func httpResponseError(status int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      fmt.Errorf("http response error StatusCode: %d", status),
		},
		RequestID: "req-1",
	}
}

func testS3Config() S3QuerySourceConfig {
	return S3QuerySourceConfig{
		Bucket:           "bkt",
		ManifestPrefix:   "mf",
		ManifestTemplate: "manifest/{{.SchemaID}}.json",
		DataPrefix:       "data",
	}
}

func TestNewS3QuerySource_Fields(t *testing.T) {
	client := &fakeProbeClient{}

	src := NewS3QuerySource(client, testS3Config())

	require.Equal(t, "bkt", src.Bucket)
	require.Equal(t, PathResolver{Prefix: "mf", PathTemplate: "manifest/{{.SchemaID}}.json"}, src.Resolver)
	store, ok := src.Store.(*S3Store)
	require.True(t, ok, "Store must be the S3-backed manifest store")
	require.Same(t, client, store.Client)
	require.Equal(t, "bkt", store.Bucket)
	require.NotNil(t, src.Exists, "missing-key classification requires a probe")
	require.NotNil(t, src.Fallback)
	// Byte-identical to the harness glob (production/engine.go:88): a single
	// `*` never crosses `/`, so `{prefix}/{id}/_tmp/...` stays excluded.
	require.Equal(t, "s3://bkt/data/21/*.parquet", src.Fallback(21))
}

func TestNewS3QuerySource_NoDataPrefix_NoFallback(t *testing.T) {
	cfg := testS3Config()
	cfg.DataPrefix = ""

	src := NewS3QuerySource(&fakeProbeClient{}, cfg)

	// A literal nil Fallback takes query_source.go's nil branch: schemas with
	// no manifest entries scan nothing rather than a bucket-root glob.
	require.Nil(t, src.Fallback)
}

func TestS3FallbackGlob_NoDataPrefixIsNil(t *testing.T) {
	require.Nil(t, S3FallbackGlob("bkt", ""))
}

func TestS3FallbackGlob_MatchesHarnessFormat(t *testing.T) {
	glob := S3FallbackGlob("bkt", "forma-data")
	require.NotNil(t, glob)
	require.Equal(t, fmt.Sprintf("s3://%s/%s/%d/*.parquet", "bkt", "forma-data", int16(7)), glob(7))
}

// TestS3FallbackGlob_TrailingSlashCanonicalizesLikeWriter pins writer parity.
// The CDC writers canonicalize their prefix with strings.TrimSuffix(prefix,
// "/") before joining (internal/cdc.BuildDeltaPath / BuildBasePath), so
// S3Prefix "delta/" writes objects at "delta/{schemaID}/{uuid}.parquet". A
// reader that Sprintf's the raw prefix builds "s3://b/delta//7/*.parquet",
// whose empty path segment matches nothing — the empty-manifest fallback then
// silently reads zero rows instead of the schema's parquet.
func TestS3FallbackGlob_TrailingSlashCanonicalizesLikeWriter(t *testing.T) {
	glob := S3FallbackGlob("b", "delta/")
	require.NotNil(t, glob)
	// Mirrors cdc.BuildDeltaPath("delta/", 7, uuid) == "delta/7/<uuid>.parquet".
	// (Asserted as a literal rather than by calling the writer: internal/cdc
	// imports this package, so referencing it here would be an import cycle.)
	require.Equal(t, "s3://b/delta/7/*.parquet", glob(7))
}

// TestS3FallbackGlob_RootDataPrefixIsNil: a prefix of "/" canonicalizes to
// the empty prefix, which must take the same "no fallback" branch as "" —
// otherwise the glob scans the bucket root.
func TestS3FallbackGlob_RootDataPrefixIsNil(t *testing.T) {
	require.Nil(t, S3FallbackGlob("b", "/"))
}

// TestQuerySource_EmptyManifestFallbackUsesCanonicalGlob closes the loop at
// the level the reader actually uses: a schema with no manifest entries and a
// trailing-slash DataPrefix must resolve the writer's canonical prefix.
func TestQuerySource_EmptyManifestFallbackUsesCanonicalGlob(t *testing.T) {
	cfg := testS3Config()
	cfg.DataPrefix = "delta/"
	src := NewS3QuerySource(&fakeProbeClient{}, cfg)
	src.Store = &memStore{} // no manifest object => empty manifest

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/delta/7/*.parquet"}, paths)
}

func TestS3ExistsProbe_Found(t *testing.T) {
	client := &fakeProbeClient{}
	probe := S3ExistsProbe(client, "bkt")

	ok, err := probe(context.Background(), "p/7/a.parquet")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"bkt"}, client.headBuckets)
	require.Equal(t, []string{"p/7/a.parquet"}, client.headKeys)
}

func TestS3ExistsProbe_NotFound(t *testing.T) {
	client := &fakeProbeClient{head: func(string) (*s3.HeadObjectOutput, error) {
		return nil, &types.NotFound{}
	}}
	probe := S3ExistsProbe(client, "bkt")

	ok, err := probe(context.Background(), "p/7/gone.parquet")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestS3ExistsProbe_HTTP404(t *testing.T) {
	// Some S3-compatible stores answer HeadObject with a bare 404 that the
	// SDK cannot model as *types.NotFound (HEAD carries no body to parse).
	client := &fakeProbeClient{head: func(string) (*s3.HeadObjectOutput, error) {
		return nil, httpResponseError(http.StatusNotFound)
	}}
	probe := S3ExistsProbe(client, "bkt")

	ok, err := probe(context.Background(), "p/7/gone.parquet")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestS3ExistsProbe_OtherErrorPropagates(t *testing.T) {
	// A probe failure is not proof of absence — anything but a confirmed 404
	// must propagate so MissingIn never fabricates a data-loss report.
	cases := map[string]error{
		"transport":  errors.New("connection refused"),
		"http500":    httpResponseError(http.StatusInternalServerError),
		"http403":    httpResponseError(http.StatusForbidden),
		"apiTimeout": fmt.Errorf("wrapped: %w", errors.New("context deadline exceeded")),
	}
	for name, injected := range cases {
		t.Run(name, func(t *testing.T) {
			client := &fakeProbeClient{head: func(string) (*s3.HeadObjectOutput, error) {
				return nil, injected
			}}
			probe := S3ExistsProbe(client, "bkt")

			ok, err := probe(context.Background(), "p/7/a.parquet")
			require.Error(t, err)
			require.False(t, ok)
			require.ErrorIs(t, err, injected)
			require.ErrorContains(t, err, "p/7/a.parquet")
		})
	}
}

func TestNewS3QuerySource_MissingInReportsOnlyAbsentKeys(t *testing.T) {
	client := &fakeProbeClient{head: func(key string) (*s3.HeadObjectOutput, error) {
		if key == "data/7/gone.parquet" {
			return nil, &types.NotFound{}
		}
		return &s3.HeadObjectOutput{}, nil
	}}
	src := NewS3QuerySource(client, testS3Config())

	missing, err := src.MissingIn(context.Background(), []string{
		"s3://bkt/data/7/a.parquet",
		"s3://bkt/data/7/gone.parquet",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"data/7/gone.parquet"}, missing)
}
