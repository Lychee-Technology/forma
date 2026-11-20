package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

func TestOptionsRefuseARetentionThatDoesNotCoverTheReader(t *testing.T) {
	ok := Options{RetentionGrace: 2 * time.Minute, ReaderDeadline: time.Minute, ClockMargin: time.Minute, OrphanAge: time.Hour}
	require.NoError(t, ok.Validate(), "G = R_max + C is enough")

	for name, mutate := range map[string]func(*Options){
		"grace below the deadline plus the margin": func(o *Options) { o.RetentionGrace = 2*time.Minute - time.Millisecond },
		"no reader deadline":                       func(o *Options) { o.ReaderDeadline = 0 },
	} {
		o := ok
		mutate(&o)
		require.ErrorIs(t, o.Validate(), ErrRetentionUnsafe, name)
	}
	o := ok
	o.OrphanAge = 0
	require.ErrorContains(t, o.Validate(), "orphan age")
	o = ok
	o.ClockMargin = -time.Second
	require.ErrorContains(t, o.Validate(), "clock margin")

	_, err := (&Reconciler{Opts: Options{}}).Run(context.Background())
	require.ErrorIs(t, err, ErrRetentionUnsafe, "a run with unsafe options visits no schema")
}

func TestReportResidualAndRender(t *testing.T) {
	clean := SchemaReport{SchemaID: 1, State: publication.Published, Epoch: 2, Generation: 5,
		Plan: publication.Plan{Objects: []string{"a"}, Forget: []int64{3}}}
	require.False(t, clean.Residual(), "garbage awaiting collection needs no operator")
	require.True(t, SchemaReport{Skipped: true}.Residual())
	require.True(t, SchemaReport{Err: errors.New("boom")}.Residual())
	require.True(t, SchemaReport{Findings: []Finding{{Kind: UnrecognizedObject}}}.Residual())

	collected := clean
	collected.SchemaID = 2
	collected.Collected = &Collected{Retired: []int64{4}, Objects: []string{"a"}}
	refused := SchemaReport{SchemaID: 3, Refused: "no_record: x",
		Findings: []Finding{recordFinding(NoRecord, publication.ErrNoRecord)}}
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{clean, collected, refused, {SchemaID: 4, Skipped: true}}}.Render(&out)
	text := out.String()
	for _, want := range []string{
		"schema 1: clean (PUBLISHED, epoch 2, generation 5)",
		"garbage collection would delete 1 objects, 0 manifests, and forgets generations [3]",
		"retired generations: [4]",
		"garbage collection deleted 1 objects",
		"deleted object: a",
		"schema 3: needs attention (no record read, epoch 0, generation 0)",
		"no_record: publication record missing",
		"action: run init-db",
		"garbage collection refused: no_record: x",
		"schema 4: skipped",
	} {
		require.Contains(t, text, want)
	}
	block := text[strings.Index(text, "schema 3:"):strings.Index(text, "schema 4:")]
	require.NotContains(t, block, "garbage collection would delete", "a refused pass plans no deletes")
}

func TestEveryBlockingFindingHasAnAction(t *testing.T) {
	g := publication.Generation{Epoch: 1, Generation: 2}
	for _, kind := range []FindingKind{MissingGeneration, MissingRetainedManifest, InvalidManifest, ForeignManifest} {
		require.True(t, kind.blocking(), kind)
		require.NotEmpty(t, generationFinding(kind, g, false, "").Action, kind)
	}
	for _, kind := range []FindingKind{LostObject, CorruptObject, UnrecognizedObject, AwaitingInit} {
		require.False(t, kind.blocking(), "%s leaves what no reader scans collectable", kind)
	}
	require.Equal(t, actionLostCurrent, objectFinding(LostObject, g, true, "k", "").Action)
	require.Equal(t, actionLostSuperseded, objectFinding(LostObject, g, false, "k", "").Action)
}

// fakeS3 answers the S3Objects calls from a map, with one listing page per
// call.
type fakeS3 struct {
	objects   map[string][]byte
	truncated bool
	getErr    error
	deleted   []string
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(f.truncated)}
	for k := range f.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			out.Contents = append(out.Contents, types.Object{Key: aws.String(k), LastModified: aws.Time(time.UnixMilli(1000))})
		}
	}
	return out, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	data, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func (f *fakeS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("the collector never puts")
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if _, ok := f.objects[aws.ToString(in.Key)]; !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.deleted = append(f.deleted, aws.ToString(in.Key))
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3ObjectsTellsAMissingObjectFromAnUnreadableOne(t *testing.T) {
	ctx := context.Background()
	client := &fakeS3{objects: map[string][]byte{"data/1/a": []byte("abc"), "data/10/b": nil}}
	s := &S3Objects{Client: client, Bucket: "b"}

	listed, err := s.List(ctx, "data/1/")
	require.NoError(t, err)
	require.Equal(t, []publication.Object{{Key: "data/1/a", LastModified: 1000}}, listed)
	sum, n, err := s.Hash(ctx, "data/1/a")
	require.NoError(t, err)
	require.Equal(t, [2]any{manifestv2.Digest([]byte("abc")), int64(3)}, [2]any{sum, n})
	ok, err := s.Exists(ctx, "data/1/zz")
	require.NoError(t, err)
	require.False(t, ok)
	_, err = s.Get(ctx, "data/1/zz")
	require.ErrorIs(t, err, manifestv2.ErrObjectNotFound)
	require.NoError(t, s.Delete(ctx, "data/1/a"))
	require.Equal(t, []string{"data/1/a"}, client.deleted)

	client.getErr = errors.New("503")
	_, err = s.Get(ctx, "data/1/a")
	require.Error(t, err)
	require.NotErrorIs(t, err, manifestv2.ErrObjectNotFound, "a failed read is never a missing object")

	client.truncated = true
	_, err = s.List(ctx, "data/1/")
	require.ErrorIs(t, err, cdc.ErrIncompleteObjectListing, "a listing that stops short fails")
}
