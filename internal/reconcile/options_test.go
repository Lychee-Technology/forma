package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
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

// TestOptionsRefuseAFloorThatOverflows: R_max + C summed in a Duration
// would wrap negative and let a zero grace pass.
func TestOptionsRefuseAFloorThatOverflows(t *testing.T) {
	for _, o := range []Options{
		{RetentionGrace: 0, ReaderDeadline: time.Nanosecond, ClockMargin: math.MaxInt64, OrphanAge: time.Nanosecond},
		{RetentionGrace: math.MaxInt64, ReaderDeadline: time.Minute, ClockMargin: math.MaxInt64 - time.Minute + 1, OrphanAge: time.Hour},
	} {
		require.ErrorIs(t, o.Validate(), ErrRetentionUnsafe, "%+v", o)
	}
	edge := Options{RetentionGrace: math.MaxInt64, ReaderDeadline: time.Minute, ClockMargin: math.MaxInt64 - time.Minute, OrphanAge: time.Hour}
	require.NoError(t, edge.Validate(), "a floor of exactly MaxInt64 is representable")

	sum, ok := AddDurations(math.MaxInt64-1, 1)
	require.Equal(t, [2]any{time.Duration(math.MaxInt64), true}, [2]any{sum, ok})
	_, ok = AddDurations(math.MaxInt64, 1)
	require.False(t, ok)
	_, ok = AddDurations(-1, 0)
	require.False(t, ok, "a negative term is no floor")

	_, err := (&Reconciler{Opts: Options{GC: true, RetentionGrace: time.Hour, ReaderDeadline: time.Minute, OrphanAge: time.Hour}}).Run(context.Background())
	require.ErrorContains(t, err, "schema lock", "a collection without the schema lock visits no schema")
}

func TestReportResidualAndRender(t *testing.T) {
	clean := SchemaReport{SchemaID: 1, State: publication.Published, Epoch: 2, Generation: 5,
		Plan: publication.Plan{Objects: []string{"a"}, Forget: []int64{3}}}
	require.False(t, clean.Residual(), "garbage awaiting collection needs no operator")
	require.True(t, SchemaReport{Skipped: true}.Residual())
	require.True(t, SchemaReport{Err: errors.New("boom")}.Residual())
	require.True(t, SchemaReport{Findings: []Finding{{Kind: UnrecognizedObject}}}.Residual())
	inconclusive := SchemaReport{SchemaID: 6, State: publication.Published, Epoch: 2, Generation: 5,
		ConcurrentlyExpired: 1, Refused: "a collection ran beside the diagnosis"}
	require.True(t, inconclusive.Inconclusive())
	require.True(t, inconclusive.Residual(), "a diagnosis that left findings out is not clean")
	require.False(t, clean.Inconclusive())
	refusedNoFinding := SchemaReport{SchemaID: 7, State: publication.Unpublished, Epoch: 1,
		Refused: "no manifest of schema 7's generation history verified in the bucket"}
	require.False(t, refusedNoFinding.Inconclusive())
	require.True(t, refusedNoFinding.Residual(), "a refused collection without findings is not clean")

	collected := clean
	collected.SchemaID = 2
	collected.Collected = &Collected{Retired: []int64{4}, RecoveryRetired: true, Objects: []string{"a"}, Forgotten: []int64{3}}
	partial := SchemaReport{SchemaID: 5, State: publication.Published,
		Plan:      publication.Plan{Objects: []string{"p", "q"}, Manifests: []string{"m"}, Forget: []int64{2}},
		Collected: &Collected{Objects: []string{"p"}}, Err: errors.New("injected delete failure of q")}
	refused := SchemaReport{SchemaID: 3, Refused: "no_record: x",
		Findings: []Finding{recordFinding(NoRecord, publication.ErrNoRecord)}}
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{clean, collected, refused, {SchemaID: 4, Skipped: true}, partial, inconclusive, refusedNoFinding}}.Render(&out)
	text := out.String()
	for _, want := range []string{
		"schema 1: clean (PUBLISHED, epoch 2, generation 5)",
		"garbage collection would delete 1 objects and 0 manifests, and forget generations [3]",
		"would delete object: a",
		"retired generations: [4]",
		"retired recovery fence (grace period started)",
		"garbage collection deleted 1 of 1 planned objects and 0 of 0 planned manifests, and forgot generations [3] of [3] planned",
		"deleted object: a",
		"schema 3: needs attention (no record read, epoch 0, generation 0)",
		"no_record: publication record missing",
		"action: run init-db",
		"garbage collection refused: no_record: x",
		"schema 4: skipped",
		"schema 6: inconclusive (run the diagnosis again) (PUBLISHED, epoch 2, generation 5)",
		"1 finding(s) left out",
		"schema 7: collection refused (UNPUBLISHED, epoch 1, generation 0)",
		"garbage collection refused: no manifest of schema 7's generation history verified in the bucket",
	} {
		require.Contains(t, text, want)
	}
	block := text[strings.Index(text, "schema 3:"):strings.Index(text, "schema 4:")]
	require.NotContains(t, block, "garbage collection would delete", "a refused pass plans no deletes")
	block = text[strings.Index(text, "schema 7:"):]
	require.NotContains(t, block, ": clean (", "a refused pass without findings must not render as clean")
	require.NotContains(t, block, "garbage collection would delete", "a refused pass without findings plans no deletes")
	block = text[strings.Index(text, "schema 5:"):strings.Index(text, "schema 6:")]
	for _, want := range []string{
		"garbage collection deleted 1 of 2 planned objects and 0 of 1 planned manifests, and forgot generations [] of [2] planned",
		"deleted object: p",
		"left for the next pass: 3 planned deletes and forgets did not run",
		"error: injected delete failure of q",
	} {
		require.Contains(t, block, want)
	}
	require.NotContains(t, block, "deleted object: q", "a failed delete is not reported as done")
	require.NotContains(t, block, "deleted manifest", "the manifest step never ran")
}

func TestEveryBlockingFindingHasAnAction(t *testing.T) {
	g := publication.Generation{Epoch: 1, Generation: 2}
	for _, kind := range []FindingKind{MissingGeneration, HistoryMismatch, HistoryGap, MissingRetainedManifest, InvalidManifest, ForeignManifest} {
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
	undated   string
	truncated bool
	getErr    error
	deleted   []string
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(f.truncated)}
	for k := range f.objects {
		if !strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			continue
		}
		o := types.Object{Key: aws.String(k), LastModified: aws.Time(time.UnixMilli(1000))}
		if k == f.undated {
			o.LastModified = nil
		}
		out.Contents = append(out.Contents, o)
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
	_, _, err = s.Hash(ctx, "data/1/zz")
	require.ErrorIs(t, err, manifestv2.ErrObjectNotFound, "a hash finds a missing object as a read does")
	require.NoError(t, s.Delete(ctx, "data/1/a"))
	require.Equal(t, []string{"data/1/a"}, client.deleted)

	client.getErr = errors.New("503")
	_, err = s.Get(ctx, "data/1/a")
	require.Error(t, err)
	require.NotErrorIs(t, err, manifestv2.ErrObjectNotFound, "a failed read is never a missing object")
	_, _, err = s.Hash(ctx, "data/1/a")
	require.Error(t, err)
	require.NotErrorIs(t, err, manifestv2.ErrObjectNotFound, "nor a failed hash")

	client.truncated = true
	_, err = s.List(ctx, "data/1/")
	require.ErrorIs(t, err, cdc.ErrIncompleteObjectListing, "a listing that stops short fails")
}

// TestS3ObjectsRefusesAListingWithoutLastModified: aws.ToTime of a missing
// LastModified is the zero time, which would date fresh staging before
// every horizon; the listing fails instead.
func TestS3ObjectsRefusesAListingWithoutLastModified(t *testing.T) {
	key := "data/1/e1/delta/" + uuid.Must(uuid.NewV7()).String() + ".parquet"
	client := &fakeS3{objects: map[string][]byte{key: []byte("staged")}, undated: key}
	_, err := (&S3Objects{Client: client, Bucket: "b"}).List(context.Background(), "data/1/")
	require.ErrorIs(t, err, ErrUnknownAge)
	require.ErrorContains(t, err, key)
	require.Empty(t, client.deleted)
}
