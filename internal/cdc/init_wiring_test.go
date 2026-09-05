package cdc

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The init run context's S3-derived fields are wired in one place, so the
// content-checksum seam hashes through the run's own S3 client (#347).
func TestApplyInitS3WiringHashesThroughRunClient(t *testing.T) {
	runCtx := &initRunContext{logger: zap.NewNop()}
	cfg := CDCConfig{S3Bucket: "test-bucket", ManifestPrefix: "cdc", ManifestTemplate: "manifest/{{.SchemaID}}.json"}

	applyInitS3Wiring(runCtx, cfg, &hashableFullS3Client{body: []byte("hello parquet")})

	require.NotNil(t, runCtx.checksumObject, "init run context built without the checksum seam")
	got, err := runCtx.checksumObject(context.Background(), "cdc/7/a_b.parquet")
	require.NoError(t, err)
	// sha256("hello parquet")
	require.Equal(t, "sha256:950423965d5b936670f1549c58ce0594b58e1027c2b5e1e2a4f1515b1bc2f1b0", got)
	require.NotNil(t, runCtx.manifestStore, "init run context built without the manifest store")
	require.NotNil(t, runCtx.s3Client, "init run context built without the object client")
	require.Equal(t, cfg.ManifestTemplate, runCtx.manifestResolver.PathTemplate)
}

// A run with no usable S3 client leaves the seam nil — entries go unstamped
// instead of the first hash panicking on a typed-nil client (#302).
func TestApplyInitS3WiringLeavesChecksumSeamNilWithoutClient(t *testing.T) {
	runCtx := &initRunContext{logger: zap.NewNop()}
	applyInitS3Wiring(runCtx, CDCConfig{S3Bucket: "test-bucket"}, nil)

	require.Nil(t, runCtx.checksumObject, "checksum seam wired from a nil S3 client")
	require.Nil(t, runCtx.manifestStore, "manifest store wired without a manifest template")
}

// newInitRunContext built through its real signature: sql.Open("pgx", …) is
// lazy and NewDuckExporter opens an in-memory DuckDB, so the constructor needs
// neither a live Postgres nor a live S3 to return. That makes the production
// wiring directly observable — if the constructor ever stops delegating to
// applyInitS3Wiring, or delegates without a client, checksumObject is nil here
// and every init-written manifest entry ships unstamped.
//
// The seam's hashing behavior is covered by
// TestApplyInitS3WiringHashesThroughRunClient: InitOptions.S3Client is the
// concrete *s3.Client, so no fake body can be injected through this path.
func TestNewInitRunContextWiresChecksumSeamOnTheRealPath(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")

	cfg := newExporterInitTestConfig()
	cfg.S3Bucket = "test-bucket"
	cfg.ManifestPrefix = "cdc"
	cfg.ManifestTemplate = "manifest/{{.SchemaID}}.json"
	cfg.PGHost = "127.0.0.1"
	cfg.PGPort = 5432
	cfg.PGUser = "pguser"
	cfg.PGDB = "forma"
	cfg.PGSSLMode = "disable"

	runCtx, err := newInitRunContext(context.Background(), InitOptions{
		Config:   cfg,
		S3Client: &s3.Client{},
		Logger:   zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(runCtx.close)

	require.NotNil(t, runCtx.checksumObject,
		"the real init constructor left the checksum seam nil; every entry it writes ships unstamped (#347)")
	require.NotNil(t, runCtx.s3Client, "the real init constructor left the object client nil")
	require.NotNil(t, runCtx.manifestStore, "the real init constructor left the manifest store nil")
	require.Equal(t, cfg.ManifestTemplate, runCtx.manifestResolver.PathTemplate)
}

// The behavioral test above proves the seam is wired today; this pins how.
// It walks every non-test file in the package, so a construction site that
// moves to another file stays visible, and it checks the argument the
// constructor actually hands over: `applyInitS3Wiring(runCtx, cfg, nil)`
// compiles, keeps the helper's own tests green, and silently unstamps
// production (#318, #347).
func TestNewInitRunContextWiresS3ThroughHelper(t *testing.T) {
	dirEntries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()

	var storeLiteralFuncs []string
	var constructorFrames []string
	frameCallsHelper := map[string]bool{}
	framePassesOptsClient := map[string]bool{}

	for _, dirEntry := range dirEntries {
		path := dirEntry.Name()
		if dirEntry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, parseErr)

		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			frame := fmt.Sprintf("%s:%s", path, fn.Name.Name)
			if fn.Name.Name == "newInitRunContext" {
				constructorFrames = append(constructorFrames, frame)
			}

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, isLit := n.(*ast.CompositeLit); isLit {
					if sel, isSel := lit.Type.(*ast.SelectorExpr); isSel && sel.Sel.Name == "S3Store" {
						storeLiteralFuncs = append(storeLiteralFuncs, frame)
					}
				}
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				ident, isIdent := call.Fun.(*ast.Ident)
				if !isIdent || ident.Name != "applyInitS3Wiring" {
					return true
				}
				frameCallsHelper[frame] = true
				framePassesOptsClient[frame] = callPassesOptsS3Client(call)
				return true
			})
		}
	}

	require.Len(t, constructorFrames, 1,
		"expected exactly one newInitRunContext frame in the package, found %v", constructorFrames)
	frame := constructorFrames[0]
	require.True(t, frameCallsHelper[frame],
		"%s does not wire its S3-derived fields through applyInitS3Wiring, so the checksum seam is unwired on the real init path", frame)
	require.True(t, framePassesOptsClient[frame],
		"%s calls applyInitS3Wiring without handing over opts.S3Client, so the checksum seam and manifest store are nil on that path", frame)

	for _, site := range storeLiteralFuncs {
		funcName := site[strings.Index(site, ":")+1:]
		require.Contains(t, []string{"applyInitS3Wiring", "newSchemaFlushContext"}, funcName,
			"manifest.S3Store is built in %s; each path's S3 wiring must stay in its one wiring function", site)
	}
}

// callPassesOptsS3Client reports whether an applyInitS3Wiring call hands over
// the run options' S3 client as its client argument, rather than nil or an
// arbitrary expression.
func callPassesOptsS3Client(call *ast.CallExpr) bool {
	if len(call.Args) != 3 {
		return false
	}
	sel, isSel := call.Args[2].(*ast.SelectorExpr)
	if !isSel || sel.Sel.Name != "S3Client" {
		return false
	}
	ident, isIdent := sel.X.(*ast.Ident)
	return isIdent && ident.Name == "opts"
}

// pagedListClient answers ListObjectsV2 in two pages so the continuation
// loop is exercised, and records every key it is asked to delete.
type pagedListClient struct {
	hashableFullS3Client
	calls   int
	deleted []string
}

func (c *pagedListClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.calls++
	if in.ContinuationToken == nil {
		return &s3.ListObjectsV2Output{
			Contents:              []types.Object{{Key: aws.String(aws.ToString(in.Prefix) + "a.parquet")}},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("page-2"),
		}, nil
	}
	return &s3.ListObjectsV2Output{
		Contents: []types.Object{{Key: aws.String(aws.ToString(in.Prefix) + "b.parquet")}},
	}, nil
}

func (c *pagedListClient) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.deleted = append(c.deleted, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

// The delta-tier seams (#371) are wired over the run's client: the listing
// follows continuation tokens and the delete addresses the run's bucket.
func TestApplyInitS3WiringWiresDeltaTierSeams(t *testing.T) {
	client := &pagedListClient{}
	runCtx := &initRunContext{logger: zap.NewNop()}
	applyInitS3Wiring(runCtx, CDCConfig{S3Bucket: "test-bucket"}, client)

	require.NotNil(t, runCtx.listObjectKeys)
	require.NotNil(t, runCtx.deleteObject)
	keys, err := runCtx.listObjectKeys(context.Background(), "delta/7/")
	require.NoError(t, err)
	require.Equal(t, []string{"delta/7/a.parquet", "delta/7/b.parquet"}, keys)
	require.Equal(t, 2, client.calls, "the listing must follow the continuation token")
	require.NoError(t, runCtx.deleteObject(context.Background(), "delta/7/a.parquet"))
	require.Equal(t, []string{"delta/7/a.parquet"}, client.deleted)
}

// A nil or typed-nil client leaves both seams nil so the inventory skips the
// listing and the purge refuses honestly instead of panicking (#302).
func TestNewDeltaTierSeamsNilWithoutClient(t *testing.T) {
	list, del := newDeltaTierSeams(nil, "b")
	require.Nil(t, list)
	require.Nil(t, del)
	var typedNil *s3.Client
	list, del = newDeltaTierSeams(typedNil, "b")
	require.Nil(t, list)
	require.Nil(t, del)
}

// malformedListClient answers ListObjectsV2 with a scripted sequence of
// pages and records every key it is asked to delete.
type malformedListClient struct {
	hashableFullS3Client
	pages   []*s3.ListObjectsV2Output
	calls   int
	deleted []string
}

func (c *malformedListClient) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if c.calls >= len(c.pages) {
		return nil, fmt.Errorf("unexpected ListObjectsV2 call %d", c.calls+1)
	}
	out := c.pages[c.calls]
	c.calls++
	return out, nil
}

func (c *malformedListClient) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.deleted = append(c.deleted, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func listPage(truncated bool, token *string, keys ...string) *s3.ListObjectsV2Output {
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(truncated), NextContinuationToken: token}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k)})
	}
	return out
}

// A page that claims to be truncated but carries no usable continuation
// token, or hands back a token the listing already used, cannot be followed
// to the end. The listing fails closed with the named sentinel instead of
// returning the keys seen so far as if they were the whole prefix (#371
// review P1): a short inventory under --replace-delta would purge less than
// the delta tier holds and leave stale objects for --repair to re-adopt.
func TestListObjectKeys_UnfollowableTruncationFailsClosed(t *testing.T) {
	cases := []struct {
		name  string
		pages []*s3.ListObjectsV2Output
		calls int
		want  string
	}{
		{
			name:  "truncated without token",
			pages: []*s3.ListObjectsV2Output{listPage(true, nil, "delta/7/a.parquet")},
			calls: 1,
			want:  "page 1 is truncated but carries no continuation token",
		},
		{
			name:  "truncated with empty token",
			pages: []*s3.ListObjectsV2Output{listPage(true, aws.String(""), "delta/7/a.parquet")},
			calls: 1,
			want:  "page 1 is truncated but carries no continuation token",
		},
		{
			name: "repeated token",
			pages: []*s3.ListObjectsV2Output{
				listPage(true, aws.String("page-2"), "delta/7/a.parquet"),
				listPage(true, aws.String("page-2"), "delta/7/b.parquet"),
			},
			calls: 2,
			want:  `page 2 repeats continuation token "page-2"`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := &malformedListClient{pages: c.pages}
			keys, err := ListObjectKeys(context.Background(), client, "test-bucket", "delta/7/")
			require.ErrorIs(t, err, ErrIncompleteObjectListing)
			require.Contains(t, err.Error(), "list objects under delta/7/: "+c.want)
			require.Nil(t, keys, "an incomplete listing must not hand back a partial key set")
			require.Equal(t, c.calls, client.calls)
		})
	}
}

// The malformed listing stops the schema at the pre-flight: with
// --replace-delta and a manifest-listed delta entry, the run returns the
// listing error, deletes nothing and leaves the manifest untouched. initSchema
// returns that error before prepareSchemaInit, so no export, publish or
// purge can follow it.
func TestPreflightDeltaTier_IncompleteListingStopsBeforeAnySwapOrPurge(t *testing.T) {
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	client := &malformedListClient{pages: []*s3.ListObjectsV2Output{listPage(true, nil, listedKey)}}
	cfg := CDCConfig{S3Bucket: "test-bucket", ManifestTemplate: deltaTestTemplate}
	runCtx := &initRunContext{cfg: cfg, logger: zap.NewNop(), deltaPrefix: "delta", replaceDelta: true}
	applyInitS3Wiring(runCtx, cfg, client)
	// The listing seam is the wired one; the manifest lives in memory so the
	// no-op PutObject of the fake client cannot mask a swap.
	st := &memManifestStore{}
	runCtx.manifestStore = st
	seedDeltaManifest(t, st, listedKey)
	before, _, err := st.Load(context.Background(), "manifest/1.json")
	require.NoError(t, err)

	_, err = preflightDeltaTier(context.Background(), runCtx, 1)
	require.ErrorIs(t, err, ErrIncompleteObjectListing)
	require.Contains(t, err.Error(), "list delta prefix delta/1/")
	require.Empty(t, client.deleted, "an incomplete inventory must not purge anything")
	after, _, err := st.Load(context.Background(), "manifest/1.json")
	require.NoError(t, err)
	require.Equal(t, before, after, "the manifest must not be swapped on a failed pre-flight")
}
