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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The checksum seam is built from the run's full client, so a flush context
// assembled from a manifest-capable client can hash the object it just
// published (#347).
func TestNewSchemaFlushContextWiresChecksumSeam(t *testing.T) {
	full := &hashableFullS3Client{body: []byte("hello parquet")}

	flushCtx := newSchemaFlushContext(flushContextParams{
		cfg:     CDCConfig{S3Bucket: "test-bucket"},
		clients: resolvedS3Clients{object: full, full: full},
		logger:  zap.NewNop(),
	})

	require.NotNil(t, flushCtx.checksumObject, "flush context built without the checksum seam")
	got, err := flushCtx.checksumObject(context.Background(), "cdc/7/delta-file.parquet")
	require.NoError(t, err)
	// sha256("hello parquet")
	require.Equal(t, "sha256:950423965d5b936670f1549c58ce0594b58e1027c2b5e1e2a4f1515b1bc2f1b0", got)
}

// A run whose caller supplied an object-only client has nothing to hash with,
// so the seam stays nil and entries go unstamped rather than panicking.
func TestNewSchemaFlushContextLeavesChecksumSeamNilWithoutFullClient(t *testing.T) {
	object := &objectOnlyS3Client{}

	flushCtx := newSchemaFlushContext(flushContextParams{
		cfg:     CDCConfig{S3Bucket: "test-bucket"},
		clients: resolvedS3Clients{object: object},
		logger:  zap.NewNop(),
	})

	require.Nil(t, flushCtx.checksumObject)
	require.Same(t, object, flushCtx.s3Client)
}

// The manifest store and resolver are the other S3-derived fields the
// constructor owns; they follow the configured template, and the change_log
// table name falls back to the default.
func TestNewSchemaFlushContextWiresManifestStoreAndTableDefault(t *testing.T) {
	full := &fullS3ClientMock{}

	withManifest := newSchemaFlushContext(flushContextParams{
		cfg: CDCConfig{
			S3Bucket:         "test-bucket",
			ManifestPrefix:   "cdc",
			ManifestTemplate: "manifest/{{.SchemaID}}.json",
		},
		clients: resolvedS3Clients{object: full, full: full},
		logger:  zap.NewNop(),
	})
	require.NotNil(t, withManifest.manifestStore)
	require.Equal(t, "manifest/{{.SchemaID}}.json", withManifest.manifestResolver.PathTemplate)
	require.Equal(t, "cdc", withManifest.manifestResolver.Prefix)
	require.Equal(t, "change_log", withManifest.tableName)

	withoutManifest := newSchemaFlushContext(flushContextParams{
		cfg:     CDCConfig{S3Bucket: "test-bucket", ChangeLogTable: "custom_log"},
		clients: resolvedS3Clients{object: full, full: full},
		logger:  zap.NewNop(),
	})
	require.Nil(t, withoutManifest.manifestStore)
	require.Equal(t, "custom_log", withoutManifest.tableName)
}

// Both flush entrypoints — package-level RunOnce (cmd/tools cdc-flush) and
// Runner.RunOnce — must assemble their context through newSchemaFlushContext,
// handing it the resolved client pair. Neither frame has DB and AWS
// preconditions a unit test can satisfy, so this is what keeps the wiring from
// existing on one path only: a hand-rolled schemaFlushContext literal at either
// site, or a params literal that drops the clients, silently unstamps every
// manifest entry that path writes while the constructor's own tests stay green
// (#318, #347).
func TestRunOnceFramesBuildFlushContextThroughConstructor(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()

	var literalSites []string
	var runOnceFrames []string
	frameBuildsThroughConstructor := map[string]bool{}
	framePassesClients := map[string]bool{}

	for _, entry := range entries {
		path := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
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

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, isLit := n.(*ast.CompositeLit); isLit {
					if ident, isIdent := lit.Type.(*ast.Ident); isIdent && ident.Name == "schemaFlushContext" {
						literalSites = append(literalSites, frame)
					}
				}
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				ident, isIdent := call.Fun.(*ast.Ident)
				if !isIdent || ident.Name != "newSchemaFlushContext" {
					return true
				}
				frameBuildsThroughConstructor[frame] = true
				framePassesClients[frame] = callPassesResolvedClients(call)
				return true
			})

			if fn.Name.Name == "RunOnce" {
				runOnceFrames = append(runOnceFrames, frame)
			}
		}
	}

	require.Len(t, runOnceFrames, 2, "expected exactly two RunOnce frames (package-level and Runner), found %v", runOnceFrames)
	for _, frame := range runOnceFrames {
		require.True(t, frameBuildsThroughConstructor[frame],
			"%s does not build its flush context through newSchemaFlushContext, so the checksum seam is unwired on that path", frame)
		require.True(t, framePassesClients[frame],
			"%s calls newSchemaFlushContext without passing the resolved clients, so the checksum seam and manifest store are nil on that path", frame)
	}

	require.Len(t, literalSites, 1,
		"schemaFlushContext must be built only by newSchemaFlushContext; found literals in %v", literalSites)
	require.Contains(t, literalSites[0], "newSchemaFlushContext")
}

// callPassesResolvedClients reports whether a newSchemaFlushContext call hands
// over a flushContextParams literal that sets the clients field.
func callPassesResolvedClients(call *ast.CallExpr) bool {
	if len(call.Args) != 1 {
		return false
	}
	lit, ok := call.Args[0].(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}
		if key, isIdent := kv.Key.(*ast.Ident); isIdent && key.Name == "clients" {
			return true
		}
	}
	return false
}
