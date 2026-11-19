package cdc

import (
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The constructor wires the run's V2 writer, its object store and the
// engine's schema lock, and the change_log table falls back to its default.
func TestNewSchemaFlushContextWiresTheWriter(t *testing.T) {
	db := &sql.DB{}
	store := &manifestv2.FSStore{Dir: t.TempDir()}

	flushCtx, err := newSchemaFlushContext(flushContextParams{cfg: CDCConfig{S3Bucket: "b"}, db: db, store: store, logger: zap.NewNop()})
	require.NoError(t, err)
	require.NotNil(t, flushCtx.writer)
	require.Same(t, store, flushCtx.store)
	require.Equal(t, "change_log", flushCtx.tableName)
	require.IsType(t, advisorySchemaLocker{}, flushCtx.locker)

	custom, err := newSchemaFlushContext(flushContextParams{
		cfg: CDCConfig{ChangeLogTable: "custom_log", Engine: forma.EngineAuroraDSQL}, db: db, store: store,
	})
	require.NoError(t, err)
	require.Equal(t, "custom_log", custom.tableName)
	require.IsType(t, &leaseSchemaLocker{}, custom.locker)
	require.NotNil(t, custom.logger, "a nil logger falls back to a no-op one")
}

// A flush context without a store, or with a batch a PostgreSQL mark cannot
// bind, is refused before any schema is touched.
func TestNewSchemaFlushContextRefusesWhatTheWriterCannotRun(t *testing.T) {
	_, err := newSchemaFlushContext(flushContextParams{cfg: CDCConfig{}, db: &sql.DB{}})
	require.ErrorContains(t, err, "object store")

	store := &manifestv2.FSStore{Dir: t.TempDir()}
	_, err = newSchemaFlushContext(flushContextParams{cfg: CDCConfig{BatchSize: cdcv2.MaxMarkEntries + 1}, db: &sql.DB{}, store: store})
	require.ErrorIs(t, err, ErrBatchSizeTooLarge)

	_, err = newSchemaFlushContext(flushContextParams{cfg: CDCConfig{Engine: "oracle"}, db: &sql.DB{}, store: store})
	require.Error(t, err, "an unknown engine is refused")
}

// Both flush entrypoints, package-level RunOnce (cmd/tools cdc-flush) and
// Runner.RunOnce, must reach the flush through runFlushPass with the run's
// object store, and only runFlushPass builds a flush context. Neither frame
// has DB and AWS preconditions a unit test can satisfy, so this keeps the
// wiring from existing on one path only: a hand-rolled schemaFlushContext
// at either site, or a params literal that drops the store, would publish
// nothing on that path while the constructor's own tests stay green.
func TestRunOnceFramesBuildFlushContextThroughConstructor(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()

	var literalSites, constructorCallers, runOnceFrames []string
	framePassesStore := map[string]bool{}
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
			if fn.Name.Name == "RunOnce" {
				runOnceFrames = append(runOnceFrames, frame)
			}
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
				switch ident, _ := call.Fun.(*ast.Ident); {
				case ident == nil:
				case ident.Name == "newSchemaFlushContext":
					constructorCallers = append(constructorCallers, frame)
				case ident.Name == "runFlushPass":
					framePassesStore[frame] = callSetsField(call, "store")
				}
				return true
			})
		}
	}

	require.Len(t, runOnceFrames, 2, "expected exactly two RunOnce frames (package-level and Runner), found %v", runOnceFrames)
	for _, frame := range runOnceFrames {
		require.True(t, framePassesStore[frame], "%s must run runFlushPass with the run's object store", frame)
	}
	require.Equal(t, []string{"flusher.go:runFlushPass"}, constructorCallers, "only runFlushPass builds a flush context")
	require.Len(t, literalSites, 1, "schemaFlushContext must be built only by newSchemaFlushContext; found literals in %v", literalSites)
	require.Contains(t, literalSites[0], "newSchemaFlushContext")
}

// callSetsField reports whether call's last argument is a composite literal
// that sets field.
func callSetsField(call *ast.CallExpr, field string) bool {
	if len(call.Args) == 0 {
		return false
	}
	lit, ok := call.Args[len(call.Args)-1].(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}
		if key, isIdent := kv.Key.(*ast.Ident); isIdent && key.Name == field {
			return true
		}
	}
	return false
}
