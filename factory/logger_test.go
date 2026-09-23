package factory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/errorid"
	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// loggedLine is the part of a production JSON log line the tests read.
type loggedLine struct {
	Msg     string `json:"msg"`
	Logger  string `json:"logger"`
	ErrorID string `json:"error_id"`
}

// buildEmbedderLogger does what an embedder is told to do, against a file
// the test can read back: the real production config — JSON, Info, the
// 100:100 sampler — built through the public BuildLogger. The clock is
// frozen so every line lands in one sampler tick. Returns the logger, for
// the test to install as the global, and a reader for the lines written so
// far. level replaces the production Info threshold, as an embedder's own
// config may.
func buildEmbedderLogger(t *testing.T, level zapcore.Level) (*zap.Logger, func() []loggedLine) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "forma.log")
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.OutputPaths = []string{path}
	logger, err := BuildLogger(cfg, erroridtest.FrozenClock())
	require.NoError(t, err)

	return logger, func() []loggedLine {
		require.NoError(t, logger.Sync())
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var lines []loggedLine
		for _, text := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if text == "" {
				continue
			}
			var line loggedLine
			require.NoError(t, json.Unmarshal([]byte(text), &line), "not a JSON log line: %s", text)
			lines = append(lines, line)
		}
		return lines
	}
}

// embedderBatchFailures is the failure storm from the outside: a manager
// built the way an external project builds one, 150 ordinary warnings to
// prove the sampler is live, then one best-effort batch of 150 operations
// that all fail — inside the default MaxBatchSize of 1000 and past the
// sampler's 100.
const embedderBatchFailures = 150

func runEmbedderBatchFailures(t *testing.T) *forma.BatchResult {
	t.Helper()
	manager, err := newEntityManagerWithConfigContext(context.Background(), newUnitEntityManagerConfig(t), nil,
		buildUnitEntityManagerDeps(schemameta.NewMetadataCache()))
	require.NoError(t, err)

	operations := make([]forma.EntityOperation, embedderBatchFailures)
	for i := range operations {
		operations[i] = forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "nosuchschema"},
			Type:             forma.OperationCreate,
			Data:             map[string]any{"i": i},
		}
	}
	for i := 0; i < embedderBatchFailures; i++ {
		zap.S().Warnw("an ordinary warning", "i", i)
	}

	result, err := manager.BatchCreate(context.Background(), &forma.BatchOperation{Operations: operations})
	require.NoError(t, err)
	require.Len(t, result.Failed, embedderBatchFailures)
	return result
}

// requireEveryIDJoinsOneLine: each returned id appears on exactly one written
// line, under wantLogger, while the ordinary lines were cut to the sampler's
// 100 — so it is the exemption, not a dead sampler, that kept them.
func requireEveryIDJoinsOneLine(t *testing.T, result *forma.BatchResult, lines []loggedLine, wantLogger string) {
	t.Helper()
	linesByID := map[string]int{}
	ordinary := 0
	for _, line := range lines {
		switch line.Msg {
		case "BatchCreate operation failed":
			require.Equal(t, wantLogger, line.Logger, "the correlation line names its logger")
			linesByID[line.ErrorID]++
		case "an ordinary warning":
			ordinary++
		}
	}
	require.Equal(t, 100, ordinary, "the production sampler is live for every other line")
	for _, failure := range result.Failed {
		_, err := uuid.Parse(failure.ErrorID)
		require.NoError(t, err)
		require.Equal(t, 1, linesByID[failure.ErrorID],
			"id %s must join exactly one written line; the sampler must not have dropped it", failure.ErrorID)
	}
}

// TestEmbedderBatchFailureIDsSurviveProductionSampling is the review finding
// on #398 from the outside: an external project builds its manager through
// this package and cannot reach internal/bootstrap, so the sampler exemption
// has to be installable through the public surface for the OperationError
// contract to hold there.
func TestEmbedderBatchFailureIDsSurviveProductionSampling(t *testing.T) {
	logger, readLog := buildEmbedderLogger(t, zap.InfoLevel)
	t.Cleanup(zap.ReplaceGlobals(logger))

	result := runEmbedderBatchFailures(t)

	requireEveryIDJoinsOneLine(t, result, readLog(), "errorid")
}

// TestEmbedderBatchFailureIDsSurviveProductionSamplingUnderANamedGlobal is
// the same contract for an embedder that names its global logger —
// zap.ReplaceGlobals(logger.Named("svc")) — so the correlation line arrives
// as "svc.errorid". The exemption keys on Forma's own marker, not the
// name, so the embedder's naming cannot send these lines back through the
// sampler.
func TestEmbedderBatchFailureIDsSurviveProductionSamplingUnderANamedGlobal(t *testing.T) {
	logger, readLog := buildEmbedderLogger(t, zap.InfoLevel)
	t.Cleanup(zap.ReplaceGlobals(logger.Named("svc")))

	result := runEmbedderBatchFailures(t)

	requireEveryIDJoinsOneLine(t, result, readLog(), "svc.errorid")
}

// TestEmbedderBatchFailuresIssueNoIDAboveWarn: BuildLogger keeps a caller's
// level, and zap tests the level before the sampler exemption sees the line,
// so under an Error-level config the Warn correlation line is never written.
// A failure must then carry no error_id at all — never one that leads
// nowhere — while still reporting its index and published message.
func TestEmbedderBatchFailuresIssueNoIDAboveWarn(t *testing.T) {
	logger, readLog := buildEmbedderLogger(t, zap.ErrorLevel)
	t.Cleanup(zap.ReplaceGlobals(logger))

	result := runEmbedderBatchFailures(t)

	for i, failure := range result.Failed {
		require.Empty(t, failure.ErrorID, "failure %d carries an id whose line an Error-level logger dropped", i)
		require.Equal(t, i, failure.Index)
		require.NotEmpty(t, failure.Error)
	}
	for _, line := range readLog() {
		require.NotEqual(t, "BatchCreate operation failed", line.Msg, "the Warn line cannot clear an Error threshold")
	}
}

// TestNewProductionLoggerIsTheBootstrapLogger: the public constructor has to
// be the production logger, not a variant — Info threshold, JSON, sampled —
// so an embedder that follows the documented setup runs what cmd/server runs.
func TestNewProductionLoggerIsTheBootstrapLogger(t *testing.T) {
	logger, err := NewProductionLogger()
	require.NoError(t, err)
	require.Equal(t, zap.InfoLevel, logger.Level())
	require.False(t, logger.Core().Enabled(zap.DebugLevel))
}

// TestSamplerOptionExemptsCorrelationLinesOverAHandBuiltCore covers the
// embedder who assembles a core rather than a config and installs the
// sampler as an option, the way zap.Config.Build would have: with
// SamplerOption in the sampler's place, Forma's correlation line clears it
// and every other line is still cut — including the embedder's own lines
// under a logger it happened to name "errorid", which must not borrow the
// exemption (PR #606 review).
func TestSamplerOptionExemptsCorrelationLinesOverAHandBuiltCore(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core, erroridtest.FrozenClock(), SamplerOption(zap.NewProductionConfig().Sampling))
	t.Cleanup(zap.ReplaceGlobals(logger))

	for i := 0; i < 150; i++ {
		require.NotEmpty(t, errorid.Log(zap.WarnLevel, "correlated"))
		logger.Sugar().Warnw("uncorrelated", "i", i)
		logger.Sugar().Named("errorid").Warnw("host line under the same name", "i", i)
	}

	require.Len(t, logs.FilterMessage("correlated").All(), 150)
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100)
	require.Len(t, logs.FilterMessage("host line under the same name").All(), 100)
}
