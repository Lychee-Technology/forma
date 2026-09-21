package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// loggedLine is the part of a production JSON log line the tests read.
type loggedLine struct {
	Msg     string `json:"msg"`
	Logger  string `json:"logger"`
	ErrorID string `json:"error_id"`
}

// installEmbedderLogger does what an embedder is told to do, against a file
// the test can read back: the real production config — JSON, Info, the
// 100:100 sampler — built through the public BuildLogger and installed as the
// global logger. The clock is frozen so every line lands in one sampler
// tick. Returns a reader for the lines written so far.
func installEmbedderLogger(t *testing.T) func() []loggedLine {
	t.Helper()
	path := filepath.Join(t.TempDir(), "forma.log")
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{path}
	logger, err := BuildLogger(cfg, erroridtest.FrozenClock())
	require.NoError(t, err)
	t.Cleanup(zap.ReplaceGlobals(logger))

	return func() []loggedLine {
		require.NoError(t, logger.Sync())
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var lines []loggedLine
		for _, text := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var line loggedLine
			require.NoError(t, json.Unmarshal([]byte(text), &line), "not a JSON log line: %s", text)
			lines = append(lines, line)
		}
		return lines
	}
}

// TestEmbedderBatchFailureIDsSurviveProductionSampling is the review finding
// on #398 from the outside: an external project builds its manager through
// this package and cannot reach internal/bootstrap, so the sampler exemption
// has to be installable through the public surface for the OperationError
// contract to hold there. 150 failed best-effort operations — inside the
// default MaxBatchSize of 1000 and past the sampler's 100 — must each return
// an id that appears on exactly one written line, while the plain lines the
// manager logs on the way stay under the sampler as before.
func TestEmbedderBatchFailureIDsSurviveProductionSampling(t *testing.T) {
	readLog := installEmbedderLogger(t)
	manager, err := newEntityManagerWithConfigContext(context.Background(), newUnitEntityManagerConfig(t), nil,
		buildUnitEntityManagerDeps(schemameta.NewMetadataCache()))
	require.NoError(t, err)

	const failures = 150
	operations := make([]forma.EntityOperation, failures)
	for i := range operations {
		operations[i] = forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "nosuchschema"},
			Type:             forma.OperationCreate,
			Data:             map[string]any{"i": i},
		}
	}
	for i := 0; i < failures; i++ {
		zap.S().Warnw("an ordinary warning", "i", i)
	}

	result, err := manager.BatchCreate(context.Background(), &forma.BatchOperation{Operations: operations})

	require.NoError(t, err)
	require.Len(t, result.Failed, failures)
	linesByID := map[string]int{}
	ordinary := 0
	for _, line := range readLog() {
		switch line.Msg {
		case "BatchCreate operation failed":
			require.Equal(t, "errorid", line.Logger, "the correlation line names its logger")
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
// SamplerOption in the sampler's place, the exempt line clears it and every
// other line is still cut.
func TestSamplerOptionExemptsCorrelationLinesOverAHandBuiltCore(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core, erroridtest.FrozenClock(), SamplerOption(zap.NewProductionConfig().Sampling))

	for i := 0; i < 150; i++ {
		logger.Sugar().Named("errorid").Warnw("correlated", "error_id", fmt.Sprint(i))
		logger.Sugar().Warnw("uncorrelated", "i", i)
	}

	require.Len(t, logs.FilterMessage("correlated").All(), 150)
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100)
}
