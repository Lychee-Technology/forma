package errorid_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/errorid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// observeGlobal installs an observer at level as the global logger for the
// rest of the test.
func observeGlobal(t *testing.T, level zapcore.LevelEnabler) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(level)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
	return logs
}

// TestLogIssuesAParseableUUID pins the shape both write surfaces rely on: the
// httpapi tests uuid.Parse every error_id they read off a body, and an
// operator greps the log for the string verbatim, so the id must be a
// canonical UUID string and nothing looser.
func TestLogIssuesAParseableUUID(t *testing.T) {
	observeGlobal(t, zap.InfoLevel)
	id := errorid.Log(zap.WarnLevel, "failed")
	parsed, err := uuid.Parse(id)
	require.NoError(t, err)
	require.Equal(t, parsed.String(), id, "the id must already be in canonical form")
}

// TestLogDoesNotRepeat is the correlation property: two failures must never
// share a handle, or the join from body to log line becomes ambiguous.
func TestLogDoesNotRepeat(t *testing.T) {
	observeGlobal(t, zap.InfoLevel)
	require.NotEqual(t, errorid.Log(zap.WarnLevel, "failed"), errorid.Log(zap.WarnLevel, "failed"))
}

// TestLogWritesTheIdItReturns is the join itself: the returned id is on the
// line, under error_id, beside the caller's fields, at the requested level
// and under LoggerName.
func TestLogWritesTheIdItReturns(t *testing.T) {
	logs := observeGlobal(t, zap.InfoLevel)
	id := errorid.Log(zap.ErrorLevel, "failed", "error", "boom")

	entries := logs.All()
	require.Len(t, entries, 1)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	require.Equal(t, errorid.LoggerName, entries[0].LoggerName)
	require.Equal(t, map[string]any{"error": "boom", "error_id": id}, entries[0].ContextMap())
}

// TestLogIssuesNoIdForALineItWillNotWrite is the other half of the contract:
// under a logger set above the line's level — an embedder's Error-level
// config against a Warn line, or zap's default no-op global — the line is
// dropped before any sampler exemption can act, so Log must return no id
// rather than a handle to nothing.
func TestLogIssuesNoIdForALineItWillNotWrite(t *testing.T) {
	t.Run("level above the line", func(t *testing.T) {
		logs := observeGlobal(t, zap.ErrorLevel)
		require.Empty(t, errorid.Log(zap.WarnLevel, "failed"))
		require.Zero(t, logs.Len())
		require.NotEmpty(t, errorid.Log(zap.ErrorLevel, "failed"), "a line at the threshold still gets an id")
	})
	t.Run("no-op global", func(t *testing.T) {
		t.Cleanup(zap.ReplaceGlobals(zap.NewNop()))
		require.Empty(t, errorid.Log(zap.ErrorLevel, "failed"))
	})
}

// raisingCore stands in for an embedder that raises an AtomicLevel under live
// traffic: the first time the logger consults it — at Enabled, or at Check —
// it lets the answer through and then raises level to Error, so any later
// level check in the same write sees the new threshold.
type raisingCore struct {
	zapcore.Core
	level   zap.AtomicLevel
	raiseAt string
}

func (c *raisingCore) raise(at string) {
	if c.raiseAt == at {
		c.level.SetLevel(zap.ErrorLevel)
	}
}

func (c *raisingCore) Enabled(level zapcore.Level) bool {
	defer c.raise("enabled")
	return c.Core.Enabled(level)
}

func (c *raisingCore) With(fields []zapcore.Field) zapcore.Core {
	return &raisingCore{Core: c.Core.With(fields), level: c.level, raiseAt: c.raiseAt}
}

func (c *raisingCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	defer c.raise("check")
	return c.Core.Check(entry, checked)
}

// TestLogIdMatchesALineWhenTheLevelRisesMidWrite pins the contract against a
// level raised between Log's decision and its write: the id is returned if
// and only if the line carrying it was written. Minting after an Enabled
// probe and then writing through Logw, which checks the level again, lost
// the line and kept the id when the raise landed between the two checks.
func TestLogIdMatchesALineWhenTheLevelRisesMidWrite(t *testing.T) {
	for _, raiseAt := range []string{"enabled", "check"} {
		t.Run("raised after "+raiseAt, func(t *testing.T) {
			level := zap.NewAtomicLevelAt(zap.WarnLevel)
			observed, logs := observer.New(level)
			core := &raisingCore{Core: observed, level: level, raiseAt: raiseAt}
			t.Cleanup(zap.ReplaceGlobals(zap.New(core)))

			id := errorid.Log(zap.WarnLevel, "failed")

			require.Equal(t, zap.ErrorLevel, level.Level(), "the level must have risen during the write")
			written := logs.FilterField(zap.String("error_id", id)).Len()
			if id == "" {
				require.Zero(t, logs.Len(), "no id, so no line")
			} else {
				require.Equal(t, 1, written, "an issued id must lead to its line")
			}
		})
	}
}
