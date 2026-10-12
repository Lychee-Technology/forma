package conditionexpr

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestParseServerTimeOffsets(t *testing.T) {
	cases := map[string]int64{
		"$now":        0,
		"$now-30s":    -30000,
		"$now+5m":     300000,
		"$now+1h2m3s": 3723000,
		"$now-1.5s":   -1500,
		"$now+250ms":  250,
		"$now-0s":     0,
	}
	for value, want := range cases {
		got, err := ParseServerTime("next_attempt_at", forma.ValueTypeDateTime, value)
		require.NoError(t, err, value)
		require.Equal(t, want, got.OffsetMs, value)
	}
}

func TestParseServerTimeRefusesMalformedOffsets(t *testing.T) {
	cases := map[string]string{
		"$now+":      `invalid $now operand "$now+" for datetime attribute 'locked_until'`,
		"$now-1x":    `invalid $now operand "$now-1x" for datetime attribute 'locked_until'`,
		"$now5m":     `invalid $now operand "$now5m"`,
		"$nowish":    `invalid $now operand "$nowish"`,
		"$now+-5s":   `invalid $now operand "$now+-5s"`,
		"$now+1.5ms": `the offset must be a whole number of milliseconds`,
		"$now-10us":  `the offset must be a whole number of milliseconds`,
	}
	for value, want := range cases {
		_, err := ParseServerTime("locked_until", forma.ValueTypeDateTime, value)
		require.Error(t, err, value)
		require.ErrorIs(t, err, forma.ErrInvalidInput, value)
		msg, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok, value)
		require.Contains(t, msg, want, value)
	}
}

func TestIsServerTime(t *testing.T) {
	require.True(t, IsServerTime("$now"))
	require.True(t, IsServerTime("$now-30s"))
	require.True(t, IsServerTime("$nowish"), "a malformed offset is still refused by name")
	require.False(t, IsServerTime("now"))
	require.False(t, IsServerTime("2024-01-02T03:04:05Z"))
}
