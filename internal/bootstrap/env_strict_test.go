package bootstrap

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
)

// envErrorKeys walks an error tree, errors.Join branches included, and returns
// the Key of every *EnvError in it, in order. errors.As stops at the first
// match, which cannot show that a *FromEnv function named every bad variable.
func envErrorKeys(err error) []string {
	if err == nil {
		return nil
	}
	if envErr, ok := err.(*EnvError); ok {
		return []string{envErr.Key}
	}
	var keys []string
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			keys = append(keys, envErrorKeys(child)...)
		}
	case interface{ Unwrap() error }:
		keys = envErrorKeys(wrapped.Unwrap())
	}
	return keys
}

func assertEnvErrorKeys(t *testing.T, err error, want ...string) {
	t.Helper()
	if got := envErrorKeys(err); !slices.Equal(got, want) {
		t.Fatalf("expected *EnvError for %v, got %v (err: %v)", want, got, err)
	}
}

func TestEnvIntStrictContract(t *testing.T) {
	const key = "BOOTSTRAP_INT_TEST"

	got, err := EnvInt(key+"_UNSET", 42)
	if err != nil || got != 42 {
		t.Fatalf("unset must take the default: got %d, %v", got, err)
	}
	t.Setenv(key, "")
	if got, err = EnvInt(key, 42); err != nil || got != 42 {
		t.Fatalf("empty must take the default: got %d, %v", got, err)
	}
	t.Setenv(key, "-7")
	if got, err = EnvInt(key, 42); err != nil || got != -7 {
		t.Fatalf("a parsable value must be read: got %d, %v", got, err)
	}

	cases := []struct {
		value string
		cause error
	}{
		{"invalid", strconv.ErrSyntax},
		{"30s", strconv.ErrSyntax},
		{"1_000", strconv.ErrSyntax},
		{" 5", strconv.ErrSyntax},
		{"1.5", strconv.ErrSyntax},
		{"99999999999999999999", strconv.ErrRange},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(key, tc.value)
			_, err := EnvInt(key, 42)
			var envErr *EnvError
			if !errors.As(err, &envErr) {
				t.Fatalf("expected *EnvError for %q, got %v", tc.value, err)
			}
			if envErr.Key != key || envErr.Value != tc.value {
				t.Fatalf("EnvError must carry the key and raw value, got %+v", envErr)
			}
			if !errors.Is(err, tc.cause) {
				t.Fatalf("expected cause %v, got %v", tc.cause, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, key) || !strings.Contains(msg, strconv.Quote(tc.value)) {
				t.Fatalf("message must name the variable and quote the value: %s", msg)
			}
		})
	}
}

// TestEnvSecondsRefusesDurationOverflow pins that a seconds value too large
// for a time.Duration is refused rather than wrapped: 18446744074 seconds
// would otherwise multiply out to about 0.29s.
func TestEnvSecondsRefusesDurationOverflow(t *testing.T) {
	const key = "BOOTSTRAP_SECONDS_TEST"

	t.Setenv(key, strconv.FormatInt(maxEnvSeconds, 10))
	if got, err := envSeconds(key, time.Second); err != nil || got != time.Duration(maxEnvSeconds)*time.Second {
		t.Fatalf("the largest representable value must be read: got %s, %v", got, err)
	}

	for _, value := range []string{"18446744074", strconv.FormatInt(maxEnvSeconds+1, 10), strconv.FormatInt(-maxEnvSeconds-1, 10)} {
		t.Setenv(key, value)
		_, err := envSeconds(key, time.Second)
		var envErr *EnvError
		if !errors.As(err, &envErr) || envErr.Key != key || envErr.Value != value {
			t.Fatalf("expected *EnvError for %s=%s, got %v", key, value, err)
		}
		if !errors.Is(err, strconv.ErrRange) {
			t.Fatalf("an overflowing duration is a range error, got %v", err)
		}
	}
}

func TestDatabaseConfigFromEnvRejectsUnparsable(t *testing.T) {
	t.Setenv("DB_PORT", "tcp://10.0.0.7:5432")
	cfg, err := DatabaseConfigFromEnv(testDBDefaults)
	assertEnvErrorKeys(t, err, "DB_PORT")
	if cfg != (forma.DatabaseConfig{}) {
		t.Fatalf("a failed overlay must not return a half-read config: %+v", cfg)
	}

	// Every bad variable is named, not only the first.
	t.Setenv("DB_MAX_IDLE_CONNS", "two")
	t.Setenv("DB_TIMEOUT_SECONDS", "30s")
	_, err = DatabaseConfigFromEnv(testDBDefaults)
	assertEnvErrorKeys(t, err, "DB_PORT", "DB_MAX_IDLE_CONNS", "DB_TIMEOUT_SECONDS")
}

func TestHTTPServerConfigFromEnvRejectsUnparsable(t *testing.T) {
	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "not-a-number")
	_, err := HTTPServerConfigFromEnv(DefaultHTTPServerConfig())
	assertEnvErrorKeys(t, err, "HTTP_READ_TIMEOUT_SECONDS")

	t.Setenv("HTTP_MAX_HEADER_BYTES", "1MiB")
	_, err = HTTPServerConfigFromEnv(DefaultHTTPServerConfig())
	assertEnvErrorKeys(t, err, "HTTP_READ_TIMEOUT_SECONDS", "HTTP_MAX_HEADER_BYTES")
}

func TestApplyLimitsFromEnvRejectsUnparsable(t *testing.T) {
	// A parsable variable next to the bad ones must not be applied either:
	// a failed overlay leaves cfg untouched.
	t.Setenv("MAX_BATCH_SIZE", "250")
	t.Setenv("QUERY_TIMEOUT_SECONDS", "30s")
	t.Setenv("MAX_ENTITY_SIZE_BYTES", "1_000")

	cfg := forma.DefaultConfig(nil)
	want := *forma.DefaultConfig(nil)
	err := ApplyLimitsFromEnv(cfg)
	assertEnvErrorKeys(t, err, "MAX_ENTITY_SIZE_BYTES", "QUERY_TIMEOUT_SECONDS")
	if cfg.Entity.MaxEntitySize != want.Entity.MaxEntitySize || cfg.Performance.MaxBatchSize != want.Performance.MaxBatchSize ||
		cfg.Query.DefaultTimeout != want.Query.DefaultTimeout {
		t.Fatalf("a failed overlay must leave cfg untouched: entity=%d batch=%d query=%s",
			cfg.Entity.MaxEntitySize, cfg.Performance.MaxBatchSize, cfg.Query.DefaultTimeout)
	}
}
