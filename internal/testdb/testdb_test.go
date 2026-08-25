package testdb

import (
	"strings"
	"testing"
)

// clearDBEnv blanks every variable ResolveDSN reads so ambient shell/CI env
// cannot leak into a test case. t.Setenv also registers restoration.
func clearDBEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL", "DB_HOST", "DB_PORT", "DB_NAME",
		"DB_USER", "DB_PASSWORD", "DB_SSL_MODE",
	} {
		t.Setenv(key, "")
	}
}

func TestResolveDSNDefaultsMatchLocalServer(t *testing.T) {
	clearDBEnv(t)
	got := ResolveDSN()
	want := "postgres://postgres:postgres@localhost:5432/forma?sslmode=disable"
	if got != want {
		t.Errorf("ResolveDSN() with no env = %q, want local_server.sh defaults %q", got, want)
	}
}

func TestResolveDSNReadsDBVars(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_HOST", "db.example")
	t.Setenv("DB_PORT", "15432")
	t.Setenv("DB_NAME", "forma_test")
	t.Setenv("DB_USER", "test")
	t.Setenv("DB_PASSWORD", "test")
	t.Setenv("DB_SSL_MODE", "require")
	got := ResolveDSN()
	want := "postgres://test:test@db.example:15432/forma_test?sslmode=require"
	if got != want {
		t.Errorf("ResolveDSN() = %q, want %q", got, want)
	}
}

func TestResolveDSNEscapesPassword(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_PASSWORD", "p@ss/word")
	got := ResolveDSN()
	if !strings.Contains(got, "p%40ss%2Fword") {
		t.Errorf("ResolveDSN() = %q, want URL-escaped password p%%40ss%%2Fword", got)
	}
}

func TestResolveDSNPrefersDatabaseURL(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_HOST", "ignored.example")
	t.Setenv("DATABASE_URL", "postgres://u:p@override:5/db")
	if got := ResolveDSN(); got != "postgres://u:p@override:5/db" {
		t.Errorf("ResolveDSN() = %q, want DATABASE_URL to win over DB_*", got)
	}
}

func TestFailOnUnreachableFollowsCIVar(t *testing.T) {
	t.Setenv("CI", "")
	if FailOnUnreachable() {
		t.Error("FailOnUnreachable() = true with CI unset, want false (developer machines skip)")
	}
	t.Setenv("CI", "true")
	if !FailOnUnreachable() {
		t.Error("FailOnUnreachable() = false with CI=true, want true (CI provisions Postgres on purpose)")
	}
}
