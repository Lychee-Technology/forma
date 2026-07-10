package production

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedactedPGDSN_OmitsPassword(t *testing.T) {
	e := &Env{
		Cluster: &Cluster{
			PGUser:     "svc",
			PGPassword: "s3cret-hunter2",
			PGHost:     "db.internal",
			PGPort:     "5432",
			PGSSLMode:  "require",
		},
		DBName: "e2e_1",
	}

	redacted := e.redactedPGDSN()
	if strings.Contains(redacted, "s3cret-hunter2") {
		t.Fatalf("redacted DSN still contains the password: %s", redacted)
	}
	if !strings.Contains(redacted, redactedPassword) {
		t.Fatalf("redacted DSN missing the %s marker: %s", redactedPassword, redacted)
	}
	// The unredacted DSN keeps working for live connections.
	if !strings.Contains(e.PGDSN(), "s3cret-hunter2") {
		t.Fatal("PGDSN no longer carries the real password")
	}
}

func TestWriteJSONArtifact_OwnerOnlyPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := writeJSONArtifact(dir, "run.json", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("writeJSONArtifact: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatalf("stat artifact: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("artifact permissions = %o, want 600", perm)
	}
}

func TestDumpDiff_AlwaysWritesStructuredDocument(t *testing.T) {
	dir := t.TempDir()
	e := &Env{}

	if err := e.dumpDiff(t.Context(), dir); err != nil {
		t.Fatalf("dumpDiff without a recorded diff: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "diff.json"))
	if err != nil {
		t.Fatalf("diff.json missing for non-query failure: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("diff.json is not valid JSON: %v", err)
	}
	if payload["status"] != "no_query_mismatch_recorded" {
		t.Fatalf("diff.json status = %v, want no_query_mismatch_recorded", payload["status"])
	}
}

func TestEnforceContainerRetention_DisablesRyukUnderKeepEnv(t *testing.T) {
	t.Setenv(ryukDisabledVar, "")
	_ = os.Unsetenv(ryukDisabledVar) // t.Setenv registered the restore; test empty-unset state

	t.Setenv(KeepEnvVar, "1")
	enforceContainerRetention()
	if got := os.Getenv(ryukDisabledVar); got != "true" {
		t.Fatalf("%s = %q after enforceContainerRetention under KEEP_E2E_ENV=1, want true", ryukDisabledVar, got)
	}
}

func TestEnforceContainerRetention_RespectsExplicitRyukSetting(t *testing.T) {
	t.Setenv(KeepEnvVar, "1")
	t.Setenv(ryukDisabledVar, "false")
	enforceContainerRetention()
	if got := os.Getenv(ryukDisabledVar); got != "false" {
		t.Fatalf("%s = %q, want the caller's explicit false preserved", ryukDisabledVar, got)
	}
}

func TestEnforceContainerRetention_NoopWithoutKeepEnv(t *testing.T) {
	t.Setenv(KeepEnvVar, "")
	t.Setenv(ryukDisabledVar, "")
	_ = os.Unsetenv(ryukDisabledVar)
	enforceContainerRetention()
	if got := os.Getenv(ryukDisabledVar); got != "" {
		t.Fatalf("%s = %q without KEEP_E2E_ENV, want unset", ryukDisabledVar, got)
	}
}
