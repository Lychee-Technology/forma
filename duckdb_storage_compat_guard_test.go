package forma

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestCITestJobRunsDuckDBStorageCompat pins the #753 cross-version Parquet
// check to CI's test job, the job that runs on arm64. Comment lines are
// skipped so prose never satisfies the guard (same as the sibling ci.yml
// guards).
func TestCITestJobRunsDuckDBStorageCompat(t *testing.T) {
	wf, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	for _, line := range strings.Split(workflowJobBlock(t, string(wf), "test"), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") && strings.Contains(trimmed, "make duckdb-storage-compat") {
			return
		}
	}
	t.Error("ci.yml test job does not run `make duckdb-storage-compat`: a DuckDB bump or a COPY option change could write Parquet the previous release cannot read without CI noticing (#753)")
}

// TestStorageCompatPairsTwoDriverReleases fails when the probe's previous
// driver is the current one: the cross-version check would then compare a
// release with itself and pass vacuously. Moving go.mod to a new driver
// without moving scripts/duckdbcompat/previous.mod keeps an older pair,
// which this guard allows; docs/duckdb-upgrades.md says to move it.
func TestStorageCompatPairsTwoDriverReleases(t *testing.T) {
	current := duckdbDriverVersion(t, "go.mod")
	previous := duckdbDriverVersion(t, "scripts/duckdbcompat/previous.mod")
	if current == previous {
		t.Errorf("scripts/duckdbcompat/previous.mod pins duckdb-go %s, the same driver as go.mod: point it at the release a rollback returns to (docs/duckdb-upgrades.md)", previous)
	}
}

var duckdbDriverRequire = regexp.MustCompile(`(?m)^\s*(?:require\s+)?github\.com/duckdb/duckdb-go/v2\s+(v\S+)`)

func duckdbDriverVersion(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	m := duckdbDriverRequire.FindStringSubmatch(string(data))
	if m == nil {
		t.Fatalf("%s requires no github.com/duckdb/duckdb-go/v2 version", path)
	}
	return m[1]
}
