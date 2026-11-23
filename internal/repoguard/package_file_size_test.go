package repoguard

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file of this package,
// all of them tests, under the 500-line cap from coding-standard.md. It reads
// the package directory from the repository root TestMain resolved, never the
// working directory: a guard binary launched from the module root would
// otherwise measure the root package's files and pass in silence.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	dir, err := packageFS()
	if err != nil {
		t.Fatalf("open %s: %v", packageDir, err)
	}
	sizeguard.CheckFS(t, dir, sizeguard.IncludeTests)
}
