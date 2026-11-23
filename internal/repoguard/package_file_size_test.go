package repoguard

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file of this package,
// all of them tests, under the 500-line cap from coding-standard.md. It reads
// the package directory: the guards' TestMain resolves the repository root
// without changing the working directory.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
