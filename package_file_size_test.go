package forma

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps the package's non-test files
// under the 500-line cap from coding-standard.md. types.go once reached 501
// lines with no guard to notice, and config.go was at 491 when this guard
// landed (#449). It excludes tests because types_test.go is over the cap; #451
// splits it and then switches this guard to IncludeTests.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.ExcludeTests)
}
