package forma

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
// types.go once reached 501 lines with no guard to notice, and config.go was
// at 491 when the guard landed (#449). It watched non-test files only until
// #451 split the 686-line types_test.go.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
