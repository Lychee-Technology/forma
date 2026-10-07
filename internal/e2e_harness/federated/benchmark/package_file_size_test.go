package benchmark

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
// trend.go was at 495 lines when this guard landed (#449, on #452's watch
// list).
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
