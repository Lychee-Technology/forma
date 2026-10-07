package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
// predicate_characterization_test.go sat at 497/500 when this guard landed;
// the natural way to extend a characterization matrix is to append a row, and
// this guard is what fires before the cap is crossed rather than after (#320).
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
