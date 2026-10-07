package internal

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md, so
// split files cannot silently accrete back (#320).
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
