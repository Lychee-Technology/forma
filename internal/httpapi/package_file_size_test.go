package httpapi

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit prevents handler and helper concerns
// from accumulating back into an oversized source file (#220). Test files are
// watched too: none was ever over the cap here, so nothing held them out (#449).
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
