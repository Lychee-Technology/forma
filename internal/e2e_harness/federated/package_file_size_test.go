package federated

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit prevents any concern in this harness
// package (query assembly, seeding, assertions, infrastructure) from
// accumulating back into an oversized source file (#220, #369). It watches
// _test.go files too (#433): the e2e scenario files are the ones that grew past
// the cap here, so an oversized one is split by scenario group, as
// performance_test.go and consistency_test.go were.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
