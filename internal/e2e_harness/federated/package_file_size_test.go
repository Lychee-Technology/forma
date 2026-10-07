package federated

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit prevents any concern in this harness
// package (query assembly, seeding, assertions, infrastructure) from
// accumulating back into an oversized source file (#220, #369). It watches
// non-test files only, because performance_test.go and consistency_test.go are
// over the cap; #433 splits them and then switches this guard to IncludeTests.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.ExcludeTests)
}
