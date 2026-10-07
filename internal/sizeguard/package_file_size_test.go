package sizeguard

import "testing"

// TestPackageFilesStayWithinFileSizeLimit is this package's own guard, and the
// one run of Check against a real directory that the package itself owns.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	Check(t, IncludeTests)
}
