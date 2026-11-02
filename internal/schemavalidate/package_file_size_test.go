package schemavalidate

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
// validator_test.go was at exactly 500 lines when this guard landed (#449);
// #408 split it into resolve_test.go, validator_new_test.go and
// validator_test.go, so new tests have room in the file that fits them.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
