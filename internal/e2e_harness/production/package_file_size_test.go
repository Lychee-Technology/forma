package production

import (
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
// schema_evolution_e2e_test.go was at 492 lines when this guard landed (#449,
// on #452's watch list). The guard reads files rather than compiling them, so
// it measures the e2e-tagged files too although a plain go test leaves them out.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
