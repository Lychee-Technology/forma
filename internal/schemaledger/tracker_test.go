package schemaledger

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

type fakeTracker struct {
	loaded, active int64
	baselined      bool
}

func (f fakeTracker) LoadedGeneration(int16) (int64, bool) { return f.loaded, f.loaded != 0 }

func (f fakeTracker) ActiveGeneration(context.Context, int16) (int64, bool, error) {
	return f.active, f.baselined, nil
}

func TestRequireUnchanged(t *testing.T) {
	ctx := context.Background()
	if err := RequireUnchanged(ctx, fakeTracker{loaded: 3, active: 3, baselined: true}, 9); err != nil {
		t.Fatalf("an unchanged generation must pass, got %v", err)
	}
	if err := RequireUnchanged(ctx, fakeTracker{loaded: 3, active: 4, baselined: true}, 9); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("a moved generation = %v, want ErrGenerationChanged", err)
	}
	if err := RequireUnchanged(ctx, fakeTracker{loaded: 3}, 9); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("a cleared pointer = %v, want ErrGenerationChanged", err)
	}
	if err := RequireUnchanged(ctx, fakeTracker{active: 3, baselined: true}, 9); err == nil {
		t.Fatal("a schema the gate never admitted must not pass")
	}
	if err := RequireUnchanged(ctx, struct{}{}, 9); err != nil {
		t.Fatalf("a registry without a ledger has nothing to check, got %v", err)
	}
}

// TestPackageFilesStayWithinFileSizeLimit keeps every file in the package,
// sources and tests alike, under the 500-line cap from coding-standard.md.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}
