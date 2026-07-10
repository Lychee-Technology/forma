//go:build e2e

package production

import (
	"context"
	"os"
	"testing"
)

// TestMain shuts the shared cluster down after all docker-gated tests run.
func TestMain(m *testing.M) {
	code := m.Run()
	if err := ShutdownSharedCluster(context.Background()); err != nil {
		os.Exit(1)
	}
	os.Exit(code)
}
