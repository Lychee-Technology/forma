//go:build e2e

package federated_test

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkScaffold_SchemaRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	runner, err := bench.NewRunner(bench.DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, runner.RegisterSchemas(h))

	for _, fixture := range bench.DefaultSchemaFixtures() {
		var schemaName string
		err := h.PGDB.QueryRowContext(ctx,
			`SELECT schema_name FROM schema_registry WHERE schema_id = $1`,
			fixture.ID,
		).Scan(&schemaName)
		require.NoError(t, err)
		require.Equal(t, fixture.Name, schemaName)
	}
}
