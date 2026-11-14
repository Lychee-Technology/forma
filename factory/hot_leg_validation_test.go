package factory

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEntityManagerWithConfig_HotLegValidationBeforeIO(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		transport forma.HotLegTransport
		cap       int
		wantField string
	}{
		{name: "empty transport"},
		{name: "pgx with a cap", transport: forma.HotLegTransportPgx, cap: 100},
		{name: "retired attach", transport: "attach", wantField: "duckdb.hotLegTransport"},
		{name: "unknown transport", transport: "PGX", wantField: "duckdb.hotLegTransport"},
		{name: "negative cap", cap: -1, wantField: "duckdb.hotLegMaxDirtyRows"},
	}
	for _, tc := range cases {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", tc.name, enabled), func(t *testing.T) {
				t.Parallel()

				cfg := forma.DefaultConfig(nil)
				cfg.DuckDB.Enabled = enabled
				cfg.DuckDB.HotLegTransport = tc.transport
				cfg.DuckDB.HotLegMaxDirtyRows = tc.cap
				deps := defaultEntityManagerDependencies()
				ioCalls := 0
				deps.collectTables = func(context.Context, queryPool, string) ([]string, error) {
					ioCalls++
					return nil, assert.AnError
				}

				em, err := newEntityManagerWithConfigContext(context.Background(), cfg, nil, deps)
				assert.Nil(t, em)
				if tc.wantField == "" {
					require.ErrorIs(t, err, assert.AnError, "valid hot-leg settings must reach table collection")
					assert.Equal(t, 1, ioCalls)
					return
				}
				assert.Zero(t, ioCalls, "invalid hot-leg settings must fail before the first I/O")
				var configErr *forma.ConfigError
				require.ErrorAs(t, err, &configErr)
				assert.Equal(t, tc.wantField, configErr.Field)
				if tc.transport == "attach" {
					assert.Contains(t, configErr.Message, "retired")
				}
			})
		}
	}
}

func TestConfigFactoryConstructorsRefuseRetiredAttach(t *testing.T) {
	t.Parallel()

	constructors := map[string]func(*forma.Config, *pgxpool.Pool) (forma.EntityManager, error){
		"without context": NewEntityManagerWithConfig,
		"with context": func(cfg *forma.Config, pool *pgxpool.Pool) (forma.EntityManager, error) {
			return NewEntityManagerWithConfigContext(context.Background(), cfg, pool)
		},
	}
	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := forma.DefaultConfig(nil)
			cfg.DuckDB.Enabled = true
			cfg.DuckDB.HotLegTransport = "attach"
			// A nil pool cannot do I/O: both public entry points must reject
			// the config without requiring callers to run Config.Validate.
			em, err := construct(cfg, nil)
			assert.Nil(t, em)
			var configErr *forma.ConfigError
			require.ErrorAs(t, err, &configErr)
			assert.Equal(t, "duckdb.hotLegTransport", configErr.Field)
			assert.Contains(t, configErr.Message, "retired")
		})
	}
}
