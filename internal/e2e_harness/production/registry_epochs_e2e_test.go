//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
)

// TestServerStartupUnpinsASchemaRedroppedAfterItsRegistryLoaded follows the
// server's and the Lambda's startup order: factory.
// LoadFileSchemaRegistryContext reads the publication epochs, then loads the
// caller-facing schema registry, and the factory then loads its own metadata
// and pins the epochs. A drop and re-registration of a schema that commits
// after the registry loaded and before the factory reads anything leaves the
// registry, which resolves the schema, its sort attributes and its decoding
// for every query, describing the dropped schema, while the factory's own
// metadata and the pins agree on the new epoch. The reader pins the schema
// only when its epoch is also the one read before the registry loaded, so the
// re-registered schema's reads fail closed, degraded mode included, and the
// untouched schema is served.
func TestServerStartupUnpinsASchemaRedroppedAfterItsRegistryLoaded(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple, wide := DefaultSchemaFixtures()[0], DefaultSchemaFixtures()[1]
	mustApplyEvents(ctx, t, env, "served schema seed", env.GenerateScript(ScriptSpec{Schema: simple, Creates: 3})...)
	mustApplyEvents(ctx, t, env, "redropped schema seed", env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})...)

	for _, degraded := range []bool{false, true} {
		cfg := factoryConfig(env, degraded)
		cfg.SchemaRegistry, cfg.SchemaRegistryEpochs = nil, nil
		if err := factory.LoadFileSchemaRegistryContext(ctx, cfg, env.Pool, FixtureSchemasDir()); err != nil {
			t.Fatalf("load the registry as the server does: %v", err)
		}
		before := cfg.SchemaRegistryEpochs[wide.ID]
		after := redropSchema(ctx, t, env, wide) // between the registry load and the factory
		if after.Epoch != before+2 {
			t.Fatalf("redrop moved schema %s from epoch %d to %d, want %d", wide.Name, before, after.Epoch, before+2)
		}
		manager := buildFactoryManager(ctx, t, env, cfg, degraded)

		res, err := factoryQuery(ctx, manager, simple)
		if err != nil || len(res.Data) != 3 {
			t.Fatalf("degraded=%t: the untouched schema answered %v, %v; want its 3 rows", degraded, res, err)
		}
		res, err = factoryQuery(ctx, manager, wide)
		if !errors.Is(err, forma.ErrPublicationInconsistent) {
			t.Fatalf("degraded=%t: the schema re-registered after the registry loaded answered %v, %v; want it to fail closed",
				degraded, res, err)
		}
		var typed *forma.PublicationConsistencyError
		if !errors.As(err, &typed) || typed.SchemaID != wide.ID {
			t.Fatalf("degraded=%t: want a *forma.PublicationConsistencyError naming schema %d, got %v", degraded, wide.ID, err)
		}
	}
}
