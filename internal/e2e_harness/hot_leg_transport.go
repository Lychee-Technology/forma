package e2e_harness

import (
	"fmt"
	"os"

	"github.com/lychee-technology/forma"
)

// HotLegTransportEnv selects the federated hot-leg transport the e2e
// harnesses build their engines with (#856): "attach", "pgx", or unset for
// the engine's default. The federated suite and the production harness run
// under both values (pgx-hot-leg-plan.md step 5):
//
//	FORMA_E2E_HOT_LEG_TRANSPORT=pgx go test -tags=e2e ./internal/e2e_harness/...
const HotLegTransportEnv = "FORMA_E2E_HOT_LEG_TRANSPORT"

// HotLegTransportFromEnv reads HotLegTransportEnv. An unset or empty value
// answers the empty transport, which the engine reads as attach; any other
// value must be one ValidateHotLeg accepts.
func HotLegTransportFromEnv() (forma.HotLegTransport, error) {
	t := forma.HotLegTransport(os.Getenv(HotLegTransportEnv))
	if err := (forma.DuckDBConfig{HotLegTransport: t}).ValidateHotLeg(); err != nil {
		return "", fmt.Errorf("%s: %w", HotLegTransportEnv, err)
	}
	return t, nil
}
