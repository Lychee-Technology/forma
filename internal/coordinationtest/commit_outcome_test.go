package coordinationtest

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/testutil/faultproxy"
	"github.com/stretchr/testify/require"
)

// A COMMIT whose reply never arrives (#645): the adapter's InTx answers
// ErrCommitOutcomeUnknown unchanged, and direct SQL shows what happened.

func TestCommitOutcomeUnknown(t *testing.T) {
	cases := []struct {
		name   string
		fault  faultproxy.Fault
		landed bool
	}{
		{"the connection drops after the commit landed", faultproxy.DropCommitReply, true},
		{"the connection drops before the commit reached the server", faultproxy.SwallowCommit, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var proxy *faultproxy.Proxy
			e := newEnvOn(t, func(e *env) *pgxpool.Pool {
				var pool *pgxpool.Pool
				pool, proxy = e.proxiedPool(t)
				return pool
			})
			inst := e.seedInstance(t, 7)
			fired := proxy.Arm(c.fault)
			calls := 0
			err := e.inTx(func(uow UnitOfWork) error {
				calls++
				if err := uow.Instances().Update(e.ctx, advanced(inst, "shipping"), 7); err != nil {
					return err
				}
				return uow.History().Insert(e.ctx, []HistoryEvent{{InstanceID: inst.ID, Type: "transition"}})
			})
			select {
			case <-fired:
			case <-time.After(30 * time.Second):
				t.Fatal("the armed fault never fired: no COMMIT reached the proxy")
			}
			require.Equal(t, 1, calls, "InTx does not re-run fn after an unknown outcome")
			require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
			require.NotErrorIs(t, err, forma.ErrRetryableConflict)
			require.False(t, forma.IsRetryable(err))

			version, histories := int64(7), 0
			if c.landed {
				version, histories = 8, 1
			}
			require.Equal(t, map[string]any{"version": version}, e.stored(t, InstanceSchema, inst.ID, "version"),
				"direct SQL shows whether the commit landed")
			require.Equal(t, histories, e.liveRows(t, HistorySchema), "the Tx's writes landed together or not at all")
		})
	}
}

// proxiedPool answers a pool that reaches the backend through a fault proxy,
// and the proxy.
func (e *env) proxiedPool(t *testing.T) (*pgxpool.Pool, *faultproxy.Proxy) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(activeBackend().dsn)
	require.NoError(t, err)
	if strings.HasPrefix(cfg.ConnConfig.Host, "/") {
		if testdb.InCI() {
			t.Fatalf("the fault proxy needs a TCP database address, got socket %s", cfg.ConnConfig.Host)
		}
		t.Skipf("skipping: the fault proxy needs a TCP database address, got socket %s", cfg.ConnConfig.Host)
	}
	proxy := faultproxy.Start(t, net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))))
	host, port, err := net.SplitHostPort(proxy.Addr())
	require.NoError(t, err)
	portNum, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	cfg.ConnConfig.Host, cfg.ConnConfig.Port = host, uint16(portNum)
	cfg.ConnConfig.TLSConfig, cfg.ConnConfig.Fallbacks = nil, nil
	cfg.ConnConfig.RuntimeParams["application_name"] = e.app
	pool, err := pgxpool.NewWithConfig(e.ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, proxy
}
