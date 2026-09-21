package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// respondNTimes drives respondError n times under one constant op and
// returns the error_id from each body, so the test can join them to the log.
func respondNTimes(t *testing.T, n int, op string, err error) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rec := httptest.NewRecorder()
		respondError(rec, op, err, "schema", "orders")
		var resp APIResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.ErrorID, "response %d carries no error_id", i)
		ids = append(ids, resp.ErrorID)
	}
	return ids
}

// requireEachIDJoinsOneLine: every id a body carried appears on exactly one
// logged line under op, and that line holds the detail the body withheld.
func requireEachIDJoinsOneLine(t *testing.T, ids []string, logs *observer.ObservedLogs, op, withheld string) {
	t.Helper()
	linesByID := map[string]int{}
	for _, entry := range logs.FilterMessage(op).All() {
		fields := entry.ContextMap()
		require.Contains(t, fmt.Sprint(fields["error"]), withheld)
		linesByID[fmt.Sprint(fields["error_id"])]++
	}
	for _, id := range ids {
		require.Equal(t, 1, linesByID[id], "id %s must join exactly one line; the sampler must not have dropped it", id)
	}
}

// TestRespondErrorIDsSurviveProductionSampling is the HTTP half of the same
// invariant the batch path pins in TestBatchResultEveryFailureSurvivesProductionSampling:
// op is a constant message, the production sampler keys on level and message,
// so a burst of identical failures — a database outage is exactly that —
// would otherwise hand callers ids whose lines were never written. Both
// id-minting branches are covered: the redacted Errorw line (#301) and the
// withheld-detail Warnw line (#361). The observer sits under the production
// sampler as every production logger installs it, on a frozen clock so all
// 150 lines land in one sampler tick.
func TestRespondErrorIDsSurviveProductionSampling(t *testing.T) {
	const bursts = 150

	t.Run("redacted 5xx", func(t *testing.T) {
		logs := erroridtest.ObserveUnderProductionSampler(t, zap.DebugLevel)

		ids := respondNTimes(t, bursts, "query failed", operatorDetailError())

		requireEachIDJoinsOneLine(t, ids, logs, "query failed", "manifest lists")
	})

	t.Run("disclosed 4xx with withheld detail", func(t *testing.T) {
		logs := erroridtest.ObserveUnderProductionSampler(t, zap.DebugLevel)
		err := forma.WithOperatorDetail(forma.InvalidInputf("attribute 'age'"), fmt.Errorf("operator cause"))

		ids := respondNTimes(t, bursts, "create failed", err)

		requireEachIDJoinsOneLine(t, ids, logs, "create failed", "operator cause")
	})
}
