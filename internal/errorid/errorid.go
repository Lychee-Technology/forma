// Package errorid mints the correlation id a caller quotes back to an
// operator when a response withholds an error's text, and owns the logger
// that writes the line the id leads to.
//
// Two write surfaces publish such an id: internal/httpapi puts one on every
// redacted body and on a disclosed 4xx that withholds operator detail
// (respondErrorWithStatus, #301/#361), and the best-effort batch path puts one
// on every failed forma.OperationError (#398). Each writes the same id on the
// log line that keeps the full error, so the id is the join between what the
// caller saw and what the operator can read. It lives here rather than at
// either call site for the same reason internal/redact does: the shape is a
// contract shared by both surfaces, and a second copy would drift.
//
// The join only holds if the line is actually written. The production logger
// samples (zap.NewProductionConfig: the first 100 entries per second with the
// same level and message, then every 100th), and every correlation line has a
// constant message, so under a failure storm — the moment an operator needs
// the join most — the sampler would drop lines whose ids callers already hold.
// Logger routes such lines around the sampler: it names them LoggerName, and
// the core SamplerOption installs sends entries whose last name segment is
// LoggerName (IsCorrelationLogger) to the core beneath the sampler. Nothing
// else changes for them: level, fields and encoding are the global logger's.
// internal/bootstrap applies SamplerOption to every cmd/ binary's logger, and
// factory.NewProductionLogger, BuildLogger and SamplerOption hand the same
// installation to an embedder, since the global logger is the embedder's and
// the join holds only if their sampler carries the exemption too.
package errorid

import (
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// LoggerName is the name Logger gives its entries, and the only thing the
// sampler exemption keys on (IsCorrelationLogger). It reaches the output as
// the encoder's name field ("logger" in the production config) — on its own
// under an unnamed global logger, or as the last segment ("svc.errorid")
// under a global the embedder named — so an operator can filter on it.
const LoggerName = "errorid"

// New returns a fresh correlation id: a canonical UUID string, so it parses
// with uuid.Parse and greps verbatim out of a log line.
func New() string {
	return uuid.NewString()
}

// Logger returns the global sugared logger for a line that carries an id
// minted by New. It is the global logger under LoggerName and nothing more,
// so a test that installs an observer through zap.ReplaceGlobals sees these
// lines like any other; the name only matters once SamplerOption's core is
// in the chain. Resolved on every call rather than cached so it follows
// zap.ReplaceGlobals, exactly as zap.S() does.
func Logger() *zap.SugaredLogger {
	return zap.S().Named(LoggerName)
}
