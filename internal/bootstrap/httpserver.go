package bootstrap

import (
	"fmt"
	"net/http"
	"time"

	"github.com/lychee-technology/forma"
)

// HTTPServerConfig holds the http.Server bounds cmd/server applies (#465).
// Every field is a hard ceiling on one phase of a connection's life; none of
// them cancels a handler's context, which is why the manager applies its own
// per-request budgets (QueryConfig.DefaultTimeout and friends) on top.
//
// The phases overlap, and Validate encodes how. net/http arms the write
// deadline when the request headers have been read, before the handler
// consumes the body, so WriteTimeout is spent on three things in sequence:
// reading the body (bounded by ReadTimeout, which is measured from the
// start of the request), the handler's work (bounded by the largest manager
// budget), and writing the response. A WriteTimeout that does not exceed the
// first two leaves nothing for the third, and a slow-but-legal upload
// followed by a request that spends its budget loses its 504 to a closed
// connection.
type HTTPServerConfig struct {
	// ReadHeaderTimeout bounds reading the request line and headers; it is
	// the Slowloris defence.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading the whole request, headers and body,
	// measured from the first byte of the request.
	ReadTimeout time.Duration
	// WriteTimeout bounds the response, measured from the end of the header
	// read; see the type comment for what it has to cover.
	WriteTimeout time.Duration
	// IdleTimeout bounds a keep-alive connection waiting for its next request.
	IdleTimeout time.Duration
	// MaxHeaderBytes caps the request header block.
	MaxHeaderBytes int
}

// DefaultHTTPServerConfig returns the bounds a fresh server starts from.
// WriteTimeout is the 30s ReadTimeout plus the 30s default query and
// transaction budgets plus a 30s margin, so a request that spends the whole
// of both still gets its response out.
func DefaultHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// HTTPServerConfigFromEnv overlays the HTTP_* environment on defaults. The
// timeouts are whole seconds; an unparsable value keeps the default, matching
// every other EnvInt overlay in this package.
func HTTPServerConfigFromEnv(defaults HTTPServerConfig) HTTPServerConfig {
	return HTTPServerConfig{
		ReadHeaderTimeout: envSeconds("HTTP_READ_HEADER_TIMEOUT_SECONDS", defaults.ReadHeaderTimeout),
		ReadTimeout:       envSeconds("HTTP_READ_TIMEOUT_SECONDS", defaults.ReadTimeout),
		WriteTimeout:      envSeconds("HTTP_WRITE_TIMEOUT_SECONDS", defaults.WriteTimeout),
		IdleTimeout:       envSeconds("HTTP_IDLE_TIMEOUT_SECONDS", defaults.IdleTimeout),
		MaxHeaderBytes:    EnvInt("HTTP_MAX_HEADER_BYTES", defaults.MaxHeaderBytes),
	}
}

// Validate refuses a configuration net/http would accept but that quietly
// removes a protection (#465 review): a negative duration disables that
// phase's timeout, and a negative MaxHeaderBytes silently falls back to
// net/http's default. Zero stays legal and means "unbounded" or "default",
// as NewHTTPServer documents.
//
// When budgets is non-nil and names a bounded request budget, a bounded
// WriteTimeout must also leave room for the response after the body read
// and the budget (see the type comment): WriteTimeout > ReadTimeout +
// LargestRequestBudget, strictly, so that the difference is the margin the
// response is written in. A bounded WriteTimeout then also needs a bounded
// ReadTimeout, since an unbounded body phase can consume any write deadline
// before the handler starts. An unbounded budget or an unbounded
// WriteTimeout is the operator opting out of the guarantee and needs no
// check.
func (c HTTPServerConfig) Validate(budgets *forma.Config) error {
	phases := []struct {
		name string
		d    time.Duration
	}{
		{"readHeaderTimeout", c.ReadHeaderTimeout},
		{"readTimeout", c.ReadTimeout},
		{"writeTimeout", c.WriteTimeout},
		{"idleTimeout", c.IdleTimeout},
	}
	for _, phase := range phases {
		if phase.d < 0 {
			return &forma.ConfigError{Field: "http." + phase.name, Message: "must be greater than or equal to 0"}
		}
	}
	if c.MaxHeaderBytes < 0 {
		return &forma.ConfigError{Field: "http.maxHeaderBytes", Message: "must be greater than or equal to 0"}
	}
	if budgets == nil || c.WriteTimeout == 0 {
		return nil
	}
	budget, name := LargestRequestBudget(budgets)
	if budget == 0 {
		return nil
	}
	if c.ReadTimeout == 0 {
		return &forma.ConfigError{Field: "http.readTimeout",
			Message: fmt.Sprintf("must be bounded when writeTimeout (%s) and the %s budget (%s) are: the write deadline starts before the body is read",
				c.WriteTimeout, name, budget)}
	}
	if c.WriteTimeout <= c.ReadTimeout+budget {
		return &forma.ConfigError{Field: "http.writeTimeout",
			Message: fmt.Sprintf("%s must exceed readTimeout %s plus the %s budget %s to leave room for writing the response",
				c.WriteTimeout, c.ReadTimeout, name, budget)}
	}
	return nil
}

// LargestRequestBudget returns the longest a handler may legitimately spend
// inside the manager under cfg, and which budget sets it, or zero when no
// budget bounds it. The read side is QueryConfig.DefaultTimeout when that is
// bounded: DuckDBConfig.QueryTimeout runs inside it (a nested
// context.WithTimeout keeps the earlier deadline) and can only lengthen a
// request when the query budget is unbounded, which is when it is the read
// side instead (#465 review). The write side is
// TransactionConfig.DefaultTimeout.
func LargestRequestBudget(cfg *forma.Config) (time.Duration, string) {
	budget, name := cfg.Query.DefaultTimeout, "query"
	if budget == 0 {
		budget, name = cfg.DuckDB.QueryTimeout, "duckdb query"
	}
	if cfg.Transaction.DefaultTimeout > budget {
		budget, name = cfg.Transaction.DefaultTimeout, "transaction"
	}
	return budget, name
}

// NewHTTPServer builds the http.Server cmd/server listens on, with every
// bound from cfg applied. Callers that want a phase unbounded pass zero for
// it, which net/http reads as "no timeout".
func NewHTTPServer(addr string, handler http.Handler, cfg HTTPServerConfig) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
}

// envSeconds reads a whole-seconds duration; unset or unparsable keeps the
// default.
func envSeconds(key string, defaultValue time.Duration) time.Duration {
	seconds := EnvInt(key, int(defaultValue/time.Second))
	return time.Duration(seconds) * time.Second
}
