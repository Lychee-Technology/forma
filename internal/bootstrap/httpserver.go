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
type HTTPServerConfig struct {
	// ReadHeaderTimeout bounds reading the request line and headers; it is
	// the Slowloris defence.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading the whole request, body included.
	ReadTimeout time.Duration
	// WriteTimeout bounds the response, measured from the end of the header
	// read; it must exceed the largest manager budget or a slow-but-legal
	// query is cut off mid-response.
	WriteTimeout time.Duration
	// IdleTimeout bounds a keep-alive connection waiting for its next request.
	IdleTimeout time.Duration
	// MaxHeaderBytes caps the request header block.
	MaxHeaderBytes int
}

// DefaultHTTPServerConfig returns the bounds a fresh server starts from.
// WriteTimeout is twice the 30s default query and transaction budgets so a
// request that spends its full budget still gets its response out.
func DefaultHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
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
// When budgets is non-nil, a bounded WriteTimeout must also cover the query
// and transaction budgets, or a request that legitimately spends its budget
// has its connection cut instead of receiving the 504. An unbounded budget
// (zero) is left to the write timeout, so it needs no check.
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
	if budgets.Query.DefaultTimeout > c.WriteTimeout {
		return &forma.ConfigError{Field: "http.writeTimeout",
			Message: fmt.Sprintf("%s is shorter than the query budget %s", c.WriteTimeout, budgets.Query.DefaultTimeout)}
	}
	if budgets.Transaction.DefaultTimeout > c.WriteTimeout {
		return &forma.ConfigError{Field: "http.writeTimeout",
			Message: fmt.Sprintf("%s is shorter than the transaction budget %s", c.WriteTimeout, budgets.Transaction.DefaultTimeout)}
	}
	return nil
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
