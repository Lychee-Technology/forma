package bootstrap

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
)

type DBDefaults struct {
	Host                   string
	Port                   int
	Database               string
	Username               string
	Password               string
	SSLMode                string
	Schema                 string
	MaxConnections         int
	MaxIdleConns           int
	ConnMaxLifetimeSeconds int
	ConnMaxIdleTimeSeconds int
	TimeoutSeconds         int
}

func Env(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// EnvAllowEmpty is Env for a setting whose empty value is meaningful (for
// example a table name where empty disables a feature): only an unset var
// takes the default, so an explicit KEY= is returned as "".
func EnvAllowEmpty(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

// EnvError reports an environment variable that is set to a value its setting
// cannot use (#600). An unset or empty variable never produces one; it takes
// the default. Before #600 a value like QUERY_TIMEOUT_SECONDS=30s silently
// kept the default, so the operator believed a limit was set when it was not.
type EnvError struct {
	// Key is the variable's name and Value its raw, unparsed value.
	Key   string
	Value string
	// Want is the form the setting needs, such as "an integer".
	Want string
	// Err is why Value is not that form; strconv.ErrSyntax or
	// strconv.ErrRange, possibly wrapped.
	Err error
}

func (e *EnvError) Error() string {
	return fmt.Sprintf("environment variable %s=%q must be %s: %v", e.Key, e.Value, e.Want, e.Err)
}

func (e *EnvError) Unwrap() error { return e.Err }

// EnvInt reads a base-10 integer variable. Unset or empty takes defaultValue;
// a set value that does not parse is an *EnvError naming the variable and the
// value, and the default is returned alongside it only so a caller that
// collects errors still has a value to fill a field with.
func EnvInt(key string, defaultValue int) (int, error) {
	value, ok, err := lookupEnvInt(key, "an integer")
	if !ok {
		return defaultValue, err
	}
	return value, nil
}

// maxEnvSeconds is the largest whole number of seconds a time.Duration holds;
// past it the multiplication by time.Second wraps around.
const maxEnvSeconds = math.MaxInt64 / int64(time.Second)

// envSeconds reads a whole-seconds duration with EnvInt's contract. A value
// that parses but does not fit a time.Duration is refused as well: it would
// otherwise wrap to an unrelated duration (18446744074 becomes about 0.29s).
func envSeconds(key string, defaultValue time.Duration) (time.Duration, error) {
	const want = "a whole number of seconds"
	seconds, ok, err := lookupEnvInt(key, want)
	if !ok {
		return defaultValue, err
	}
	if s := int64(seconds); s > maxEnvSeconds || s < -maxEnvSeconds {
		return defaultValue, &EnvError{Key: key, Value: os.Getenv(key), Want: want,
			Err: fmt.Errorf("%w: at most %d seconds", strconv.ErrRange, maxEnvSeconds)}
	}
	return time.Duration(seconds) * time.Second, nil
}

// lookupEnvInt parses key as a base-10 int. ok is false when the variable is
// unset or empty (err is nil) or when it does not parse (err is an *EnvError
// that describes the expected form as want).
func lookupEnvInt(key, want string) (value int, ok bool, err error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, false, nil
	}
	value, err = strconv.Atoi(raw)
	if err != nil {
		// The NumError repeats the value the EnvError already quotes; keep
		// only its cause.
		var numErr *strconv.NumError
		if errors.As(err, &numErr) {
			err = numErr.Err
		}
		return 0, false, &EnvError{Key: key, Value: raw, Want: want, Err: err}
	}
	return value, true, nil
}

// envOverlay reads the integer variables of one *FromEnv function and keeps
// every parse failure, so the function can fill all of its fields and then
// name each bad variable at once rather than only the first.
type envOverlay struct {
	errs []error
}

func (o *envOverlay) integer(key string, defaultValue int) int {
	value, err := EnvInt(key, defaultValue)
	o.keep(err)
	return value
}

func (o *envOverlay) seconds(key string, defaultValue time.Duration) time.Duration {
	value, err := envSeconds(key, defaultValue)
	o.keep(err)
	return value
}

func (o *envOverlay) keep(err error) {
	if err != nil {
		o.errs = append(o.errs, err)
	}
}

// err joins every failure kept so far; nil when there is none.
func (o *envOverlay) err() error {
	return errors.Join(o.errs...)
}

// EnvBool reads a boolean env var, treating "true"/"1" (case-insensitive) as
// true. Any other non-empty value is false; an unset var returns the default.
func EnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return strings.EqualFold(value, "true") || value == "1"
}

// DatabaseConfigFromEnv overlays the DB_* environment on defaults. A set but
// unparsable integer variable fails it with an *EnvError per bad variable
// (#600); see EnvInt.
func DatabaseConfigFromEnv(defaults DBDefaults) (forma.DatabaseConfig, error) {
	var env envOverlay
	cfg := forma.DatabaseConfig{
		Host:            Env("DB_HOST", defaults.Host),
		Port:            env.integer("DB_PORT", defaults.Port),
		Database:        Env("DB_NAME", defaults.Database),
		Username:        Env("DB_USER", defaults.Username),
		Password:        Env("DB_PASSWORD", defaults.Password),
		SSLMode:         Env("DB_SSL_MODE", defaults.SSLMode),
		Schema:          Env("DB_SCHEMA", defaults.Schema),
		MaxConnections:  env.integer("DB_MAX_CONNECTIONS", defaults.MaxConnections),
		MaxIdleConns:    env.integer("DB_MAX_IDLE_CONNS", defaults.MaxIdleConns),
		ConnMaxLifetime: env.seconds("DB_CONN_MAX_LIFETIME_SECONDS", time.Duration(defaults.ConnMaxLifetimeSeconds)*time.Second),
		ConnMaxIdleTime: env.seconds("DB_CONN_MAX_IDLE_TIME_SECONDS", time.Duration(defaults.ConnMaxIdleTimeSeconds)*time.Second),
		Timeout:         env.seconds("DB_TIMEOUT_SECONDS", time.Duration(defaults.TimeoutSeconds)*time.Second),
	}
	if err := env.err(); err != nil {
		return forma.DatabaseConfig{}, err
	}
	return cfg, nil
}

// EntityConfigFromEnv overlays operator-settable entity options onto defaults.
//
// This exists because #314's rollout is staged: creates are always enforced, but
// updates start report-only so that rows written before enforcement stay
// updatable. Flipping to enforcing is an operational decision, so it has to be
// reachable from the environment and not only by library embedders.
func EntityConfigFromEnv(defaults forma.EntityConfig) forma.EntityConfig {
	cfg := defaults
	cfg.ValidateUpdatesStrict = EnvBool("VALIDATE_UPDATES_STRICT", defaults.ValidateUpdatesStrict)
	return cfg
}

func TableNamesFromEnv(defaults forma.TableNames) forma.TableNames {
	return forma.TableNames{
		SchemaRegistry: Env("SCHEMA_TABLE", defaults.SchemaRegistry),
		EAVData:        Env("EAV_TABLE", defaults.EAVData),
		EntityMain:     Env("ENTITY_MAIN_TABLE", defaults.EntityMain),
		ChangeLog:      Env("CHANGE_LOG_TABLE", defaults.ChangeLog),
	}
}
