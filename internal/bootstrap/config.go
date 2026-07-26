package bootstrap

import (
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

func EnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
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

func DatabaseConfigFromEnv(defaults DBDefaults) forma.DatabaseConfig {
	return forma.DatabaseConfig{
		Host:            Env("DB_HOST", defaults.Host),
		Port:            EnvInt("DB_PORT", defaults.Port),
		Database:        Env("DB_NAME", defaults.Database),
		Username:        Env("DB_USER", defaults.Username),
		Password:        Env("DB_PASSWORD", defaults.Password),
		SSLMode:         Env("DB_SSL_MODE", defaults.SSLMode),
		Schema:          Env("DB_SCHEMA", defaults.Schema),
		MaxConnections:  EnvInt("DB_MAX_CONNECTIONS", defaults.MaxConnections),
		MaxIdleConns:    EnvInt("DB_MAX_IDLE_CONNS", defaults.MaxIdleConns),
		ConnMaxLifetime: time.Duration(EnvInt("DB_CONN_MAX_LIFETIME_SECONDS", defaults.ConnMaxLifetimeSeconds)) * time.Second,
		ConnMaxIdleTime: time.Duration(EnvInt("DB_CONN_MAX_IDLE_TIME_SECONDS", defaults.ConnMaxIdleTimeSeconds)) * time.Second,
		Timeout:         time.Duration(EnvInt("DB_TIMEOUT_SECONDS", defaults.TimeoutSeconds)) * time.Second,
	}
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
