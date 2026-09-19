package forma

// Validate checks the cross-field and range rules a Config must satisfy
// before a manager is built from it. cmd/server and cmd/lambda call it on the
// fully overlaid config before opening the database, so a misconfiguration
// fails at boot rather than on the first request.
func (c *Config) Validate() error {
	if c.Database.MaxConnections <= 0 {
		return &ConfigError{Field: "database.maxConnections", Message: "must be greater than 0"}
	}

	if c.Query.DefaultPageSize <= 0 {
		return &ConfigError{Field: "query.defaultPageSize", Message: "must be greater than 0"}
	}

	if c.Query.MaxPageSize < c.Query.DefaultPageSize {
		return &ConfigError{Field: "query.maxPageSize", Message: "must be greater than or equal to defaultPageSize"}
	}

	if c.Performance.BatchSize <= 0 {
		return &ConfigError{Field: "performance.batchSize", Message: "must be greater than 0"}
	}

	if c.Performance.MaxBatchSize < c.Performance.BatchSize {
		return &ConfigError{Field: "performance.maxBatchSize", Message: "must be greater than or equal to batchSize"}
	}

	if err := c.validateRequestLimits(); err != nil {
		return err
	}

	if err := c.validateDuckDBConfig(); err != nil {
		return err
	}

	return nil
}

// validateRequestLimits checks the limits the services and the HTTP layer
// enforce per request (#465). The entity size cap doubles as the HTTP body
// cap, so it must be positive; the two budgets may be zero (unbounded) but
// never negative, matching the duckdb.queryTimeout rule. The batch cap needs
// no rule of its own: performance.maxBatchSize >= batchSize > 0 above already
// keeps it positive.
func (c *Config) validateRequestLimits() error {
	if c.Entity.MaxEntitySize <= 0 {
		return &ConfigError{Field: "entity.maxEntitySize", Message: "must be greater than 0"}
	}
	if c.Query.DefaultTimeout < 0 {
		return &ConfigError{Field: "query.defaultTimeout", Message: "must be greater than or equal to 0"}
	}
	if c.Transaction.DefaultTimeout < 0 {
		return &ConfigError{Field: "transaction.defaultTimeout", Message: "must be greater than or equal to 0"}
	}
	return nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ConfigError) Error() string {
	return "config validation error for field '" + e.Field + "': " + e.Message
}
