package forma

import "testing"

// The engine decides how writes exclude each other (#633), so a name that is
// neither engine fails Validate instead of falling back to PostgreSQL's
// locks. The empty name is PostgreSQL, so an existing config is unchanged.
func TestConfigValidationDatabaseEngine(t *testing.T) {
	tests := []validationCase{
		{name: "empty is postgresql", config: configWith(func(c *Config) { c.Database.Engine = "" })},
		{name: "postgresql", config: configWith(func(c *Config) { c.Database.Engine = EnginePostgreSQL })},
		{name: "aurora-dsql", config: configWith(func(c *Config) { c.Database.Engine = EngineAuroraDSQL })},
		{
			name:        "unknown engine",
			config:      configWith(func(c *Config) { c.Database.Engine = "dsql" }),
			expectError: true,
			errorField:  "database.engine",
		},
		{
			name:        "wrong case",
			config:      configWith(func(c *Config) { c.Database.Engine = "PostgreSQL" }),
			expectError: true,
			errorField:  "database.engine",
		},
	}

	runValidationCases(t, tests)
}

func TestDatabaseEngineResolved(t *testing.T) {
	if got := DatabaseEngine("").Resolved(); got != EnginePostgreSQL {
		t.Fatalf("the empty engine must resolve to %q, got %q", EnginePostgreSQL, got)
	}
	if got := EngineAuroraDSQL.Resolved(); got != EngineAuroraDSQL {
		t.Fatalf("a named engine must resolve to itself, got %q", got)
	}
}
