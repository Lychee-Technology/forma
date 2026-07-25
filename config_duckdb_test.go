package forma

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDuckDBBreakerConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultDuckDBConfig()

	if cfg.CircuitBreakerThreshold != 0 {
		t.Errorf("Expected deprecated circuit breaker threshold to default to 0, got %v", cfg.CircuitBreakerThreshold)
	}
	if cfg.CircuitBreakerFailureThreshold != 5 {
		t.Errorf("Expected circuit breaker failure threshold to be 5, got %d", cfg.CircuitBreakerFailureThreshold)
	}
	if cfg.CircuitBreakerWindow != time.Minute {
		t.Errorf("Expected circuit breaker window to be 1m, got %v", cfg.CircuitBreakerWindow)
	}
	if cfg.CircuitBreakerOpenDuration != time.Minute {
		t.Errorf("Expected circuit breaker open duration to be 1m, got %v", cfg.CircuitBreakerOpenDuration)
	}
}

func TestDuckDBResourceConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultDuckDBConfig()

	if cfg.MaxParallelism != 4 {
		t.Errorf("Expected max parallelism to default to 4 (parity with the removed template PRAGMA threads=4), got %d", cfg.MaxParallelism)
	}
	if cfg.MemoryLimitMB != 4096 {
		t.Errorf("Expected memory limit to default to 4096MB (parity with the removed template PRAGMA memory_limit='4GB'), got %d", cfg.MemoryLimitMB)
	}
}

func TestDefaultDuckDBConfig_ManifestReadOff(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig(nil)

	if cfg.DuckDB.ManifestReadEnabled() {
		t.Error("Expected manifest read to be disabled by default; a non-empty default template would silently switch existing deployments to manifest-driven reads")
	}
	if cfg.DuckDB.ManifestTemplate != "" {
		t.Errorf("Expected default manifest template to be empty, got %q", cfg.DuckDB.ManifestTemplate)
	}
	if cfg.DuckDB.ManifestPrefix != "" {
		t.Errorf("Expected default manifest prefix to be empty, got %q", cfg.DuckDB.ManifestPrefix)
	}
	if cfg.DuckDB.S3Bucket != "" {
		t.Errorf("Expected default s3 bucket to be empty, got %q", cfg.DuckDB.S3Bucket)
	}
	if cfg.DuckDB.S3DataPrefix != "" {
		t.Errorf("Expected default s3 data prefix to be empty, got %q", cfg.DuckDB.S3DataPrefix)
	}
}

func TestValidateDuckDBConfig_ManifestRead(t *testing.T) {
	t.Parallel()

	tests := []manifestReadCase{
		{
			name: "template with bucket and data prefix is valid",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				S3DataPrefix:     "forma",
				ManifestPrefix:   "forma",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "",
		},
		{
			name: "template without bucket",
			duckDB: DuckDBConfig{
				S3Bucket:         "   ",
				ManifestPrefix:   "forma",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "duckdb.s3Bucket",
		},
		{
			name: "template that fails to parse",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/{{.SchemaID}.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "inert manifest prefix without template",
			duckDB: DuckDBConfig{
				S3Bucket:       "forma-lake",
				ManifestPrefix: "forma",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "inert s3 data prefix without template",
			duckDB: DuckDBConfig{
				S3Bucket:     "forma-lake",
				S3DataPrefix: "forma",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "template without data prefix is valid (fallback glob disabled)",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestPrefix:   "forma",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "",
		},
		{
			name: "complete manifest config is valid while duckdb is disabled",
			duckDB: DuckDBConfig{
				Enabled:          false,
				S3Bucket:         "forma-lake",
				S3DataPrefix:     "forma",
				ManifestPrefix:   "forma",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "",
		},
	}

	runManifestReadCases(t, tests)
}

// manifestReadCase is one ValidateManifestRead expectation: an empty
// errorField means the combination must validate.
type manifestReadCase struct {
	name       string
	duckDB     DuckDBConfig
	errorField string
}

// runManifestReadCases asserts each case and checks the ConfigError field, so a
// rejection is attributed to the offending setting rather than merely counted.
func runManifestReadCases(t *testing.T, cases []manifestReadCase) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.duckDB.ValidateManifestRead()
			if tt.errorField == "" {
				if err != nil {
					t.Fatalf("Expected no validation error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatal("Expected validation error, got nil")
			}
			configErr, ok := err.(*ConfigError)
			if !ok {
				t.Fatalf("Expected ConfigError, got %T", err)
			}
			if configErr.Field != tt.errorField {
				t.Errorf("Expected error field %s, got %s", tt.errorField, configErr.Field)
			}
		})
	}
}

// TestValidateDuckDBConfig_ManifestReadWhitespace covers the byte-parity rule:
// the four manifest read fields are shared with the CDC/compaction write side,
// which never trims them, so a padded value would resolve a different object key
// on each side and surface only as a silently empty cold tier. Split from
// TestValidateDuckDBConfig_ManifestRead for the 100-line function cap
// (coding-standard.md).
func TestValidateDuckDBConfig_ManifestReadWhitespace(t *testing.T) {
	t.Parallel()

	runManifestReadCases(t, []manifestReadCase{
		{
			// Padded values are rejected rather than trimmed at the reader: the
			// CDC/compaction write side never trims, so a shared value carrying
			// a stray newline (a YAML quoting slip, a mounted secret file) would
			// resolve one key on the write side and a different one on the read
			// side. Byte parity is the contract; the config surface enforces it.
			name: "padded manifest template",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: " manifest/{{.SchemaID}}.json\n",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// A whitespace-only template escapes both gates today:
			// ManifestReadEnabled() trims it to "" (reads stay off) while the
			// inert-prefix check sees a non-empty string. The whitespace rule
			// closes that hole.
			name: "whitespace-only manifest template",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "   ",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "padded s3 bucket",
			duckDB: DuckDBConfig{
				S3Bucket:         " forma-lake",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "duckdb.s3Bucket",
		},
		{
			name: "padded manifest prefix",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestPrefix:   "forma ",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "duckdb.manifestPrefix",
		},
		{
			name: "padded s3 data prefix",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				S3DataPrefix:     "\tforma",
				ManifestTemplate: "manifest/{{.SchemaID}}.json",
			},
			errorField: "duckdb.s3DataPrefix",
		},
	})
}

// TestValidateDuckDBConfig_ManifestTemplateProbe covers the #300 Execute-probe:
// parsing a template is not enough, because the resolver executes it against a
// map, so a template can parse cleanly and still address the wrong object — or
// the same object — for every schema. Split from
// TestValidateDuckDBConfig_ManifestRead, which was over the 100-line function
// cap (coding-standard.md).
func TestValidateDuckDBConfig_ManifestTemplateProbe(t *testing.T) {
	t.Parallel()

	runManifestReadCases(t, []manifestReadCase{
		{
			// #300: a case-typo field name parses fine but renders "<no value>"
			// against the resolver's map, so every schema resolves to the same
			// bogus manifest key, every manifest loads empty, and reads degrade
			// silently — the exact loss shape manifest reads exist to prevent,
			// one typo away.
			name: "template with a misspelled field renders no value",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/{{.SchemaId}}.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Same collapse, different cause: a template that ignores the schema
			// entirely points every schema at one object.
			name: "template that does not vary by schema",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/all.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "template rendering to nothing",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "{{if false}}manifest/{{.SchemaID}}.json{{end}}",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Printf-style verbs are a plausible slip for someone who has not
			// used text/template: they render literally and never vary.
			name: "printf style placeholder instead of template action",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/%d.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Review P1: only the first probe output was screened, so a template
			// that renders for schema 1 and nothing for schema 2 passed — the two
			// outputs differ, and the empty one was never checked. Those schemas
			// would resolve an empty manifest path at runtime.
			name: "template rendering empty for only some schemas",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "{{if eq .SchemaID 1}}manifest/1.json{{end}}",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Same gap, reached differently: index bypasses missingkey=error, so
			// only an output check catches the "<no value>" it renders.
			name: "template rendering no value for only some schemas",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: `{{if eq .SchemaID 1}}{{.SchemaID}}{{else}}{{index . "SchemaId"}}{{end}}`,
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "schema id embedded in a longer path is valid",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "lake/v2/schema-{{.SchemaID}}/manifest.json",
			},
			errorField: "",
		},
	})
}

func TestValidateDuckDBConfig_ManifestReadWiredIntoValidate(t *testing.T) {
	t.Parallel()

	config := DefaultConfig(NewMockSchemaRegistry())
	config.DuckDB.ManifestPrefix = "forma"

	err := config.Validate()
	if err == nil {
		t.Fatal("Expected Validate to reject inert manifest config, got nil")
	}
	// Validate() wraps the delegated manifest error with context, so callers
	// must unwrap: a bare type assertion would break on the wrapper.
	var configErr *ConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("Expected ConfigError, got %T (%v)", err, err)
	}
	if configErr.Field != "duckdb.manifestTemplate" {
		t.Errorf("Expected error field duckdb.manifestTemplate, got %s", configErr.Field)
	}
	if !strings.Contains(err.Error(), "invalid duckdb manifest read configuration") {
		t.Errorf("Expected the delegated error to be wrapped with context, got %q", err.Error())
	}
}

func TestDuckDBConfig_ManifestReadEnabled(t *testing.T) {
	t.Parallel()

	if (DuckDBConfig{}).ManifestReadEnabled() {
		t.Error("Expected zero-value DuckDBConfig to have manifest read disabled")
	}
	if (DuckDBConfig{ManifestTemplate: "  "}).ManifestReadEnabled() {
		t.Error("Expected whitespace-only manifest template to be treated as disabled")
	}
	if !(DuckDBConfig{ManifestTemplate: "manifest/{{.SchemaID}}.json"}).ManifestReadEnabled() {
		t.Error("Expected non-empty manifest template to enable manifest read")
	}
}

func TestDuckDBBreakerConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		duckDB     DuckDBConfig
		errorField string
	}{
		{
			name:       "zero breaker values are allowed for runtime default fallback",
			duckDB:     DuckDBConfig{},
			errorField: "",
		},
		{
			name: "negative failure threshold",
			duckDB: DuckDBConfig{
				CircuitBreakerFailureThreshold: -1,
			},
			errorField: "duckdb.circuitBreakerFailureThreshold",
		},
		{
			name: "negative window",
			duckDB: DuckDBConfig{
				CircuitBreakerWindow: -time.Second,
			},
			errorField: "duckdb.circuitBreakerWindow",
		},
		{
			name: "negative open duration",
			duckDB: DuckDBConfig{
				CircuitBreakerOpenDuration: -time.Second,
			},
			errorField: "duckdb.circuitBreakerOpenDuration",
		},
		{
			name: "negative max parallelism",
			duckDB: DuckDBConfig{
				MaxParallelism: -1,
			},
			errorField: "duckdb.maxParallelism",
		},
		{
			name: "negative memory limit",
			duckDB: DuckDBConfig{
				MemoryLimitMB: -1,
			},
			errorField: "duckdb.memoryLimitMB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := DefaultConfig(NewMockSchemaRegistry())
			config.DuckDB = tt.duckDB

			err := config.Validate()
			if tt.errorField == "" {
				if err != nil {
					t.Fatalf("Expected no validation error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatal("Expected validation error, got nil")
			}
			configErr, ok := err.(*ConfigError)
			if !ok {
				t.Fatalf("Expected ConfigError, got %T", err)
			}
			if configErr.Field != tt.errorField {
				t.Errorf("Expected error field %s, got %s", tt.errorField, configErr.Field)
			}
		})
	}
}
