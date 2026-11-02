package main

import (
	"errors"
	"flag"
	"io"
	"testing"

	"github.com/lychee-technology/forma/internal/bootstrap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestSampleConfigFromEnvRejectsUnrecognisedBoolean pins #642 on the sample
// importer: VALIDATE_UPDATES_STRICT=yes used to read as false and import with
// updates report-only. sampleConfigFromEnv does no I/O and runSampleImport
// calls it before connecting, so the run now stops before touching the
// database.
func TestSampleConfigFromEnvRejectsUnrecognisedBoolean(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "yes")

	config, err := sampleConfigFromEnv("./schemas")
	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) || envErr.Key != "VALIDATE_UPDATES_STRICT" {
		t.Fatalf("expected an *bootstrap.EnvError on VALIDATE_UPDATES_STRICT, got %v", err)
	}
	if config != nil {
		t.Fatalf("a refused environment must return no config, got %+v", config)
	}
}

// TestSampleConfigFromEnv pins what the extraction from runSampleImport must
// keep: the sample tables, the schema directory and the strict-update overlay.
func TestSampleConfigFromEnv(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "TRUE")

	config, err := sampleConfigFromEnv("./schemas")
	if err != nil {
		t.Fatalf("sampleConfigFromEnv: %v", err)
	}
	if !config.Entity.ValidateUpdatesStrict {
		t.Fatal("VALIDATE_UPDATES_STRICT=TRUE must enable strict updates")
	}
	if config.Entity.SchemaDirectory != "./schemas" {
		t.Fatalf("expected schema directory ./schemas, got %q", config.Entity.SchemaDirectory)
	}
	tables := config.Database.TableNames
	if tables.EAVData != "eav_data_sample" || tables.EntityMain != "entity_main_sample" ||
		tables.SchemaRegistry != "schema_registry_sample" || tables.ChangeLog != "change_log_sample" {
		t.Fatalf("expected the sample tables, got %+v", tables)
	}
}

// TestSetupSampleVerboseEnablesDebug pins #439: the logger used to be built
// before the flags were parsed, so it always saw -verbose's zero value and
// never logged at debug level.
func TestSetupSampleVerboseEnablesDebug(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantDebug bool
	}{
		{name: "verbose", args: []string{"-verbose", "-csv", "in.csv"}, wantDebug: true},
		{name: "default", args: []string{"-csv", "in.csv"}, wantDebug: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := zap.ReplaceGlobals(zap.L())
			t.Cleanup(restore)

			fs := flag.NewFlagSet("sample", flag.ContinueOnError)
			flags, sugar, syncLogger, err := setupSample(fs, tc.args)
			if err != nil {
				t.Fatalf("setupSample(%v): %v", tc.args, err)
			}
			t.Cleanup(syncLogger)

			if got := *flags.verbose; got != tc.wantDebug {
				t.Fatalf("-verbose parsed as %v, want %v", got, tc.wantDebug)
			}
			if *flags.csvFile != "in.csv" {
				t.Fatalf("-csv parsed as %q, want %q", *flags.csvFile, "in.csv")
			}
			if got := sugar.Desugar().Core().Enabled(zapcore.DebugLevel); got != tc.wantDebug {
				t.Fatalf("logger debug level enabled = %v, want %v", got, tc.wantDebug)
			}
		})
	}
}

// TestSetupSampleRejectsUnknownFlag checks a parse failure is returned wrapped
// rather than building a logger from half-parsed flags.
func TestSetupSampleRejectsUnknownFlag(t *testing.T) {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	_, sugar, _, err := setupSample(fs, []string{"-no-such-flag"})
	if err == nil {
		t.Fatal("expected an error for an unknown flag")
	}
	if sugar != nil {
		t.Fatal("a refused parse must not build a logger")
	}
}
