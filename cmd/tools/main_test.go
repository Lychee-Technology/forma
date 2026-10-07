package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

func TestRunToolMain_UnknownCommand(t *testing.T) {
	var errOut bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{"nope"}, &errOut)
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	if !strings.Contains(errOut.String(), `unknown command "nope"`) {
		t.Fatalf("expected unknown command output, got %q", errOut.String())
	}
}

// TestRunToolMain_EveryCommandErrorExitsNonZero drives every registered
// subcommand into an error and requires a non-zero exit plus an error line
// naming the command (#643). Iterating toolCommands() covers a future
// registration without editing this test. An undefined flag is the failure
// every command shares: each parses with flag.ContinueOnError and returns the
// parse error.
func TestRunToolMain_EveryCommandErrorExitsNonZero(t *testing.T) {
	for _, cmd := range toolCommands() {
		t.Run(cmd.name, func(t *testing.T) {
			var errOut bytes.Buffer
			var code int
			captureStdStreams(t, func() {
				code = runToolMain(context.Background(), []string{cmd.name, "-no-such-flag"}, &errOut)
			})
			if code == 0 {
				t.Fatalf("%s with an undefined flag exited 0; a failing command must exit non-zero", cmd.name)
			}
			line := errOut.String()
			if !strings.HasPrefix(line, cmd.name+": ") || !strings.Contains(line, "flag provided but not defined: -no-such-flag") {
				t.Fatalf("%s: want an error line naming the command and the flag error, got %q", cmd.name, line)
			}
		})
	}
}

// TestRunToolMain_EveryCommandHelpExitsZero keeps -h a success now that
// every error exits non-zero: each command maps flag.ErrHelp to nil.
func TestRunToolMain_EveryCommandHelpExitsZero(t *testing.T) {
	for _, cmd := range toolCommands() {
		t.Run(cmd.name, func(t *testing.T) {
			var errOut bytes.Buffer
			var code int
			captureStdStreams(t, func() {
				code = runToolMain(context.Background(), []string{cmd.name, "-h"}, &errOut)
			})
			if code != 0 || errOut.Len() != 0 {
				t.Fatalf("%s -h: exit code %d, error output %q; want 0 and none", cmd.name, code, errOut.String())
			}
		})
	}
}

// TestRunToolMain_CommandFailuresExitOne covers the failures #643 names for
// the three commands that used to exit 0 on any error.
func TestRunToolMain_CommandFailuresExitOne(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	tests := []struct {
		name     string
		args     []string
		dbPort   string
		poolErr  error
		wantLine string
	}{
		{
			name:     "init-db cannot reach the database",
			args:     []string{"init-db"},
			poolErr:  errors.New("failed to ping database: connection refused"),
			wantLine: "init-db: create connection pool: failed to ping database: connection refused",
		},
		{
			name:     "init-db with an unparsable DB_PORT",
			args:     []string{"init-db"},
			dbPort:   "tcp://10.0.0.7:5432",
			wantLine: `init-db: resolve --db-port default: environment variable DB_PORT="tcp://10.0.0.7:5432"`,
		},
		{
			name:     "generate-attributes without a schema",
			args:     []string{"generate-attributes"},
			wantLine: "generate-attributes: either -schema or -schema-file must be provided",
		},
		{
			name:     "inline-schema with a missing schema file",
			args:     []string{"inline-schema", "-schema-file", missing},
			wantLine: "inline-schema: inline schema: read file " + missing,
		},
		{
			name:     "cdc-flush without its required flags",
			args:     []string{"cdc-flush"},
			wantLine: "cdc-flush: ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DB_PORT", tt.dbPort)
			stubToolPostgresPool(t, tt.poolErr)

			var errOut bytes.Buffer
			var code int
			captureStdStreams(t, func() {
				code = runToolMain(context.Background(), tt.args, &errOut)
			})
			if code != 1 {
				t.Fatalf("exit code = %d, want 1 (error output %q)", code, errOut.String())
			}
			if !strings.HasPrefix(errOut.String(), tt.wantLine) {
				t.Fatalf("error output = %q, want it to start with %q", errOut.String(), tt.wantLine)
			}
		})
	}
}

// stubToolPostgresPool makes every tool pool fail with err, so no test reaches
// a real database. A nil err stands for "the test never opens a pool".
func stubToolPostgresPool(t *testing.T, err error) {
	t.Helper()
	old := toolPostgresPoolFn
	t.Cleanup(func() { toolPostgresPoolFn = old })
	if err == nil {
		err = errors.New("unexpected pool request")
	}
	toolPostgresPoolFn = func(context.Context, forma.DatabaseConfig) (*pgxpool.Pool, error) {
		return nil, err
	}
}

// TestToolsProcess_ExitStatusAndStreams runs the real main in a child
// process, the way a script runs forma-tools: the exit status and which stream
// carries the error line are main's wiring, which the runToolMain tests cannot
// observe (#643). A failure must exit non-zero with the error line on stderr
// only, and a success keeps stdout for the command's own output.
func TestToolsProcess_ExitStatusAndStreams(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	missing := filepath.Join(dir, "missing.json")

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{"failure", []string{"inline-schema", "-schema-file", missing}, 1, "", "inline-schema: inline schema: read file " + missing},
		{"success", []string{"inline-schema", "-schema-file", schemaPath}, 0, `"type": "object"`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runToolsProcess(t, tt.args...)
			if code != tt.wantCode {
				t.Fatalf("exit code = %d, want %d (stdout %q, stderr %q)", code, tt.wantCode, stdout, stderr)
			}
			assertStream(t, "stdout", stdout, tt.wantStdout)
			assertStream(t, "stderr", stderr, tt.wantStderr)
		})
	}
}

// assertStream requires got to contain want, or to be empty when want is.
func assertStream(t *testing.T, name, got, want string) {
	t.Helper()
	if want == "" && got != "" {
		t.Fatalf("%s = %q, want it empty", name, got)
	}
	if !strings.Contains(got, want) {
		t.Fatalf("%s = %q, want it to contain %q", name, got, want)
	}
}

// runToolsProcess re-executes the test binary as forma-tools with args and
// returns its exit status and output streams.
func runToolsProcess(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestToolsMainHelperProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "FORMA_TOOLS_HELPER_PROCESS=1")
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &outBuf, &errBuf
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		code = 0
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
	default:
		t.Fatalf("run forma-tools %v: %v", args, err)
	}
	return code, outBuf.String(), errBuf.String()
}

// TestToolsMainHelperProcess is not a test: runToolsProcess starts the test
// binary with it selected, and it runs main with the arguments after "--".
func TestToolsMainHelperProcess(t *testing.T) {
	if os.Getenv("FORMA_TOOLS_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	os.Args = append([]string{"forma-tools"}, args...)
	main()
}

func TestRunToolMain_Success(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "schema_attributes.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object","properties":{"name":{"type":"string"}}}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}

	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{
		"generate-attributes",
		"-schema-file", schemaPath,
		"-out", outputPath,
		"-init",
	}, &out)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d output=%q", exitCode, out.String())
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected output file: %v", err)
	}
}

func TestRunToolMain_ValidateSchemaConsistencySuccess(t *testing.T) {
	old := runValidateSchemaConsistencyFn
	defer func() { runValidateSchemaConsistencyFn = old }()

	called := false
	runValidateSchemaConsistencyFn = func(ctx context.Context, args []string) error {
		called = true
		if len(args) != 1 || args[0] != "-h" {
			t.Fatalf("unexpected args: %v", args)
		}
		return nil
	}

	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{"validate-schema-consistency", "-h"}, &out)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d output=%q", exitCode, out.String())
	}
	if !called {
		t.Fatal("expected validate-schema-consistency command to run")
	}
}
