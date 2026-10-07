package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

type toolCommand struct {
	name string
	run  func(ctx context.Context, args []string) error
}

var runValidateSchemaConsistencyFn = runValidateSchemaConsistency

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runToolMain(ctx, os.Args[1:], os.Stderr))
}

// runToolMain dispatches args to a subcommand and returns the process exit
// status. Every failure is non-zero, so a script or deploy step running a tool
// can tell that it failed (#643). errOut receives only diagnostics (main
// passes stderr), which leaves stdout to the subcommands' own output.
func runToolMain(ctx context.Context, args []string, errOut io.Writer) int {
	if len(args) < 1 {
		printUsage(errOut)
		return 1
	}

	cmd, ok := lookupToolCommand(args[0])
	if !ok {
		fmt.Fprintf(errOut, "unknown command %q\n", args[0])
		printUsage(errOut)
		return 1
	}

	err := cmd.run(ctx, args[1:])
	if err == nil {
		return 0
	}
	// Commands report semantic outcomes (e.g. manifest-reconcile's
	// "discrepancies found" = 2) via an ExitCode; those already rendered
	// their output, so no error line is printed for them.
	var ec exitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	fmt.Fprintf(errOut, "%s: %v\n", cmd.name, err)
	return 1
}

// exitCoder lets a command error carry its own process exit code.
type exitCoder interface {
	ExitCode() int
}

func lookupToolCommand(name string) (toolCommand, bool) {
	for _, cmd := range toolCommands() {
		if cmd.name == name {
			return cmd, true
		}
	}
	return toolCommand{}, false
}

func toolCommands() []toolCommand {
	return []toolCommand{
		{name: "generate-attributes", run: runGenerateAttributes},
		{name: "init-db", run: runInitDB},
		{name: "inline-schema", run: runInlineSchema},
		{name: "validate-schema-consistency", run: runValidateSchemaConsistencyFn},
		{name: "cdc-flush", run: runCDCFlush},
		{name: "cdc-init", run: runCDCInit},
		{name: "compactor", run: runCompactor},
		{name: "manifest-reconcile", run: runManifestReconcileFn},
	}
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage: forma-tools <command> [options]")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  generate-attributes   Generate <schema>_attributes.json from a JSON schema file")
	fmt.Fprintln(out, "  init-db               Create PostgreSQL tables and indexes for Forma")
	fmt.Fprintln(out, "  inline-schema         Inline $ref references and remove x-* extension properties from a JSON schema")
	fmt.Fprintln(out, "  validate-schema-consistency Validate schema metadata and EAV storage before upgrade")
	fmt.Fprintln(out, "  cdc-flush             Run CDC change_log flush to S3 parquet files")
	fmt.Fprintln(out, "  cdc-init              Initialize S3 parquet base files from existing data")
	fmt.Fprintln(out, "  compactor             Run compaction on parquet files for a schema")
	fmt.Fprintln(out, "  manifest-reconcile    Diff S3 parquet objects against manifests; report, --repair orphaned deltas, --gc stale base/tmp leftovers (compaction + init-shaped)")
}
