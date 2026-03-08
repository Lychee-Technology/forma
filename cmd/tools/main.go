package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

type toolCommand struct {
	name        string
	run         func(ctx context.Context, args []string) error
	exitOnError bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runToolMain(ctx, os.Args[1:], os.Stdout))
}

func runToolMain(ctx context.Context, args []string, out io.Writer) int {
	if len(args) < 1 {
		printUsage(out)
		return 1
	}

	cmd, ok := lookupToolCommand(args[0])
	if !ok {
		fmt.Fprintf(out, "unknown command %q\n", args[0])
		printUsage(out)
		return 1
	}

	if err := cmd.run(ctx, args[1:]); err != nil {
		fmt.Fprintf(out, "%s: %v\n", cmd.name, err)
		if cmd.exitOnError {
			return 1
		}
	}

	return 0
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
		{name: "cdc-flush", run: runCDCFlush, exitOnError: true},
		{name: "cdc-init", run: runCDCInit, exitOnError: true},
		{name: "compactor", run: runCompactor, exitOnError: true},
	}
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage: forma-tools <command> [options]")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  generate-attributes   Generate <schema>_attributes.json from a JSON schema file")
	fmt.Fprintln(out, "  init-db               Create PostgreSQL tables and indexes for Forma")
	fmt.Fprintln(out, "  inline-schema         Inline $ref references and remove x-* extension properties from a JSON schema")
	fmt.Fprintln(out, "  cdc-flush             Run CDC change_log flush to S3 parquet files")
	fmt.Fprintln(out, "  cdc-init              Initialize S3 parquet base files from existing data")
	fmt.Fprintln(out, "  compactor             Run compaction on parquet files for a schema")
}
