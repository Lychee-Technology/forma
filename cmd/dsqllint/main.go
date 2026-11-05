// Command dsqllint is the static Aurora DSQL compatibility gate (#740) that
// `make lint-dsql` runs, locally and in CI. It needs no database, no Docker
// and no AWS credentials; on first use it downloads the pinned dsql-lint
// release and verifies its sha256.
//
//	go run ./cmd/dsqllint [-cache build/tools] [-keep-corpus DIR]
//
// DSQL_LINT_BIN names a dsql-lint binary to use instead of the download; it
// must still report the pinned version.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/lychee-technology/forma/internal/dsqllint"
)

func main() {
	if err := run(); err != nil {
		if !errors.Is(err, dsqllint.ErrGateFailed) {
			fmt.Fprintf(os.Stderr, "dsqllint: %v\n", err)
		}
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet("dsqllint", flag.ContinueOnError)
	root := flags.String("root", ".", "repository root")
	cache := flags.String("cache", "build/tools", "directory the pinned dsql-lint binary is cached in")
	keep := flags.String("keep-corpus", "", "write the corpus records and the SQL linted from them to this new directory and keep it")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	tool, err := dsqllint.EnsureTool(ctx, *cache, os.Getenv("DSQL_LINT_BIN"))
	if err != nil {
		return fmt.Errorf("prepare dsql-lint %s: %w", dsqllint.ToolVersion, err)
	}
	fmt.Printf("Using dsql-lint %s at %s\n", dsqllint.ToolVersion, tool)
	if err := dsqllint.Run(ctx, dsqllint.Config{Root: *root, Tool: tool, CorpusDir: *keep, Out: os.Stdout}); err != nil {
		return fmt.Errorf("run the DSQL lint gate: %w", err)
	}
	return nil
}
