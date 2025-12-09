package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Errorf("failed to set up logger: %w", err))
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)
	sugar := logger.Sugar()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate-attributes":
		if err := runGenerateAttributes(os.Args[2:]); err != nil {
			sugar.Fatalf("generate-attributes: %v", err)
		}
	case "init-db":
		if err := runInitDB(os.Args[2:]); err != nil {
			sugar.Fatalf("init-db: %v", err)
		}
	case "inline-schema":
		if err := runInlineSchema(os.Args[2:]); err != nil {
			sugar.Fatalf("inline-schema: %v", err)
		}
	default:
		sugar.Errorf("unknown command %q", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	logger := zap.S()
	logger.Info("Usage: forma-tools <command> [options]")
	logger.Info("")
	logger.Info("Commands:")
	logger.Info("  generate-attributes   Generate <schema>_attributes.json from a JSON schema file")
	logger.Info("  init-db               Create PostgreSQL tables and indexes for Forma")
	logger.Info("  inline-schema         Inline $ref references and remove x-* extension properties from a JSON schema")
}
