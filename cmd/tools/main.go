package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate-attributes":
		if err := runGenerateAttributes(os.Args[2:]); err != nil {
			fmt.Printf("generate-attributes: %v\n", err)
		}
	case "init-db":
		if err := runInitDB(os.Args[2:]); err != nil {
			fmt.Printf("init-db: %v\n", err)
		}
	case "inline-schema":
		if err := runInlineSchema(os.Args[2:]); err != nil {
			fmt.Printf("inline-schema: %v\n", err)
		}
	case "cdc-flush":
		if err := runCDCFlush(os.Args[2:]); err != nil {
			fmt.Printf("cdc-flush: %v\n", err)
			os.Exit(1)
		}
	case "compactor":
		if err := runCompactor(os.Args[2:]); err != nil {
			fmt.Printf("compactor: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("unknown command %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: forma-tools <command> [options]")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  generate-attributes   Generate <schema>_attributes.json from a JSON schema file")
	fmt.Println("  init-db               Create PostgreSQL tables and indexes for Forma")
	fmt.Println("  inline-schema         Inline $ref references and remove x-* extension properties from a JSON schema")
	fmt.Println("  cdc-flush             Run CDC change_log flush to S3 parquet files")
	fmt.Println("  compactor             Run compaction on parquet files for a schema")
}
