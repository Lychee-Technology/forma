package main

import (
	"flag"
	"os"
	"testing"
)

func TestPostgresFlagsResolvedPasswordFallback(t *testing.T) {
	t.Setenv("PGPASSWORD", "from-env")

	var flags postgresFlags
	fs := flag.NewFlagSet("postgres", flag.ContinueOnError)
	flags.register(fs, postgresFlagOptions{
		hostFlag:        "pg-host",
		portFlag:        "pg-port",
		userFlag:        "pg-user",
		passwordFlag:    "pg-password",
		databaseFlag:    "pg-db",
		sslModeFlag:     "pg-ssl-mode",
		hostDefault:     "localhost",
		portDefault:     5432,
		userDefault:     "postgres",
		passwordDefault: "",
		databaseDefault: "forma",
		sslModeDefault:  "require",
		hostUsage:       "host",
		portUsage:       "port",
		userUsage:       "user",
		passwordUsage:   "password",
		databaseUsage:   "database",
		sslModeUsage:    "sslmode",
	})

	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if got := flags.resolvedPassword("PGPASSWORD"); got != "from-env" {
		t.Fatalf("expected env password, got %q", got)
	}
}

func TestSchemaRegistryFlagsValidate(t *testing.T) {
	tests := []struct {
		name     string
		flags    schemaRegistryFlags
		required bool
		wantErr  bool
	}{
		{name: "optional empty", flags: schemaRegistryFlags{}, required: false},
		{name: "optional pair", flags: schemaRegistryFlags{table: "schema_registry", dir: "/tmp/schemas"}, required: false},
		{name: "optional partial", flags: schemaRegistryFlags{table: "schema_registry"}, required: false, wantErr: true},
		{name: "required pair", flags: schemaRegistryFlags{table: "schema_registry", dir: "/tmp/schemas"}, required: true},
		{name: "required missing dir", flags: schemaRegistryFlags{table: "schema_registry"}, required: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.flags.validate(tt.required)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestS3FlagsValidate(t *testing.T) {
	if err := (s3Flags{}).validate(true); err == nil {
		t.Fatal("expected bucket validation error")
	}
	if err := (s3Flags{bucket: "bucket"}).validate(true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPostgresFlagsRegisterDefaults(t *testing.T) {
	t.Setenv("DB_HOST", "db.internal")
	t.Setenv("DB_PORT", "15432")
	defer os.Unsetenv("DB_HOST")
	defer os.Unsetenv("DB_PORT")

	var flags postgresFlags
	fs := flag.NewFlagSet("postgres", flag.ContinueOnError)
	flags.register(fs, postgresFlagOptions{
		hostFlag:        "db-host",
		portFlag:        "db-port",
		userFlag:        "db-user",
		passwordFlag:    "db-password",
		databaseFlag:    "db-name",
		sslModeFlag:     "db-ssl-mode",
		hostDefault:     os.Getenv("DB_HOST"),
		portDefault:     15432,
		userDefault:     "postgres",
		passwordDefault: "postgres",
		databaseDefault: "forma",
		sslModeDefault:  "disable",
		hostUsage:       "host",
		portUsage:       "port",
		userUsage:       "user",
		passwordUsage:   "password",
		databaseUsage:   "database",
		sslModeUsage:    "sslmode",
	})

	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if flags.host != "db.internal" || flags.port != 15432 {
		t.Fatalf("unexpected defaults: host=%s port=%d", flags.host, flags.port)
	}
}
