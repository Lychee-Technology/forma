package main

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
)

// drop-schema names its schema exactly once and purges in positive chunks;
// anything else is a flag error before any connection is opened.
func TestDropSchemaRefusesAnAmbiguousTargetBeforeConnecting(t *testing.T) {
	t.Setenv("DB_HOST", "unreachable.invalid")
	for name, c := range map[string]struct {
		args  []string
		field string
	}{
		"no schema":    {nil, "schema-name"},
		"both":         {[]string{"--schema-name", "timer", "--schema-id", "101"}, "schema-name"},
		"negative id":  {[]string{"--schema-id", "-4"}, "schema-id"},
		"id too large": {[]string{"--schema-id", "40000"}, "schema-id"},
		"zero purge":   {[]string{"--schema-id", "101", "--purge-batch", "0"}, "purge-batch"},
		"neg purge":    {[]string{"--schema-name", "timer", "--purge-batch", "-1"}, "purge-batch"},
	} {
		t.Run(name, func(t *testing.T) {
			var cfgErr *forma.ConfigError
			err := runDropSchema(context.Background(), c.args)
			if !errors.As(err, &cfgErr) || cfgErr.Field != c.field {
				t.Fatalf("drop-schema %v: want a ConfigError on %s, got %v", c.args, c.field, err)
			}
		})
	}
}
