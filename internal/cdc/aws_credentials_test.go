package cdc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveStaticS3Credentials(t *testing.T) {
	cases := []struct {
		name                string
		cfgKey, cfgSecret   string
		envKey, envSecret   string
		wantKey, wantSecret string
	}{
		{name: "config pair wins over env", cfgKey: "ck", cfgSecret: "cs", envKey: "ek", envSecret: "es", wantKey: "ck", wantSecret: "cs"},
		{name: "config key never cross-mixes with env secret", cfgKey: "ck", envSecret: "es", wantKey: "ck", wantSecret: ""},
		{name: "full env pair becomes static", envKey: "ek", envSecret: "es", wantKey: "ek", wantSecret: "es"},
		{name: "env half-pair falls to default chain", envKey: "ek", wantKey: "", wantSecret: ""},
		{name: "env secret alone falls to default chain", envSecret: "es", wantKey: "", wantSecret: ""},
		{name: "config secret without key falls to default chain", cfgSecret: "cs", wantKey: "", wantSecret: ""},
		{name: "nothing set falls to default chain", wantKey: "", wantSecret: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_ACCESS_KEY_ID", tc.envKey)
			t.Setenv("AWS_SECRET_ACCESS_KEY", tc.envSecret)
			key, secret := resolveStaticS3Credentials(CDCConfig{S3AccessKeyID: tc.cfgKey, S3SecretAccessKey: tc.cfgSecret})
			require.Equal(t, tc.wantKey, key)
			require.Equal(t, tc.wantSecret, secret)
		})
	}
}
