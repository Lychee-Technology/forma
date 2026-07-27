package cdc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveStaticS3Credentials(t *testing.T) {
	cases := []struct {
		name                           string
		cfgKey, cfgSecret, cfgToken    string
		envKey, envSecret, envToken    string
		wantKey, wantSecret, wantToken string
	}{
		{name: "config pair wins over env", cfgKey: "ck", cfgSecret: "cs", envKey: "ek", envSecret: "es", wantKey: "ck", wantSecret: "cs"},
		{name: "config key never cross-mixes with env secret", cfgKey: "ck", envSecret: "es", wantKey: "ck", wantSecret: ""},
		{name: "full env pair becomes static", envKey: "ek", envSecret: "es", wantKey: "ek", wantSecret: "es"},
		{name: "env half-pair falls to default chain", envKey: "ek", wantKey: "", wantSecret: ""},
		{name: "env secret alone falls to default chain", envSecret: "es", wantKey: "", wantSecret: ""},
		{name: "config secret without key falls to default chain", cfgSecret: "cs", wantKey: "", wantSecret: ""},
		{name: "nothing set falls to default chain", wantKey: "", wantSecret: ""},

		// #329 token discipline: the token rides the source that supplied the pair.
		{name: "config pair carries the config token", cfgKey: "ck", cfgSecret: "cs", cfgToken: "ct",
			wantKey: "ck", wantSecret: "cs", wantToken: "ct"},
		{name: "config pair does not adopt an ambient env token", cfgKey: "ck", cfgSecret: "cs", envToken: "et",
			wantKey: "ck", wantSecret: "cs", wantToken: ""},
		{name: "env pair carries the env token", envKey: "ek", envSecret: "es", envToken: "et",
			wantKey: "ek", wantSecret: "es", wantToken: "et"},
		{name: "env pair does not adopt the config token", cfgToken: "ct", envKey: "ek", envSecret: "es", envToken: "et",
			wantKey: "ek", wantSecret: "es", wantToken: "et"},
		{name: "env pair with no env token does not fall back to the config token", cfgToken: "ct", envKey: "ek", envSecret: "es",
			wantKey: "ek", wantSecret: "es", wantToken: ""},
		{name: "half env pair drops the env token too", envKey: "ek", envToken: "et",
			wantKey: "", wantSecret: "", wantToken: ""},
		{name: "config token alone is not a credential source", cfgToken: "ct",
			wantKey: "", wantSecret: "", wantToken: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_ACCESS_KEY_ID", tc.envKey)
			t.Setenv("AWS_SECRET_ACCESS_KEY", tc.envSecret)
			t.Setenv("AWS_SESSION_TOKEN", tc.envToken)
			key, secret, token := ResolveStaticS3Credentials(CDCConfig{
				S3AccessKeyID:     tc.cfgKey,
				S3SecretAccessKey: tc.cfgSecret,
				S3SessionToken:    tc.cfgToken,
			})
			require.Equal(t, tc.wantKey, key)
			require.Equal(t, tc.wantSecret, secret)
			require.Equal(t, tc.wantToken, token)
		})
	}
}
