package federated

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// externalConfigEnvs is every variable loadExternalFederatedConfigFromEnv
// reads. clearExternalConfigEnv blanks them all, so a test sees only what it
// sets and not whatever the developer's shell exports.
var externalConfigEnvs = []string{
	externalPGDSNEnv, externalPGUserEnv, externalPGPasswordEnv, externalPGDBEnv,
	externalPGSSLModeEnv, externalS3EndpointEnv, externalS3BucketEnv,
	externalS3PrefixEnv, externalS3RegionEnv, externalS3AccessKeyEnv,
	externalS3SecretKeyEnv,
}

func clearExternalConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range externalConfigEnvs {
		t.Setenv(name, "")
	}
}

// setExternalS3Env sets the minimum that switches the harness to external
// mode: an S3 endpoint and both S3 credentials.
func setExternalS3Env(t *testing.T) {
	t.Helper()
	t.Setenv(externalS3EndpointEnv, " http://s3.example:9000/ ")
	t.Setenv(externalS3AccessKeyEnv, "access-id")
	t.Setenv(externalS3SecretKeyEnv, "secret-value")
}

// externalDSN builds the DSN at run time, so no literal URL carrying a
// password sits in the source.
func externalDSN() string {
	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("dsn-user", "dsn-pass"),
		Host:     "db.example:6543",
		Path:     "/dsn-db",
		RawQuery: "sslmode=require",
	}
	return dsn.String()
}

func TestLoadExternalFederatedConfigDisabledWithoutDSNOrEndpoint(t *testing.T) {
	clearExternalConfigEnv(t)
	t.Setenv(externalS3AccessKeyEnv, "access-id")

	cfg, err := loadExternalFederatedConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, externalFederatedConfig{}, *cfg)
}

func TestLoadExternalFederatedConfigRequiresS3Endpoint(t *testing.T) {
	clearExternalConfigEnv(t)
	t.Setenv(externalPGDSNEnv, externalDSN())

	_, err := loadExternalFederatedConfigFromEnv()
	require.ErrorContains(t, err, externalS3EndpointEnv)
}

func TestLoadExternalFederatedConfigRequiresBothS3Credentials(t *testing.T) {
	for _, missing := range []string{externalS3AccessKeyEnv, externalS3SecretKeyEnv} {
		t.Run(missing, func(t *testing.T) {
			clearExternalConfigEnv(t)
			setExternalS3Env(t)
			t.Setenv(missing, "")

			_, err := loadExternalFederatedConfigFromEnv()
			require.ErrorContains(t, err, "requires both "+externalS3AccessKeyEnv+" and "+externalS3SecretKeyEnv)
		})
	}
}

func TestLoadExternalFederatedConfigS3OnlyKeepsLocalPostgresDefaults(t *testing.T) {
	clearExternalConfigEnv(t)
	setExternalS3Env(t)

	cfg, err := loadExternalFederatedConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, externalFederatedConfig{
		Enabled:       true,
		UseExternalPG: false,
		UseExternalS3: true,
		PGHost:        "localhost",
		PGPort:        "5432",
		PGUser:        "postgres",
		PGDatabase:    "postgres",
		PGSSLMode:     "disable",
		S3Endpoint:    "http://s3.example:9000",
		S3Bucket:      "test-bucket",
		S3Prefix:      "test-project",
		S3Region:      "us-east-1",
		S3AccessKey:   "access-id",
		S3SecretKey:   "secret-value",
	}, *cfg)
}

func TestLoadExternalFederatedConfigTakesUnsetPostgresSettingsFromDSN(t *testing.T) {
	clearExternalConfigEnv(t)
	setExternalS3Env(t)
	dsn := externalDSN()
	t.Setenv(externalPGDSNEnv, dsn)

	cfg, err := loadExternalFederatedConfigFromEnv()
	require.NoError(t, err)
	require.True(t, cfg.UseExternalPG)
	require.Equal(t, dsn, cfg.PGDSN)
	require.Equal(t, "db.example", cfg.PGHost)
	require.Equal(t, "6543", cfg.PGPort)
	require.Equal(t, "dsn-user", cfg.PGUser)
	require.Equal(t, "dsn-pass", cfg.PGPassword)
	require.Equal(t, "dsn-db", cfg.PGDatabase)
	require.Equal(t, "require", cfg.PGSSLMode)
}

func TestLoadExternalFederatedConfigEnvOverridesDSN(t *testing.T) {
	clearExternalConfigEnv(t)
	setExternalS3Env(t)
	t.Setenv(externalPGDSNEnv, externalDSN())
	t.Setenv(externalPGUserEnv, "env-user")
	t.Setenv(externalPGPasswordEnv, "env-pass")
	t.Setenv(externalPGDBEnv, "env-db")
	t.Setenv(externalPGSSLModeEnv, "verify-full")
	t.Setenv(externalS3BucketEnv, "env-bucket")
	t.Setenv(externalS3PrefixEnv, "env-prefix")
	t.Setenv(externalS3RegionEnv, "eu-west-1")

	cfg, err := loadExternalFederatedConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, "env-user", cfg.PGUser)
	require.Equal(t, "env-pass", cfg.PGPassword)
	require.Equal(t, "env-db", cfg.PGDatabase)
	require.Equal(t, "verify-full", cfg.PGSSLMode)
	require.Equal(t, "env-bucket", cfg.S3Bucket)
	require.Equal(t, "env-prefix", cfg.S3Prefix)
	require.Equal(t, "eu-west-1", cfg.S3Region)
}

// With the user and database both set, the DSN is not consulted, so an unset
// password and SSL mode take no value from it. This is the original loader's
// behavior, and the split keeps it.
func TestLoadExternalFederatedConfigSkipsDSNWhenUserAndDatabaseSet(t *testing.T) {
	clearExternalConfigEnv(t)
	setExternalS3Env(t)
	t.Setenv(externalPGDSNEnv, externalDSN())
	t.Setenv(externalPGUserEnv, "env-user")
	t.Setenv(externalPGDBEnv, "env-db")

	cfg, err := loadExternalFederatedConfigFromEnv()
	require.NoError(t, err)
	require.Empty(t, cfg.PGPassword)
	require.Equal(t, "disable", cfg.PGSSLMode)
}

func TestLoadExternalFederatedConfigRejectsUnparseableDSN(t *testing.T) {
	clearExternalConfigEnv(t)
	setExternalS3Env(t)
	t.Setenv(externalPGDSNEnv, "postgres://host:port-not-a-number/db")

	_, err := loadExternalFederatedConfigFromEnv()
	require.ErrorContains(t, err, "parse external postgres dsn")
}
