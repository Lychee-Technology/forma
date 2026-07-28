package cdc

import "os"

// ResolveStaticS3Credentials resolves the static S3 credential triple shared by
// every CDC credential site — the SDK S3 client and the DuckDB httpfs
// plumbing (#326). Explicit config wins as-is. Otherwise the environment pair
// applies only when BOTH halves are non-empty: a lone AWS_ACCESS_KEY_ID would
// build an empty-secret static provider whose only observable behavior is an
// opaque signing failure (#302). With no fully-set pair it returns empty
// strings — SDK callers then leave the default chain in place, and DuckDB
// inherits its own environment chain.
//
// The session token rides the source that supplied the pair and never crosses
// sources (#329): a config pair carries only the config token, an environment
// pair carries only AWS_SESSION_TOKEN. Pairing a long-lived key with a foreign
// temporary token yields a combination the storage endpoint can only report as
// an opaque signing failure. A token with no accompanying pair is not a
// credential source at all and is discarded with the rest.
func ResolveStaticS3Credentials(cfg CDCConfig) (accessKeyID, secretAccessKey, sessionToken string) {
	if cfg.S3AccessKeyID != "" {
		return cfg.S3AccessKeyID, cfg.S3SecretAccessKey, cfg.S3SessionToken
	}
	envKey, envSecret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if envKey != "" && envSecret != "" {
		return envKey, envSecret, os.Getenv("AWS_SESSION_TOKEN")
	}
	return "", "", ""
}
