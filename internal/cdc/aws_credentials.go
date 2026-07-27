package cdc

import "os"

// resolveStaticS3Credentials resolves the static S3 credential pair shared by
// every CDC credential site — the SDK S3 client and the DuckDB httpfs
// plumbing (#326). Explicit config wins as-is. Otherwise the environment pair
// applies only when BOTH halves are non-empty: a lone AWS_ACCESS_KEY_ID would
// build an empty-secret static provider whose only observable behavior is an
// opaque signing failure (#302). With no fully-set pair it returns empty
// strings — SDK callers then leave the default chain in place, and DuckDB
// inherits its own environment chain.
func resolveStaticS3Credentials(cfg CDCConfig) (accessKeyID, secretAccessKey string) {
	if cfg.S3AccessKeyID != "" {
		return cfg.S3AccessKeyID, cfg.S3SecretAccessKey
	}
	envKey, envSecret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if envKey != "" && envSecret != "" {
		return envKey, envSecret
	}
	return "", ""
}
