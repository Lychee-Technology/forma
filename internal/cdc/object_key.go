package cdc

import "strings"

// NormalizeObjectKey resolves a manifest path to a bucket-relative key. A
// path with glob metacharacters, or an s3:// URI for a different bucket,
// cannot be addressed through this bucket's client and reports ok=false.
// Relative keys are returned verbatim. The V1 compactor and
// manifest-reconcile share this rule for V1 manifest paths.
func NormalizeObjectKey(bucket, path string) (string, bool) {
	if strings.ContainsAny(path, "*?[") {
		return "", false
	}
	if strings.HasPrefix(path, "s3://") {
		key, found := strings.CutPrefix(path, "s3://"+bucket+"/")
		if !found {
			return "", false
		}
		return key, true
	}
	return path, true
}
