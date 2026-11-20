package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"go.uber.org/zap"
)

// ErrSourceChecksumMismatch marks a merge source whose bytes no longer hash
// to the digest its manifest entry lists. The merge must NOT proceed: it
// folds the sources into a new base and the published generation stops
// listing them, so this gate is the last moment silent corruption (#347) is
// both detectable and attributable to a listed object. Probe failures never
// map to this error: a failed GET is transient infrastructure, not a
// corruption verdict.
var ErrSourceChecksumMismatch = errors.New("compaction source checksum mismatch")

// verifySourceChecksums re-hashes every merge source and compares it with
// the sha256 its manifest entry lists. Every V2 entry lists one, so every
// source is checked. Disabled when skip is set or no ObjectReader is wired.
// A refusal is logged at ERROR here.
func (c *Compactor) verifySourceChecksums(ctx context.Context, schemaID int16, sources []manifestv2.File, skip bool) error {
	if skip || c.ObjectReader == nil {
		return nil
	}
	for _, f := range sources {
		sum, err := cdc.ObjectSHA256(ctx, c.ObjectReader, c.Bucket, f.Key)
		if err != nil {
			return fmt.Errorf("verify merge source %s of schema %d: %w", f.Key, schemaID, err)
		}
		actual := strings.TrimPrefix(sum, "sha256:")
		if actual != f.SHA256 {
			c.Metrics.EmitParquetChecksumMismatch(ctx, schemaID)
			c.log().Error("merge source failed checksum verification; refusing to merge",
				zap.Int16("schema_id", schemaID), zap.String("key", f.Key),
				zap.String("listed", f.SHA256), zap.String("actual", actual))
			return fmt.Errorf("merge source %s of schema %d: listed %s, bytes hash to %s: %w",
				f.Key, schemaID, f.SHA256, actual, ErrSourceChecksumMismatch)
		}
	}
	return nil
}
