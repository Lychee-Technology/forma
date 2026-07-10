package cdc

import (
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// resolveInitBatchSize derives the batch size for a schema's base export from
// the target file size and (auto-)estimated row bytes.
// Extracted from cmd/tools/cdc_init.go (mechanical move, #173).
func resolveInitBatchSize(runCtx *initRunContext, schemaID int16, attrCache forma.SchemaAttributeCache) int {
	batchSize := runCtx.cfg.BatchSize
	if runCtx.cfg.TargetFileSizeMB <= 0 {
		return batchSize
	}

	rowBytes := runCtx.cfg.EstimatedRowBytes
	if runCtx.autoEstimateRowBytes {
		rowBytes = estimateRowSizeBytes(attrCache)
	}
	batchSize = calculateBatchSize(runCtx.cfg.TargetFileSizeMB, rowBytes, runCtx.cfg.MaxBatchSize)
	runCtx.logger.Info("calculated batch size for target file size",
		zap.Int16("schema_id", schemaID),
		zap.Int("target_file_size_mb", runCtx.cfg.TargetFileSizeMB),
		zap.Int("estimated_row_bytes", rowBytes),
		zap.Int("calculated_batch_size", batchSize))
	return batchSize
}

// estimateRowSizeBytes estimates the average row size in bytes based on schema attributes.
// This is used to calculate optimal batch size for target file sizes.
func estimateRowSizeBytes(attrCache forma.SchemaAttributeCache) int {
	// Base overhead: row_id (UUID=16), schema_id (int16=2), timestamps (3 * int64=24)
	const baseOverhead = 50

	if len(attrCache) == 0 {
		// Fallback when no schema: assume medium-sized rows
		return 500
	}

	totalBytes := baseOverhead
	for _, meta := range attrCache {
		switch meta.ValueType {
		case forma.ValueTypeText:
			// Text fields vary widely; assume average 100 bytes
			totalBytes += 100
		case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger:
			// Numeric types: 8 bytes (double/bigint)
			totalBytes += 8
		case forma.ValueTypeSmallInt:
			totalBytes += 2
		case forma.ValueTypeBool:
			totalBytes += 1
		case forma.ValueTypeDate, forma.ValueTypeDateTime:
			// Dates stored as unix_ms (int64)
			totalBytes += 8
		case forma.ValueTypeUUID:
			totalBytes += 16
		case forma.ValueTypeList:
			// Lists are variable; assume average 200 bytes
			totalBytes += 200
		default:
			// Unknown type, assume text-like
			totalBytes += 100
		}
	}
	return totalBytes
}

// calculateBatchSize computes optimal batch size to achieve target file size.
// Returns the calculated batch size, capped at maxBatchSize to avoid memory issues.
func calculateBatchSize(targetFileSizeMB int, estimatedRowBytes int, maxBatchSize int) int {
	if estimatedRowBytes <= 0 {
		estimatedRowBytes = 500 // fallback
	}
	if targetFileSizeMB <= 0 {
		targetFileSizeMB = 256 // default 256MB
	}

	targetBytes := int64(targetFileSizeMB) * 1024 * 1024
	batchSize := min(
		// Cap at maxBatchSize to avoid memory issues
		int(targetBytes/int64(estimatedRowBytes)), maxBatchSize)
	// Minimum batch size
	if batchSize < 1000 {
		batchSize = 1000
	}
	return batchSize
}
