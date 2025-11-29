package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lychee-technology/forma"
)

// ImportError represents an error that occurred while importing a single CSV row.
type ImportError struct {
	RowNumber  int    // CSV row number (1-based, including header)
	CSVColumn  string // CSV column name that caused the error
	SchemaPath string // Target schema path
	RawValue   string // Original CSV value
	Reason     string // Error description
}

func (e *ImportError) Error() string {
	return fmt.Sprintf("row %d, column %q -> path %q: value %q - %s",
		e.RowNumber, e.CSVColumn, e.SchemaPath, e.RawValue, e.Reason)
}

// ImportResult contains the results of a CSV import operation.
type ImportResult struct {
	TotalRows    int            // Total number of data rows in CSV (excluding header)
	SuccessCount int            // Number of successfully imported rows
	FailedCount  int            // Number of failed rows
	Errors       []*ImportError // Detailed error information for failed rows
	Duration     time.Duration  // Total import duration
}

// Summary returns a human-readable summary of the import result.
func (r *ImportResult) Summary() string {
	return fmt.Sprintf("Import completed: %d/%d rows successful, %d failed, duration: %v",
		r.SuccessCount, r.TotalRows, r.FailedCount, r.Duration)
}

// CSVImporter handles importing CSV data into a schema using the EntityManager.
type CSVImporter struct {
	entityManager forma.EntityManager
	mapper        CSVToSchemaMapper
	batchSize     int
	logger        *log.Logger
}

// NewCSVImporter creates a new CSVImporter.
// batchSize determines how many records are batched together for insertion.
// If batchSize <= 0, defaults to 100.
func NewCSVImporter(entityManager forma.EntityManager, mapper CSVToSchemaMapper, batchSize int) *CSVImporter {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &CSVImporter{
		entityManager: entityManager,
		mapper:        mapper,
		batchSize:     batchSize,
		logger:        log.New(os.Stderr, "[CSVImporter] ", log.LstdFlags),
	}
}

// SetLogger sets a custom logger for the importer.
func (i *CSVImporter) SetLogger(logger *log.Logger) {
	i.logger = logger
}

// ImportFromFile imports CSV data from a file.
func (i *CSVImporter) ImportFromFile(ctx context.Context, filePath string) (*ImportResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	return i.ImportFromReader(ctx, file)
}

// ImportFromReader imports CSV data from an io.Reader.
func (i *CSVImporter) ImportFromReader(ctx context.Context, reader io.Reader) (*ImportResult, error) {
	startTime := time.Now()

	csvReader := csv.NewReader(reader)
	csvReader.TrimLeadingSpace = true

	// Read header row
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	result := &ImportResult{
		Errors: make([]*ImportError, 0),
	}

	schemaName := i.mapper.SchemaName()
	batch := make([]forma.EntityOperation, 0, i.batchSize)
	rowNum := 1 // Header is row 1

	for {
		rowNum++
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log CSV parsing error and continue
			i.logger.Printf("[ERROR] Row %d: CSV parsing error: %v", rowNum, err)
			result.FailedCount++
			result.Errors = append(result.Errors, &ImportError{
				RowNumber: rowNum,
				Reason:    fmt.Sprintf("CSV parsing error: %v", err),
			})
			continue
		}

		result.TotalRows++

		// Build CSV record map
		csvRecord := make(map[string]string)
		for idx, col := range header {
			if idx < len(record) {
				csvRecord[col] = record[idx]
			}
		}

		// Map CSV record to schema attributes
		attributes, err := i.mapper.MapRecord(csvRecord)
		if err != nil {
			// Handle mapping error
			if mappingErr, ok := err.(*MappingError); ok {
				importErr := &ImportError{
					RowNumber:  rowNum,
					CSVColumn:  mappingErr.CSVColumn,
					SchemaPath: mappingErr.SchemaPath,
					RawValue:   mappingErr.RawValue,
					Reason:     mappingErr.Reason,
				}
				i.logger.Printf("[ERROR] %s", importErr.Error())
				result.Errors = append(result.Errors, importErr)
			} else {
				importErr := &ImportError{
					RowNumber: rowNum,
					Reason:    err.Error(),
				}
				i.logger.Printf("[ERROR] Row %d: mapping error: %v", rowNum, err)
				result.Errors = append(result.Errors, importErr)
			}
			result.FailedCount++
			continue
		}

		// Add to batch
		batch = append(batch, forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{
				SchemaName: schemaName,
			},
			Type: forma.OperationCreate,
			Data: attributes,
		})

		// Process batch if full
		if len(batch) >= i.batchSize {
			successCount, batchErrors := i.processBatch(ctx, batch, rowNum-len(batch)+1)
			result.SuccessCount += successCount
			result.FailedCount += len(batch) - successCount
			result.Errors = append(result.Errors, batchErrors...)
			batch = batch[:0] // Reset batch
		}
	}

	// Process remaining batch
	if len(batch) > 0 {
		successCount, batchErrors := i.processBatch(ctx, batch, rowNum-len(batch))
		result.SuccessCount += successCount
		result.FailedCount += len(batch) - successCount
		result.Errors = append(result.Errors, batchErrors...)
	}

	result.Duration = time.Since(startTime)

	i.logger.Printf("%s", result.Summary())

	return result, nil
}

// processBatch processes a batch of entity operations and returns the number of successful operations.
func (i *CSVImporter) processBatch(ctx context.Context, batch []forma.EntityOperation, startRowNum int) (int, []*ImportError) {
	batchOp := &forma.BatchOperation{
		Operations: batch,
		Atomic:     false, // Non-atomic to allow partial success
	}

	batchResult, err := i.entityManager.BatchCreate(ctx, batchOp)
	if err != nil {
		// If batch creation fails entirely, log error and return 0 success
		i.logger.Printf("[ERROR] Batch creation failed: %v", err)
		errors := make([]*ImportError, len(batch))
		for idx := range batch {
			errors[idx] = &ImportError{
				RowNumber: startRowNum + idx,
				Reason:    fmt.Sprintf("batch creation failed: %v", err),
			}
		}
		return 0, errors
	}

	// Log individual failures from batch result
	errors := make([]*ImportError, 0, len(batchResult.Failed))
	for _, opErr := range batchResult.Failed {
		// Find the row number for this failed operation
		// Since operations are in order, we can calculate the row number
		rowOffset := -1
		for idx, op := range batch {
			if op.SchemaName == opErr.Operation.SchemaName {
				// Simple match - in practice you might want a more sophisticated matching
				rowOffset = idx
				break
			}
		}

		importErr := &ImportError{
			RowNumber: startRowNum + rowOffset,
			Reason:    fmt.Sprintf("%s: %s", opErr.Code, opErr.Error),
		}
		i.logger.Printf("[ERROR] %s", importErr.Error())
		errors = append(errors, importErr)
	}

	return len(batchResult.Successful), errors
}

// ImportOptions provides additional configuration for import operations.
type ImportOptions struct {
	// SkipHeader indicates whether to skip the first row (default: false, first row is header)
	SkipHeader bool
	// Delimiter is the CSV field delimiter (default: comma)
	Delimiter rune
	// Comment is the character that starts a comment line (default: none)
	Comment rune
	// LazyQuotes allows lazy quotes in CSV parsing
	LazyQuotes bool
}

// DefaultImportOptions returns the default import options.
func DefaultImportOptions() ImportOptions {
	return ImportOptions{
		SkipHeader: false,
		Delimiter:  ',',
		Comment:    0,
		LazyQuotes: false,
	}
}

// ImportFromReaderWithOptions imports CSV data with custom options.
func (i *CSVImporter) ImportFromReaderWithOptions(ctx context.Context, reader io.Reader, opts ImportOptions) (*ImportResult, error) {
	startTime := time.Now()

	csvReader := csv.NewReader(reader)
	csvReader.TrimLeadingSpace = true
	csvReader.Comma = opts.Delimiter
	csvReader.Comment = opts.Comment
	csvReader.LazyQuotes = opts.LazyQuotes

	// Read header row
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	result := &ImportResult{
		Errors: make([]*ImportError, 0),
	}

	schemaName := i.mapper.SchemaName()
	batch := make([]forma.EntityOperation, 0, i.batchSize)
	rowNum := 1 // Header is row 1

	for {
		rowNum++
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			i.logger.Printf("[ERROR] Row %d: CSV parsing error: %v", rowNum, err)
			result.FailedCount++
			result.Errors = append(result.Errors, &ImportError{
				RowNumber: rowNum,
				Reason:    fmt.Sprintf("CSV parsing error: %v", err),
			})
			continue
		}

		result.TotalRows++

		csvRecord := make(map[string]string)
		for idx, col := range header {
			if idx < len(record) {
				csvRecord[col] = record[idx]
			}
		}

		attributes, err := i.mapper.MapRecord(csvRecord)
		if err != nil {
			if mappingErr, ok := err.(*MappingError); ok {
				importErr := &ImportError{
					RowNumber:  rowNum,
					CSVColumn:  mappingErr.CSVColumn,
					SchemaPath: mappingErr.SchemaPath,
					RawValue:   mappingErr.RawValue,
					Reason:     mappingErr.Reason,
				}
				i.logger.Printf("[ERROR] %s", importErr.Error())
				result.Errors = append(result.Errors, importErr)
			} else {
				importErr := &ImportError{
					RowNumber: rowNum,
					Reason:    err.Error(),
				}
				i.logger.Printf("[ERROR] Row %d: mapping error: %v", rowNum, err)
				result.Errors = append(result.Errors, importErr)
			}
			result.FailedCount++
			continue
		}

		batch = append(batch, forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{
				SchemaName: schemaName,
			},
			Type: forma.OperationCreate,
			Data: attributes,
		})

		if len(batch) >= i.batchSize {
			successCount, batchErrors := i.processBatch(ctx, batch, rowNum-len(batch)+1)
			result.SuccessCount += successCount
			result.FailedCount += len(batch) - successCount
			result.Errors = append(result.Errors, batchErrors...)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		successCount, batchErrors := i.processBatch(ctx, batch, rowNum-len(batch))
		result.SuccessCount += successCount
		result.FailedCount += len(batch) - successCount
		result.Errors = append(result.Errors, batchErrors...)
	}

	result.Duration = time.Since(startTime)
	i.logger.Printf("%s", result.Summary())

	return result, nil
}
