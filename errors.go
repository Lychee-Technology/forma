package forma

import (
	"fmt"
)

// ErrorType represents the category of error
type ErrorType string

const (
	ErrorTypeValidation   ErrorType = "validation"
	ErrorTypeExecution    ErrorType = "execution"
	ErrorTypeTimeout      ErrorType = "timeout"
	ErrorTypeUnauthorized ErrorType = "unauthorized"
	ErrorTypeNotFound     ErrorType = "not_found"
	ErrorTypeInternal     ErrorType = "internal"
	ErrorTypeTransaction  ErrorType = "transaction"
	ErrorTypeReference    ErrorType = "reference"
	ErrorTypeQuery        ErrorType = "query"
)

// FormaError represents unified errors from the enhanced DataPlane module
type FormaError struct {
	Type      ErrorType         `json:"type"`
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Operation *EntityOperation  `json:"operation,omitempty"`
	Entity    *EntityIdentifier `json:"entity,omitempty"`
	Field     string            `json:"field,omitempty"`
	Details   map[string]any    `json:"details,omitempty"`
	Cause     error             `json:"-"`
}

func (e *FormaError) Error() string {
	if e.Entity != nil {
		return fmt.Sprintf("[%s:%s] entity %s/%s: %s",
			e.Type, e.Code, e.Entity.SchemaName, e.Entity.RowID, e.Message)
	}
	if e.Operation != nil {
		return fmt.Sprintf("[%s:%s] operation %s on %s: %s",
			e.Type, e.Code, e.Operation.Type, e.Operation.SchemaName, e.Message)
	}
	if e.Field != "" {
		return fmt.Sprintf("[%s:%s] field '%s': %s", e.Type, e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Type, e.Code, e.Message)
}

func (e *FormaError) Unwrap() error {
	return e.Cause
}

// WithDetails adds details to a DataPlaneError
func (e *FormaError) WithDetails(details map[string]any) *FormaError {
	if e.Details == nil {
		e.Details = make(map[string]any)
	}
	for k, v := range details {
		e.Details[k] = v
	}
	return e
}

// WithDetail adds a single detail to a DataPlaneError
func (e *FormaError) WithDetail(key string, value any) *FormaError {
	if e.Details == nil {
		e.Details = make(map[string]any)
	}
	e.Details[key] = value
	return e
}

// WithCause adds a cause to a DataPlaneError
func (e *FormaError) WithCause(cause error) *FormaError {
	e.Cause = cause
	return e
}

// WithEntity adds entity context to a DataPlaneError
func (e *FormaError) WithEntity(entity EntityIdentifier) *FormaError {
	e.Entity = &entity
	return e
}

// WithOperation adds operation context to a DataPlaneError
func (e *FormaError) WithOperation(operation any) *FormaError {
	// Convert different operation types to the core EntityOperation type
	switch op := operation.(type) {
	case EntityOperation:
		e.Operation = &op
	case *EntityOperation:
		e.Operation = op
	default:
		// For other types, create a basic operation representation
		e.Operation = &EntityOperation{
			Type: "unknown",
		}
	}
	return e
}

// WithField adds field context to a DataPlaneError
func (e *FormaError) WithField(field string) *FormaError {
	e.Field = field
	return e
}

// Error codes consolidated from all modules
const (
	// Entity operation errors (from pkg/dataaccess)
	ErrCodeEntityNotFound       = "ENTITY_NOT_FOUND"
	ErrCodeEntityAlreadyExists  = "ENTITY_ALREADY_EXISTS"
	ErrCodeValidationFailed     = "VALIDATION_FAILED"
	ErrCodeReferenceNotFound    = "REFERENCE_NOT_FOUND"
	ErrCodeReferenceIntegrity   = "REFERENCE_INTEGRITY_VIOLATION"
	ErrCodeReferenceValidation  = "REFERENCE_VALIDATION_FAILED"
	ErrCodeCascadeNotEnabled    = "CASCADE_NOT_ENABLED"
	ErrCodeCascadeDepthExceeded = "CASCADE_DEPTH_EXCEEDED"
	ErrCodeTransactionFailed    = "TRANSACTION_FAILED"
	ErrCodeQueryFailed          = "QUERY_FAILED"
	ErrCodeQueryBuildFailed     = "QUERY_BUILD_FAILED"
	ErrCodeInternalError        = "INTERNAL_ERROR"

	// Query operation errors (from internal/dataplane)
	ErrCodeInvalidPage             = "INVALID_PAGE"
	ErrCodeInvalidPageSize         = "INVALID_PAGE_SIZE"
	ErrCodeInvalidClientId         = "INVALID_CLIENT_ID"
	ErrCodeInvalidProjectID        = "INVALID_PROJECT_ID"
	ErrCodeInvalidSchemaName       = "INVALID_SCHEMA_NAME"
	ErrCodeInvalidSchemaID         = "INVALID_SCHEMA_ID"
	ErrCodeInvalidFilter           = "INVALID_FILTER"
	ErrCodeUnsupportedDialect      = "UNSUPPORTED_DIALECT"
	ErrCodeQueryExecution          = "QUERY_EXECUTION_ERROR"
	ErrCodeQueryTimeout            = "QUERY_TIMEOUT"
	ErrCodeUnauthorizedAccess      = "UNAUTHORIZED_ACCESS"
	ErrCodeNoRecordsFound          = "NO_RECORDS_FOUND"
	ErrCodeResultAggregation       = "RESULT_AGGREGATION_ERROR"
	ErrCodeInvalidConnectionString = "INVALID_CONNECTION_STRING"
	ErrCodeInvalidDriver           = "INVALID_DRIVER"
	ErrCodeInvalidDB               = "INVALID_DB"
	ErrCodeUnknownDialect          = "UNKNOWN_DIALECT"
	ErrCodeDialectDetectionFailed  = "DIALECT_DETECTION_FAILED"
	ErrCodeInvalidDialect          = "INVALID_DIALECT"
	ErrCodeInvalidGenerator        = "INVALID_GENERATOR"

	// PostgreSQL-only specific error codes
	ErrCodeNonPostgreSQLDatabase = "NON_POSTGRESQL_DATABASE"
	ErrCodeMySQLNotSupported     = "MYSQL_NOT_SUPPORTED"
	ErrCodeSQLiteNotSupported    = "SQLITE_NOT_SUPPORTED"

	// Batch operation errors
	ErrCodeBatchOperationFailed = "BATCH_OPERATION_FAILED"
	ErrCodeBatchSizeExceeded    = "BATCH_SIZE_EXCEEDED"
	ErrCodeBatchTimeout         = "BATCH_TIMEOUT"

	// Cache errors
	ErrCodeCacheError     = "CACHE_ERROR"
	ErrCodeCacheMiss      = "CACHE_MISS"
	ErrCodeCacheCorrupted = "CACHE_CORRUPTED"

	// Connection errors
	ErrCodeConnectionFailed = "CONNECTION_FAILED"
	ErrCodeConnectionLost   = "CONNECTION_LOST"
	ErrCodePoolExhausted    = "POOL_EXHAUSTED"

	// Schema errors
	ErrCodeSchemaNotFound    = "SCHEMA_NOT_FOUND"
	ErrCodeSchemaInvalid     = "SCHEMA_INVALID"
	ErrCodeSchemaUnavailable = "SCHEMA_UNAVAILABLE"

	// Transformer errors
	ErrCodeTransformerSchemaNotFound = "SCHEMA_NOT_FOUND"
	ErrCodeInvalidJSON               = "INVALID_JSON"
	ErrCodeTypeMismatch              = "TYPE_MISMATCH"
	ErrCodeRequiredFieldMissing      = "REQUIRED_FIELD_MISSING"
	ErrCodeConversionFailed          = "CONVERSION_FAILED"
	ErrCodeInvalidFormat             = "INVALID_FORMAT"
	ErrCodeNewRowIDFailed            = "NEW_ROW_ID_FAILED"
)

// ============================================================================
// FormaError Constructors
// ============================================================================

// NewFormaError creates a new DataPlaneError
func NewFormaError(errorType ErrorType, code, message string) *FormaError {
	return &FormaError{
		Type:    errorType,
		Code:    code,
		Message: message,
		Details: make(map[string]any),
	}
}

// Entity-specific error constructors

// NewEntityNotFoundError creates an entity not found error
func NewEntityNotFoundError(entity EntityIdentifier) *FormaError {
	return &FormaError{
		Type:    ErrorTypeNotFound,
		Code:    ErrCodeEntityNotFound,
		Message: "entity not found",
		Entity:  &entity,
		Details: make(map[string]any),
	}
}

// NewEntityAlreadyExistsError creates an entity already exists error
func NewEntityAlreadyExistsError(entity EntityIdentifier) *FormaError {
	return &FormaError{
		Type:    ErrorTypeValidation,
		Code:    ErrCodeEntityAlreadyExists,
		Message: "entity already exists",
		Entity:  &entity,
		Details: make(map[string]any),
	}
}

// NewValidationError creates a validation error
func NewValidationError(field, message string) *FormaError {
	return &FormaError{
		Type:    ErrorTypeValidation,
		Code:    ErrCodeValidationFailed,
		Message: message,
		Field:   field,
		Details: make(map[string]any),
	}
}

// NewReferenceError creates a reference error
func NewReferenceError(field, message string) *FormaError {
	return &FormaError{
		Type:    ErrorTypeReference,
		Code:    ErrCodeReferenceNotFound,
		Message: message,
		Field:   field,
		Details: make(map[string]any),
	}
}

// NewReferenceIntegrityError creates a reference integrity error
func NewReferenceIntegrityError(message string) *FormaError {
	return &FormaError{
		Type:    ErrorTypeReference,
		Code:    ErrCodeReferenceIntegrity,
		Message: message,
		Details: make(map[string]any),
	}
}

// NewTransactionError creates a transaction error
func NewTransactionError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeTransaction,
		Code:    ErrCodeTransactionFailed,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// Query-specific error constructors

// NewQueryError creates a query error
func NewQueryError(errorType ErrorType, code, message string) *FormaError {
	return &FormaError{
		Type:    errorType,
		Code:    code,
		Message: message,
		Details: make(map[string]any),
	}
}

// NewQueryExecutionError creates a query execution error
func NewQueryExecutionError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeExecution,
		Code:    ErrCodeQueryExecution,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// NewQueryTimeoutError creates a query timeout error
func NewQueryTimeoutError(message string) *FormaError {
	return &FormaError{
		Type:    ErrorTypeTimeout,
		Code:    ErrCodeQueryTimeout,
		Message: message,
		Details: make(map[string]any),
	}
}

// NewInternalError creates an internal error
func NewInternalError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeInternal,
		Code:    ErrCodeInternalError,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// NewMySQLNotSupportedError creates a standardized error for MySQL usage attempts
func NewMySQLNotSupportedError() *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeMySQLNotSupported,
		"MySQL is not supported. This system only supports PostgreSQL databases. Please use AWS DSQL or a PostgreSQL-compatible database.")
}

// NewSQLiteNotSupportedError creates a standardized error for SQLite usage attempts
func NewSQLiteNotSupportedError() *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeSQLiteNotSupported,
		"SQLite is not supported. This system only supports PostgreSQL databases. Please use AWS DSQL or a PostgreSQL-compatible database.")
}

// NewUnsupportedDialectError creates a standardized error for unsupported dialects
func NewUnsupportedDialectError(dialect string) *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeUnsupportedDialect,
		"Unsupported database dialect: "+dialect+". This system only supports PostgreSQL databases. Please use AWS DSQL or a PostgreSQL-compatible database.")
}

// NewNonPostgreSQLDatabaseError creates a standardized error for non-PostgreSQL database detection
func NewNonPostgreSQLDatabaseError() *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeNonPostgreSQLDatabase,
		"Non-PostgreSQL database detected. This system only supports PostgreSQL databases. Please use AWS DSQL or a PostgreSQL-compatible database.")
}

// NewPostgreSQLOnlyConnectionError creates a standardized error for connection string issues
func NewPostgreSQLOnlyConnectionError() *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeInvalidConnectionString,
		"Invalid connection string. This system only supports PostgreSQL connection strings. Please use a PostgreSQL or AWS DSQL connection string.")
}

// NewPostgreSQLOnlyDriverError creates a standardized error for driver issues
func NewPostgreSQLOnlyDriverError(driverName string) *FormaError {
	return NewQueryError(ErrorTypeValidation, ErrCodeInvalidDriver,
		"Unsupported database driver: "+driverName+". This system only supports PostgreSQL drivers (pq, pgx). Please use a PostgreSQL-compatible driver for AWS DSQL.")
}

// Batch operation error constructors

// NewBatchOperationError creates a batch operation error
func NewBatchOperationError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeExecution,
		Code:    ErrCodeBatchOperationFailed,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// NewBatchSizeExceededError creates a batch size exceeded error
func NewBatchSizeExceededError(size, maxSize int) *FormaError {
	return &FormaError{
		Type:    ErrorTypeValidation,
		Code:    ErrCodeBatchSizeExceeded,
		Message: fmt.Sprintf("batch size %d exceeds maximum allowed size %d", size, maxSize),
		Details: map[string]any{
			"size":     size,
			"max_size": maxSize,
		},
	}
}

// Cache error constructors

// NewCacheError creates a cache error
func NewCacheError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeInternal,
		Code:    ErrCodeCacheError,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// Connection error constructors

// NewConnectionError creates a connection error
func NewConnectionError(message string, cause error) *FormaError {
	return &FormaError{
		Type:    ErrorTypeInternal,
		Code:    ErrCodeConnectionFailed,
		Message: message,
		Cause:   cause,
		Details: make(map[string]any),
	}
}

// NewPoolExhaustedError creates a pool exhausted error
func NewPoolExhaustedError() *FormaError {
	return &FormaError{
		Type:    ErrorTypeInternal,
		Code:    ErrCodePoolExhausted,
		Message: "connection pool exhausted",
		Details: make(map[string]any),
	}
}

// Schema error constructors

// NewSchemaNotFoundError creates a schema not found error
func NewSchemaNotFoundError(schemaName string) *FormaError {
	return &FormaError{
		Type:    ErrorTypeNotFound,
		Code:    ErrCodeSchemaNotFound,
		Message: fmt.Sprintf("schema '%s' not found", schemaName),
		Details: map[string]any{
			"schema_name": schemaName,
		},
	}
}

// ============================================================================
// QueryError Type and Constructors
// ============================================================================

// QueryError represents query-related errors
type QueryError struct {
	Type    ErrorType      `json:"type"`
	Message string         `json:"message"`
	Code    string         `json:"code"`
	Details map[string]any `json:"details,omitempty"`
}

// Error implements the error interface
func (e *QueryError) Error() string {
	return e.Message
}

// WithDetails adds details to a QueryError
func (e *QueryError) WithDetails(key string, value any) *QueryError {
	if e.Details == nil {
		e.Details = make(map[string]any)
	}
	e.Details[key] = value
	return e
}

// ============================================================================
// DataPlaneError Type and Constructors
// ============================================================================

// DataPlaneError consolidates error types from both modules
type DataPlaneError struct {
	Type      ErrorType        `json:"type"`
	Code      string           `json:"code"`
	Message   string           `json:"message"`
	Operation *EntityOperation `json:"operation,omitempty"`
	Field     string           `json:"field,omitempty"`
	Details   map[string]any   `json:"details,omitempty"`
	Cause     error            `json:"-"`
}

func (e *DataPlaneError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// WithCause adds a cause to the error
func (e *DataPlaneError) WithCause(cause error) *DataPlaneError {
	e.Cause = cause
	return e
}

// WithField adds a field to the error
func (e *DataPlaneError) WithField(field string) *DataPlaneError {
	e.Field = field
	return e
}

// WithDetails adds details to the error
func (e *DataPlaneError) WithDetails(details map[string]any) *DataPlaneError {
	e.Details = details
	return e
}

// WithOperation adds an operation to the error
func (e *DataPlaneError) WithOperation(op *EntityOperation) *DataPlaneError {
	e.Operation = op
	return e
}

// NewDataPlaneError creates a new unified error
func NewDataPlaneError(errorType ErrorType, code, message string) *DataPlaneError {
	return &DataPlaneError{
		Type:    errorType,
		Code:    code,
		Message: message,
	}
}

// ============================================================================
// TransformerError Type and Constructors
// ============================================================================

// TransformerError represents a structured error with context information
type TransformerError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
	Cause   error  `json:"cause,omitempty"`
}

// Error implements the error interface
func (e *TransformerError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("[%s] %s (field: %s)", e.Code, e.Message, e.Field)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause error
func (e *TransformerError) Unwrap() error {
	return e.Cause
}

// NewTransformerError creates a new TransformerError
func NewTransformerError(code, message string) *TransformerError {
	return &TransformerError{
		Code:    code,
		Message: message,
	}
}

// NewTransformerErrorWithField creates a new TransformerError with field context
func NewTransformerErrorWithField(code, message, field string) *TransformerError {
	return &TransformerError{
		Code:    code,
		Message: message,
		Field:   field,
	}
}

// NewTransformerErrorWithCause creates a new TransformerError with an underlying cause
func NewTransformerErrorWithCause(code, message string, cause error) *TransformerError {
	return &TransformerError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// ============================================================================
// ValidationErrors Type and Constructors
// ============================================================================

// ValidationErrors represents multiple validation errors
type ValidationErrors struct {
	Errors []*TransformerError `json:"errors"`
}

// Error implements the error interface for ValidationErrors
func (ve *ValidationErrors) Error() string {
	if len(ve.Errors) == 0 {
		return "no validation errors"
	}
	if len(ve.Errors) == 1 {
		return ve.Errors[0].Error()
	}
	return fmt.Sprintf("multiple validation errors: %d errors found", len(ve.Errors))
}

// Add adds a new error to the collection
func (ve *ValidationErrors) Add(err *TransformerError) {
	ve.Errors = append(ve.Errors, err)
}

// HasErrors returns true if there are any errors
func (ve *ValidationErrors) HasErrors() bool {
	return len(ve.Errors) > 0
}

// ToError returns the ValidationErrors as an error if there are any errors, nil otherwise
func (ve *ValidationErrors) ToError() error {
	if ve.HasErrors() {
		return ve
	}
	return nil
}

// NewValidationErrors creates a new ValidationErrors instance
func NewValidationErrors() *ValidationErrors {
	return &ValidationErrors{
		Errors: make([]*TransformerError, 0),
	}
}

// ============================================================================
// BatchErrors Type and Constructors
// ============================================================================

// BatchErrors represents errors from batch operations with statistics
type BatchErrors struct {
	Errors       []*TransformerError `json:"errors"`
	SuccessCount int                 `json:"success_count"`
	FailureCount int                 `json:"failure_count"`
	TotalCount   int                 `json:"total_count"`
}

// Error implements the error interface for BatchErrors
func (be *BatchErrors) Error() string {
	if len(be.Errors) == 0 {
		return "no batch errors"
	}
	if len(be.Errors) == 1 {
		return fmt.Sprintf("batch operation failed: %s (success: %d/%d)",
			be.Errors[0].Error(), be.SuccessCount, be.TotalCount)
	}
	return fmt.Sprintf("batch operation failed: %d errors found (success: %d/%d, failures: %d/%d)",
		len(be.Errors), be.SuccessCount, be.TotalCount, be.FailureCount, be.TotalCount)
}

// Add adds a new error to the batch error collection
func (be *BatchErrors) Add(err *TransformerError) {
	be.Errors = append(be.Errors, err)
}

// HasErrors returns true if there are any errors
func (be *BatchErrors) HasErrors() bool {
	return len(be.Errors) > 0
}

// SetStatistics sets the batch operation statistics
func (be *BatchErrors) SetStatistics(successCount, failureCount, totalCount int) {
	be.SuccessCount = successCount
	be.FailureCount = failureCount
	be.TotalCount = totalCount
}

// GetSuccessRate returns the success rate as a percentage
func (be *BatchErrors) GetSuccessRate() float64 {
	if be.TotalCount == 0 {
		return 0.0
	}
	return float64(be.SuccessCount) / float64(be.TotalCount) * 100.0
}

// NewBatchErrors creates a new BatchErrors instance
func NewBatchErrors() *BatchErrors {
	return &BatchErrors{
		Errors: make([]*TransformerError, 0),
	}
}

// GetErrorsByType returns errors grouped by error code
func (be *BatchErrors) GetErrorsByType() map[string][]*TransformerError {
	errorsByType := make(map[string][]*TransformerError)
	for _, err := range be.Errors {
		errorsByType[err.Code] = append(errorsByType[err.Code], err)
	}
	return errorsByType
}

// GetErrorSummary returns a summary of errors by type with counts
func (be *BatchErrors) GetErrorSummary() map[string]int {
	summary := make(map[string]int)
	for _, err := range be.Errors {
		summary[err.Code]++
	}
	return summary
}

// HasPartialSuccess returns true if some objects succeeded and some failed
func (be *BatchErrors) HasPartialSuccess() bool {
	return be.SuccessCount > 0 && be.FailureCount > 0
}

// IsCompleteFailure returns true if all objects failed
func (be *BatchErrors) IsCompleteFailure() bool {
	return be.SuccessCount == 0 && be.TotalCount > 0
}

// IsCompleteSuccess returns true if all objects succeeded
func (be *BatchErrors) IsCompleteSuccess() bool {
	return be.FailureCount == 0 && be.TotalCount > 0
}

// GetDetailedReport returns a detailed error report for logging/debugging
func (be *BatchErrors) GetDetailedReport() string {
	if !be.HasErrors() {
		return fmt.Sprintf("Batch operation completed successfully: %d/%d objects processed",
			be.SuccessCount, be.TotalCount)
	}

	report := fmt.Sprintf("Batch operation completed with errors: %d/%d succeeded, %d/%d failed\n",
		be.SuccessCount, be.TotalCount, be.FailureCount, be.TotalCount)

	// Add error summary
	errorSummary := be.GetErrorSummary()
	report += "Error Summary:\n"
	for errorCode, count := range errorSummary {
		report += fmt.Sprintf("  %s: %d errors\n", errorCode, count)
	}

	// Add individual errors (limited to first 10 for readability)
	report += "Individual Errors:\n"
	maxErrors := 10
	for i, err := range be.Errors {
		if i >= maxErrors {
			report += fmt.Sprintf("  ... and %d more errors\n", len(be.Errors)-maxErrors)
			break
		}
		report += fmt.Sprintf("  %d. %s\n", i+1, err.Error())
	}

	return report
}

// ============================================================================
// SchemaError Type and Constructors
// ============================================================================

// SchemaErrorType represents the type of schema error
type SchemaErrorType string

const (
	SchemaErrorTypeNotFound      SchemaErrorType = "schema_not_found"
	SchemaErrorTypeInvalidFormat SchemaErrorType = "invalid_format"
	SchemaErrorTypeProviderError SchemaErrorType = "provider_error"
	SchemaErrorTypeCacheError    SchemaErrorType = "cache_error"
	SchemaErrorTypeConfigError   SchemaErrorType = "config_error"
	SchemaErrorTypeTimeout       SchemaErrorType = "timeout"
)

// SchemaError represents schema-related errors
type SchemaError struct {
	Type    SchemaErrorType `json:"type"`
	Message string          `json:"message"`
	Cause   error           `json:"-"`
}

// Error implements the error interface
func (e *SchemaError) Error() string {
	return fmt.Sprintf("schema error [%s]: %s", e.Type, e.Message)
}

// Unwrap returns the underlying error
func (e *SchemaError) Unwrap() error {
	return e.Cause
}

// NewSchemaError creates a new SchemaError
func NewSchemaError(errorType SchemaErrorType, message string, cause error) *SchemaError {
	return &SchemaError{
		Type:    errorType,
		Message: message,
		Cause:   cause,
	}
}

// IsSchemaError checks if an error is a SchemaError of a specific type
func IsSchemaError(err error, errorType SchemaErrorType) bool {
	if schemaErr, ok := err.(*SchemaError); ok {
		return schemaErr.Type == errorType
	}
	return false
}

// ============================================================================
// Error checking utilities
// ============================================================================

// IsEntityNotFoundError checks if an error is an entity not found error
func IsEntityNotFoundError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Code == ErrCodeEntityNotFound
	}
	return false
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Type == ErrorTypeValidation
	}
	return false
}

// IsTransactionError checks if an error is a transaction error
func IsTransactionError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Type == ErrorTypeTransaction
	}
	return false
}

// IsReferenceError checks if an error is a reference error
func IsReferenceError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Type == ErrorTypeReference
	}
	return false
}

// IsTimeoutError checks if an error is a timeout error
func IsTimeoutError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Type == ErrorTypeTimeout
	}
	return false
}

// IsInternalError checks if an error is an internal error
func IsInternalError(err error) bool {
	if dpe, ok := err.(*FormaError); ok {
		return dpe.Type == ErrorTypeInternal
	}
	return false
}
