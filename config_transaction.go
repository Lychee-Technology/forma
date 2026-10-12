package forma

import "time"

// TransactionConfig contains transaction settings.
//
// Only DefaultTimeout has an effect. Every other field is declared, parsed and
// defaulted, but nothing reads it. The fields that would imply Forma retries a
// transaction, chooses its isolation level, or detects deadlocks itself are
// marked deprecated (#645). Each one still parses, so an existing config file
// keeps loading.
type TransactionConfig struct {
	// DefaultTimeout bounds each write transaction: Create, Update, Delete,
	// and each atomic batch. A best-effort batch is bounded per operation
	// (#465). Zero means unbounded.
	DefaultTimeout time.Duration `json:"defaultTimeout"`
	MaxTimeout     time.Duration `json:"maxTimeout"`
	// Deprecated: has no effect. Forma never retries a transaction;
	// retrying is the caller's decision. Retry only when forma.IsRetryable
	// reports true (#645).
	MaxRetryAttempts int `json:"maxRetryAttempts"`
	// Deprecated: has no effect, for the reason given on MaxRetryAttempts.
	RetryAttempts int `json:"retryAttempts"`
	// Deprecated: has no effect, for the reason given on MaxRetryAttempts.
	RetryDelay time.Duration `json:"retryDelay"`
	// Deprecated: has no effect. The isolation level is not configurable.
	// Update, the atomic batch update and RequeueForFlush pin READ COMMITTED
	// because their correctness depends on it (#457); every other write runs
	// at the database's default level.
	IsolationLevel string `json:"isolationLevel"`
	// Deprecated: has no effect. PostgreSQL always detects deadlocks, and
	// Forma reports one as forma.ErrRetryableConflict (#645).
	EnableDeadlockDetection bool `json:"enableDeadlockDetection"`
	// Deprecated: has no effect, for the reason given on
	// EnableDeadlockDetection. PostgreSQL's deadlock_timeout governs the check.
	DeadlockCheckInterval time.Duration `json:"deadlockCheckInterval"`
	// Deprecated: has no effect, for the reason given on
	// EnableDeadlockDetection. Bound lock waits with the server's
	// lock_timeout; an expired wait is forma.ErrRetryableConflict.
	DeadlockMaxWaitTime      time.Duration `json:"deadlockMaxWaitTime"`
	SlowTransactionThreshold time.Duration `json:"slowTransactionThreshold"`
	MinSuccessRate           float64       `json:"minSuccessRate"`
	MaxAverageDuration       time.Duration `json:"maxAverageDuration"`
	MaxConnectionPoolUsage   float64       `json:"maxConnectionPoolUsage"`
}

// defaultTransactionConfig returns default transaction configuration. The
// deprecated fields keep their historical defaults, so a config that prints
// or compares them is unchanged.
func defaultTransactionConfig() TransactionConfig {
	return TransactionConfig{
		DefaultTimeout:           30 * time.Second,
		MaxTimeout:               5 * time.Minute,
		MaxRetryAttempts:         3,
		RetryAttempts:            3,
		RetryDelay:               100 * time.Millisecond,
		IsolationLevel:           "READ_COMMITTED",
		EnableDeadlockDetection:  true,
		DeadlockCheckInterval:    5 * time.Second,
		DeadlockMaxWaitTime:      30 * time.Second,
		SlowTransactionThreshold: 2 * time.Second,
		MinSuccessRate:           95.0,
		MaxAverageDuration:       1 * time.Second,
		MaxConnectionPoolUsage:   80.0,
	}
}
