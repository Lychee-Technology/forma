package internal

import (
	"sync"
	"time"
)

// CircuitBreaker is a lightweight in-memory circuit breaker.
type CircuitBreaker struct {
	mu           sync.Mutex
	failures     []time.Time
	threshold    int
	window       time.Duration
	openUntil    time.Time
	openDuration time.Duration
}

// NewCircuitBreaker creates a configured circuit breaker.
func NewCircuitBreaker(threshold int, window, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		window:       window,
		openDuration: openDuration,
		failures:     make([]time.Time, 0, threshold),
	}
}

// RecordFailure records a failure occurrence and opens the breaker if threshold exceeded.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	// drop old failures outside the window
	cutoff := now.Add(-cb.window)
	i := 0
	for ; i < len(cb.failures); i++ {
		if cb.failures[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		// remove first i entries
		cb.failures = append([]time.Time{}, cb.failures[i:]...)
	}
	// append this failure
	cb.failures = append(cb.failures, now)

	if len(cb.failures) >= cb.threshold {
		// open the breaker
		cb.openUntil = now.Add(cb.openDuration)
	}
}

// RecordSuccess resets failure history when operations succeed.
//
// Design note: this breaker forgives immediately on the first success after
// the open period expires; it does not implement a half-open probe state.
// That favors fast recovery when the downstream is healthy again, but it also
// means a single success clears prior failures even if the dependency is still
// unstable. If stricter half-open semantics are needed, extend this logic with
// a probe counter guarded by mu.
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = cb.failures[:0]
	cb.openUntil = time.Time{}
}

// IsOpen returns true if the breaker is currently open.
func (cb *CircuitBreaker) IsOpen() bool {
	if cb == nil {
		return false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return time.Now().Before(cb.openUntil)
}
