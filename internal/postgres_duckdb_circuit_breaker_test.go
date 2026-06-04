package internal

import (
	"testing"
	"time"
)

func TestCircuitBreakerWiring(t *testing.T) {
	// This test verifies that the circuit breaker integration compiles and the
	// functions GetDuckDBCircuitBreaker, RecordFailure, and RecordSuccess are
	// available and can be called without panicking.
	
	// Setup circuit breaker
	breaker := NewCircuitBreaker(2, 10*time.Second, 5*time.Second)
	SetGlobalDuckDBCircuitBreaker(breaker)
	defer SetGlobalDuckDBCircuitBreaker(nil)

	// Verify we can retrieve the breaker
	retrieved := GetDuckDBCircuitBreaker()
	if retrieved == nil {
		t.Fatal("expected non-nil circuit breaker after SetGlobalDuckDBCircuitBreaker")
	}

	// Verify RecordFailure doesn't panic
	retrieved.RecordFailure()

	// Verify RecordSuccess doesn't panic
	retrieved.RecordSuccess()

	// Verify IsOpen works
	if retrieved.IsOpen() {
		t.Error("circuit breaker should not be open after single failure and success")
	}
}

func TestCircuitBreakerNilSafety(t *testing.T) {
	// Verify that circuit breaker methods are nil-safe
	var nilBreaker *CircuitBreaker

	// These should not panic
	nilBreaker.RecordFailure()
	nilBreaker.RecordSuccess()
	
	if nilBreaker.IsOpen() {
		t.Error("nil breaker should return false for IsOpen")
	}
}
