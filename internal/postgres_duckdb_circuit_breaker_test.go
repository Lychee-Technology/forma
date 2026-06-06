package internal

import (
	"sync"
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

func TestCircuitBreakerTripsOnThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second, 5*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.IsOpen() {
		t.Fatal("breaker must not open before threshold is reached")
	}

	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("breaker must open once threshold is reached")
	}
}

func TestCircuitBreakerWindowExpiry(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond, 5*time.Second)

	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)
	cb.RecordFailure()

	if cb.IsOpen() {
		t.Fatal("breaker must not open when prior failure has expired from the window")
	}
}

func TestCircuitBreakerAutoRecoveryAfterOpenDuration(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)

	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected breaker open after failure threshold")
	}

	time.Sleep(100 * time.Millisecond)
	if cb.IsOpen() {
		t.Fatal("breaker must auto-close after open duration expires")
	}
}

func TestCircuitBreakerRecordSuccessClosesBreakerImmediately(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 10*time.Second)

	cb.RecordFailure()
	if !cb.IsOpen() {
		t.Fatal("expected breaker open after first failure")
	}

	cb.RecordSuccess()
	if cb.IsOpen() {
		t.Fatal("RecordSuccess must close the breaker immediately")
	}
}

func TestCircuitBreakerConcurrentSafety(t *testing.T) {
	cb := NewCircuitBreaker(10, 10*time.Second, 1*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			switch n % 3 {
			case 0:
				cb.RecordSuccess()
			case 1:
				cb.RecordFailure()
			default:
				_ = cb.IsOpen()
			}
		}(i)
	}
	wg.Wait()
}
