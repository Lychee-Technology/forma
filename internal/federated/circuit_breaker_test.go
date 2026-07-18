package federated

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreakerWiring(t *testing.T) {
	// The breaker is injected into DBFederatedQueryEngine at construction; this
	// verifies the basic record/inspect cycle on a directly constructed breaker.
	breaker := NewCircuitBreaker(2, 10*time.Second, 5*time.Second)

	breaker.RecordFailure()
	breaker.RecordSuccess()

	if breaker.IsOpen() {
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

func TestCircuitBreakerAllowClosedAdmitsAll(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Second, 5*time.Second)
	for i := 0; i < 2; i++ {
		if !cb.Allow() {
			t.Fatalf("closed breaker must admit caller %d without reserving a probe", i+1)
		}
	}
	cb.RecordFailure() // below threshold: still closed
	if !cb.Allow() {
		t.Fatal("breaker below threshold must remain closed")
	}
}

func TestCircuitBreakerAllowNilSafety(t *testing.T) {
	var nilBreaker *CircuitBreaker
	if !nilBreaker.Allow() {
		t.Fatal("nil breaker must admit everything")
	}
}

func TestCircuitBreakerAllowRejectsWhileOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 10*time.Second)
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("open breaker must reject before openDuration elapses")
	}
}

func TestCircuitBreakerHalfOpenAdmitsExactlyOneProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)

	var admitted int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cb.Allow() {
				atomic.AddInt32(&admitted, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&admitted); got != 1 {
		t.Fatalf("half-open breaker must admit exactly 1 probe, admitted %d", got)
	}
}

func TestCircuitBreakerProbeSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("first caller after openDuration must be admitted as the probe")
	}
	if cb.Allow() {
		t.Fatal("second caller must be rejected while the probe is in flight")
	}
	cb.RecordSuccess()
	for i := 0; i < 2; i++ {
		if !cb.Allow() {
			t.Fatalf("probe success must close the breaker for all callers, caller %d rejected", i+1)
		}
	}
}

func TestCircuitBreakerProbeFailureReopensWithoutThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("breaker must be open at threshold")
	}
	time.Sleep(100 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("probe must be admitted after openDuration")
	}
	cb.RecordFailure() // ONE probe failure — far below the threshold of 3
	if cb.Allow() {
		t.Fatal("probe failure must re-open immediately, without threshold re-accumulation")
	}
	time.Sleep(100 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("a fresh probe must be admitted after the renewed openDuration")
	}
}

func TestCircuitBreakerAbandonedProbeReclaimed(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("probe must be admitted after openDuration")
	}
	if cb.Allow() {
		t.Fatal("probe slot must be exclusive while in flight")
	}
	// The probe never reports (cancelled query). The reservation lapses
	// openDuration after admission and the slot becomes reclaimable.
	time.Sleep(100 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("abandoned probe must be reclaimable after openDuration")
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
