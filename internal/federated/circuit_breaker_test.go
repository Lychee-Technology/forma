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
		if !allowed(cb) {
			t.Fatalf("closed breaker must admit caller %d without reserving a probe", i+1)
		}
	}
	cb.RecordFailure() // below threshold: still closed
	if !allowed(cb) {
		t.Fatal("breaker below threshold must remain closed")
	}
}

func TestCircuitBreakerAllowNilSafety(t *testing.T) {
	var nilBreaker *CircuitBreaker
	if !allowed(nilBreaker) {
		t.Fatal("nil breaker must admit everything")
	}
}

func TestCircuitBreakerAllowRejectsWhileOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 10*time.Second)
	cb.RecordFailure()
	if allowed(cb) {
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
			if allowed(cb) {
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

	if !allowed(cb) {
		t.Fatal("first caller after openDuration must be admitted as the probe")
	}
	if allowed(cb) {
		t.Fatal("second caller must be rejected while the probe is in flight")
	}
	cb.RecordSuccess()
	for i := 0; i < 2; i++ {
		if !allowed(cb) {
			t.Fatalf("probe success must close the breaker for all callers, caller %d rejected", i+1)
		}
	}
}

func TestCircuitBreakerProbeFailureReopensWithoutThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if allowed(cb) {
		t.Fatal("breaker must be open at threshold")
	}
	time.Sleep(100 * time.Millisecond)

	if !allowed(cb) {
		t.Fatal("probe must be admitted after openDuration")
	}
	cb.RecordFailure() // ONE probe failure — far below the threshold of 3
	if allowed(cb) {
		t.Fatal("probe failure must re-open immediately, without threshold re-accumulation")
	}
	time.Sleep(100 * time.Millisecond)
	if !allowed(cb) {
		t.Fatal("a fresh probe must be admitted after the renewed openDuration")
	}
}

func TestCircuitBreakerAbandonedProbeReclaimed(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(100 * time.Millisecond)

	if !allowed(cb) {
		t.Fatal("probe must be admitted after openDuration")
	}
	if allowed(cb) {
		t.Fatal("probe slot must be exclusive while in flight")
	}
	// The probe never reports (cancelled query). The reservation lapses
	// openDuration after admission and the slot becomes reclaimable.
	time.Sleep(100 * time.Millisecond)
	if !allowed(cb) {
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

// allowed adapts Allow's (admitted, token) return for the admission-only
// assertions above.
func allowed(cb *CircuitBreaker) bool {
	ok, _ := cb.Allow()
	return ok
}

// TestCircuitBreakerReleaseProbeScopedToOwner pins the #349 review R2-2
// contract: ReleaseProbe frees the slot only for the reservation that Allow
// handed out. A stale caller — admitted while the breaker was closed (zero
// token) or guessing a token it never held — cannot clear the probe a newer
// caller is running, so a third caller is never admitted alongside the real
// probe.
func TestCircuitBreakerReleaseProbeScopedToOwner(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Minute, 40*time.Millisecond)

	// A closed-at-admission caller finishing late releases the zero token:
	// always a no-op, at any breaker state.
	cb.ReleaseProbe(0)

	cb.RecordFailure()                // threshold 1: opens
	time.Sleep(45 * time.Millisecond) // open period lapses → half-open

	admitted, probeB := cb.Allow()
	if !admitted || probeB == 0 {
		t.Fatalf("half-open must admit the probe with a token, got admitted=%v token=%d", admitted, probeB)
	}
	// Stale releases while B's probe is live: zero token and a foreign token.
	cb.ReleaseProbe(0)
	cb.ReleaseProbe(probeB + 100)
	if ok, _ := cb.Allow(); ok {
		t.Fatal("stale releases must not clear a live probe; a second probe was admitted")
	}
	// The owner's release frees the slot for the next probe.
	cb.ReleaseProbe(probeB)
	admitted, probeC := cb.Allow()
	if !admitted || probeC == 0 || probeC == probeB {
		t.Fatalf("owner release must free the slot for a fresh reservation, got admitted=%v token=%d (prev %d)", admitted, probeC, probeB)
	}
}

// TestCircuitBreakerLapsedReservationReleaseCannotClearNewProbe covers the
// exact R2-2 interleaving: a reservation lapses, a newer caller reclaims the
// slot, and the old caller's late ReleaseProbe must not free the new probe.
func TestCircuitBreakerLapsedReservationReleaseCannotClearNewProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Minute, 30*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(35 * time.Millisecond)

	_, probeOld := cb.Allow()
	if probeOld == 0 {
		t.Fatal("expected the first half-open caller to reserve the probe")
	}
	time.Sleep(35 * time.Millisecond) // probeOld's reservation lapses

	admitted, probeNew := cb.Allow() // reclaims the slot as the new probe
	if !admitted || probeNew == 0 || probeNew == probeOld {
		t.Fatalf("lapsed slot must be reclaimed with a fresh token, got admitted=%v token=%d (old %d)", admitted, probeNew, probeOld)
	}
	cb.ReleaseProbe(probeOld) // the lapsed caller finally returns
	if ok, _ := cb.Allow(); ok {
		t.Fatal("lapsed caller's release must not clear the reclaimed probe")
	}
}
