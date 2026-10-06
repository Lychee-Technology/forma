package federated

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// elapseOpenPeriod ends the breaker's open period without sleeping, as if
// openDuration had passed.
func elapseOpenPeriod(cb *CircuitBreaker) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.openUntil = time.Now().Add(-time.Millisecond)
}

// lapseProbeReservation ages the live probe reservation past openDuration, so
// the next Allow reclaims the slot.
func lapseProbeReservation(cb *CircuitBreaker) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.probeStarted = time.Now().Add(-cb.openDuration - time.Millisecond)
}

// TestCircuitBreakerReportsEachTransitionOnce pins the #634 reporting
// contract step by step: every call returns the transition it caused, the
// reported sequence never repeats a state, and the events that leave the
// breaker where it was reported (extending or renewing an open period,
// reclaiming a lapsed probe, ReleaseProbe and the re-reservation after it)
// return the zero transition.
func TestCircuitBreakerReportsEachTransitionOnce(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Minute, time.Minute)
	none := BreakerTransition{}
	var probe ProbeToken
	allow := func(want bool) func(*testing.T) BreakerTransition {
		return func(t *testing.T) BreakerTransition {
			admitted, token, transition := cb.Allow()
			if admitted != want {
				t.Fatalf("admitted = %v, want %v", admitted, want)
			}
			if token != 0 {
				probe = token
			}
			return transition
		}
	}
	fail := func(*testing.T) BreakerTransition { return cb.RecordFailure() }
	succeed := func(*testing.T) BreakerTransition { return cb.RecordSuccess() }
	releaseThen := func(next func(*testing.T) BreakerTransition) func(*testing.T) BreakerTransition {
		return func(t *testing.T) BreakerTransition {
			cb.ReleaseProbe(probe)
			return next(t)
		}
	}

	steps := []struct {
		name   string
		before func(*CircuitBreaker)
		do     func(*testing.T) BreakerTransition
		want   BreakerTransition
	}{
		{name: "failure below threshold", do: fail, want: none},
		{name: "failure at threshold trips", do: fail, want: BreakerTransition{BreakerClosed, BreakerOpen}},
		{name: "failure while open extends the open period", do: fail, want: none},
		{name: "request while open is rejected", do: allow(false), want: none},
		{name: "probe admitted after the open period", before: elapseOpenPeriod, do: allow(true),
			want: BreakerTransition{BreakerOpen, BreakerHalfOpen}},
		{name: "request during a live probe is rejected", do: allow(false), want: none},
		{name: "lapsed probe slot reclaimed", before: lapseProbeReservation, do: allow(true), want: none},
		{name: "released probe re-reserved by the #251 retry", do: releaseThen(allow(true)), want: none},
		{name: "probe failure re-opens", do: fail, want: BreakerTransition{BreakerHalfOpen, BreakerOpen}},
		{name: "stale failure renews an elapsed open period no probe entered", before: elapseOpenPeriod, do: fail, want: none},
		{name: "probe admitted again", before: elapseOpenPeriod, do: allow(true),
			want: BreakerTransition{BreakerOpen, BreakerHalfOpen}},
		{name: "stale failure after a released probe re-opens", do: releaseThen(fail),
			want: BreakerTransition{BreakerHalfOpen, BreakerOpen}},
		{name: "stale success closes from open", do: succeed, want: BreakerTransition{BreakerOpen, BreakerClosed}},
		{name: "success while closed", do: succeed, want: none},
		{name: "request while closed", do: allow(true), want: none},
		{name: "failure below threshold after close", do: fail, want: none},
		{name: "trips again", do: fail, want: BreakerTransition{BreakerClosed, BreakerOpen}},
		{name: "probe admitted", before: elapseOpenPeriod, do: allow(true),
			want: BreakerTransition{BreakerOpen, BreakerHalfOpen}},
		{name: "probe success closes", do: succeed, want: BreakerTransition{BreakerHalfOpen, BreakerClosed}},
	}
	for _, step := range steps {
		if step.before != nil {
			step.before(cb)
		}
		if got := step.do(t); got != step.want {
			t.Fatalf("%s: transition = %v→%v, want %v→%v", step.name, got.From, got.To, step.want.From, step.want.To)
		}
	}
}

func TestBreakerStateLabels(t *testing.T) {
	for state, want := range map[BreakerState]string{
		BreakerClosed: "closed", BreakerOpen: "open", BreakerHalfOpen: "half_open",
	} {
		if got := state.String(); got != want {
			t.Errorf("BreakerState(%d).String() = %q, want %q", int(state), got, want)
		}
	}
}

func TestCircuitBreakerNilReportsNoTransition(t *testing.T) {
	var cb *CircuitBreaker
	admitted, probe, transition := cb.Allow()
	if !admitted || probe != 0 || transition.Changed() {
		t.Fatalf("nil breaker Allow = (%v, %d, %v), want (true, 0, no transition)", admitted, probe, transition)
	}
	if cb.RecordFailure().Changed() || cb.RecordSuccess().Changed() {
		t.Fatal("nil breaker must report no transition")
	}
}

// TestCircuitBreakerConcurrentTransitionsFormOneChain drives the breaker from
// many goroutines and checks that the returned transitions, in whatever
// order they are collected, are the edges of one walk that starts closed and
// ends at the state the breaker last reported. That is what makes the
// counter's per-state totals exact even when concurrent emissions reach the
// emitter out of order.
func TestCircuitBreakerConcurrentTransitionsFormOneChain(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, time.Millisecond)
	var mu sync.Mutex
	var got []BreakerTransition
	keep := func(tr BreakerTransition) {
		if !tr.Changed() {
			return
		}
		mu.Lock()
		got = append(got, tr)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 500; i++ {
				switch rng.Intn(4) {
				case 0:
					keep(cb.RecordSuccess())
				case 1, 2:
					keep(cb.RecordFailure())
				default:
					_, probe, tr := cb.Allow()
					keep(tr)
					if rng.Intn(2) == 0 {
						cb.ReleaseProbe(probe)
					}
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// For a walk from start to end, out-degree minus in-degree is +1 at the
	// start, -1 at the end and 0 elsewhere (all 0 when start == end).
	balance := map[BreakerState]int{}
	for _, tr := range got {
		balance[tr.From]++
		balance[tr.To]--
	}
	balance[BreakerClosed]--
	balance[cb.reported]++
	for state, b := range balance {
		if b != 0 {
			t.Fatalf("transitions do not form one walk from closed to %v: %v is off by %d (%d transitions)",
				cb.reported, state, b, len(got))
		}
	}
	if len(got) == 0 {
		t.Fatal("the workload produced no transitions; the check proved nothing")
	}
}
