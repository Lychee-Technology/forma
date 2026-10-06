package federated

import (
	"sync"
	"time"
)

// CircuitBreaker is a lightweight in-memory circuit breaker with strict
// single-probe half-open recovery (#246, supersedes the #185
// immediate-forgiveness design).
//
// States (all transitions guarded by mu):
//   - closed: no open period recorded; every caller is admitted.
//   - open (now < openUntil): every caller is rejected.
//   - half-open (openUntil elapsed): Allow admits exactly one caller as the
//     probe and rejects the rest until the probe resolves. RecordSuccess
//     closes the breaker (failure history cleared); RecordFailure re-opens
//     it for a fresh openDuration without threshold re-accumulation.
//
// Probe abandonment: a probe that never reports (its query was cancelled
// between Allow and Record*) lapses openDuration after admission, and the
// next Allow reclaims the slot. A lost probe therefore costs at most one
// extra openDuration of rejections. The flip side: a probe still legitimately
// running past openDuration is indistinguishable from an abandoned one, so a
// slow dependency can see more than one concurrent probe — "exactly one" holds
// only for probes that resolve within openDuration.
//
// Stale callers: RecordSuccess closes the breaker from any state — a query
// admitted before the breaker opened that completes afterwards is real
// evidence the dependency is healthy. A stale RecordFailure landing while a
// probe is in flight re-opens the breaker: conservative, and
// indistinguishable from a probe failure without per-caller tokens.
// ReleaseProbe, by contrast, IS token-scoped (#349 review R2-2): it can run
// long after admission (the #251 post-verification path), by which time the
// probe slot may belong to a newer caller — an unscoped release would free a
// reservation its caller never held and admit a second concurrent probe.
//
// Reporting (#634): Allow, RecordFailure and RecordSuccess return the
// BreakerTransition they caused, computed under mu, so the caller reports it
// after the lock is released — the breaker itself holds no telemetry sink and
// no logger. See reported for which events count.
type CircuitBreaker struct {
	mu           sync.Mutex
	failures     []time.Time
	threshold    int
	window       time.Duration
	openUntil    time.Time
	openDuration time.Duration
	probing      bool
	probeStarted time.Time
	probeSeq     ProbeToken
	probeToken   ProbeToken
	// reported is the state the breaker last reported as a transition. It is
	// not the admission state, which openUntil and probing derive: the lapse
	// of openUntil runs no code, so open turns half-open unobserved. reported
	// moves only at the observable events — a trip or re-open, Allow
	// reserving the probe, RecordSuccess — and never repeats a state, so
	// extending an open period, reclaiming a lapsed probe slot and
	// ReleaseProbe are not transitions. The #251 corrupt-confirmed path
	// releases the probe and its retry re-reserves it in one request; that
	// must not report half-open twice.
	reported BreakerState
}

// BreakerState is a breaker state as the breaker reports it (#634). Its
// String form is the state label of the duckdb_circuit_breaker_transition_total
// metric, so the three values are a fixed enumeration.
type BreakerState int

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

// String returns the metric label value: "closed", "open" or "half_open".
func (s BreakerState) String() string {
	switch s {
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// BreakerTransition is a change of the breaker's reported state, returned by
// the call that caused it. The zero value (closed to closed) means the call
// changed nothing.
type BreakerTransition struct {
	From, To BreakerState
}

// Changed reports whether the call that returned t moved the breaker.
func (t BreakerTransition) Changed() bool { return t.From != t.To }

// report moves the reported state to `to` and returns the transition, or the
// zero transition when the breaker already reported `to`. Callers hold mu.
func (cb *CircuitBreaker) report(to BreakerState) BreakerTransition {
	if cb.reported == to {
		return BreakerTransition{}
	}
	t := BreakerTransition{From: cb.reported, To: to}
	cb.reported = to
	return t
}

// ProbeToken identifies one half-open probe reservation. The zero token means
// "no probe held" — callers admitted while the breaker was closed carry it —
// and releasing it is always a no-op.
type ProbeToken uint64

// NewCircuitBreaker creates a configured circuit breaker.
func NewCircuitBreaker(threshold int, window, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		window:       window,
		openDuration: openDuration,
		failures:     make([]time.Time, 0, threshold),
	}
}

// Allow reports whether the caller may proceed. In half-open state it
// atomically reserves the single probe slot: the one caller that receives
// admitted=true with a non-zero token while others are rejected MUST resolve
// the probe via RecordSuccess or RecordFailure (or let the reservation lapse
// after openDuration). Callers admitted while the breaker is closed receive
// the zero token: they hold no reservation, and their ReleaseProbe is a no-op.
// Reserving the probe after the open period is the breaker's first observable
// half-open moment, so that admission returns the open-to-half-open
// transition.
func (cb *CircuitBreaker) Allow() (admitted bool, probe ProbeToken, transition BreakerTransition) {
	if cb == nil {
		return true, 0, BreakerTransition{}
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if now.Before(cb.openUntil) {
		return false, 0, BreakerTransition{}
	}
	if cb.openUntil.IsZero() && !cb.probing {
		return true, 0, BreakerTransition{} // closed: nothing to recover from
	}
	if cb.probing && now.Sub(cb.probeStarted) < cb.openDuration {
		return false, 0, BreakerTransition{} // half-open with a live probe in flight
	}
	// Half-open with a free (or lapsed) probe slot: this caller is the probe.
	cb.probing = true
	cb.probeStarted = now
	cb.probeSeq++
	cb.probeToken = cb.probeSeq
	return true, cb.probeToken, cb.report(BreakerHalfOpen)
}

// ReleaseProbe relinquishes a half-open probe reservation without recording
// evidence either way, freeing the slot for the next caller. It is for callers
// admitted by Allow that then failed BEFORE touching the dependency — a
// misconfiguration caught during path resolution, invalid caller input — so
// they learned nothing about its health and must not consume the probe. It is
// also for the one post-execution caller whose failure indicts a specific
// object rather than the dependency: the #251 corrupt-confirmed path, where
// per-file verification has just drained every OTHER object through this same
// engine and store — live proof of health that makes RecordFailure dishonest,
// while the query as a whole still failed, making RecordSuccess equally so.
//
// Without this, such a caller abandons the reservation: the slot stays occupied
// until it lapses (openDuration), and every request in that window is rejected
// with ErrDuckDBUnavailable. That matters because ErrDuckDBUnavailable IS
// degradable while the pre-execution errors are deliberately not, so a
// misconfiguration that must always be loud would be answered from Postgres
// alone on the very next request (#299 review P1).
//
// Neutral by design: the breaker stays open, so a real outage still gates
// traffic; only the probe slot returns. Recording success here would close the
// breaker on no evidence, and recording failure would extend the outage for a
// dependency that was never consulted.
//
// Release is scoped to the caller's own reservation (#349 review R2-2): only
// the token Allow handed out frees the slot, so a caller admitted while the
// breaker was closed (zero token) — or one whose lapsed reservation was
// already reclaimed by a newer probe — cannot clear the probe another caller
// is running.
func (cb *CircuitBreaker) ReleaseProbe(probe ProbeToken) {
	if cb == nil || probe == 0 {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.probing && cb.probeToken == probe {
		cb.probing = false
	}
}

// RecordFailure records a failure occurrence. A probe failure re-opens the
// breaker directly; otherwise failures accumulate in the sliding window and
// open the breaker at threshold. It returns the transition to open when the
// breaker was not already reported open.
func (cb *CircuitBreaker) RecordFailure() BreakerTransition {
	if cb == nil {
		return BreakerTransition{}
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if cb.probing {
		// Probe failure: re-open for a fresh openDuration. The failure
		// window is not consulted — half-open never re-accumulates the
		// threshold (#246).
		cb.probing = false
		cb.openUntil = now.Add(cb.openDuration)
		return cb.report(BreakerOpen)
	}

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

	if len(cb.failures) < cb.threshold {
		return BreakerTransition{}
	}
	// Open the breaker, or renew an open period: a stale failure landing
	// while already reported open is not a transition.
	cb.openUntil = now.Add(cb.openDuration)
	return cb.report(BreakerOpen)
}

// RecordSuccess closes the breaker and clears the failure history. It
// resolves an in-flight probe, and also closes from open state when a
// pre-open in-flight query completes (see the type doc on stale callers).
// It returns the transition to closed unless the breaker already was.
func (cb *CircuitBreaker) RecordSuccess() BreakerTransition {
	if cb == nil {
		return BreakerTransition{}
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = cb.failures[:0]
	cb.openUntil = time.Time{}
	cb.probing = false
	return cb.report(BreakerClosed)
}

// IsOpen reports whether the timed open period is currently active.
// Observation only (tests): admission control — including the half-open
// probe reservation — lives in Allow, and state changes are reported through
// the transitions Allow, RecordFailure and RecordSuccess return.
func (cb *CircuitBreaker) IsOpen() bool {
	if cb == nil {
		return false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return time.Now().Before(cb.openUntil)
}
