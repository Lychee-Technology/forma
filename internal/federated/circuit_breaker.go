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
func (cb *CircuitBreaker) Allow() (admitted bool, probe ProbeToken) {
	if cb == nil {
		return true, 0
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if now.Before(cb.openUntil) {
		return false, 0
	}
	if cb.openUntil.IsZero() && !cb.probing {
		return true, 0 // closed: nothing to recover from
	}
	if cb.probing && now.Sub(cb.probeStarted) < cb.openDuration {
		return false, 0 // half-open with a live probe in flight
	}
	// Half-open with a free (or lapsed) probe slot: this caller is the probe.
	cb.probing = true
	cb.probeStarted = now
	cb.probeSeq++
	cb.probeToken = cb.probeSeq
	return true, cb.probeToken
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
// open the breaker at threshold.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
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
		return
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

	if len(cb.failures) >= cb.threshold {
		// open the breaker
		cb.openUntil = now.Add(cb.openDuration)
	}
}

// RecordSuccess closes the breaker and clears the failure history. It
// resolves an in-flight probe, and also closes from open state when a
// pre-open in-flight query completes (see the type doc on stale callers).
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = cb.failures[:0]
	cb.openUntil = time.Time{}
	cb.probing = false
}

// IsOpen reports whether the timed open period is currently active.
// Observation only (tests, telemetry): admission control — including the
// half-open probe reservation — lives in Allow.
func (cb *CircuitBreaker) IsOpen() bool {
	if cb == nil {
		return false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return time.Now().Before(cb.openUntil)
}
