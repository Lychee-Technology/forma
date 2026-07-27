package redact

// Error returns err with any connection-string password in its message replaced
// by Placeholder. A nil error, or one whose message carries no credential, is
// returned unchanged — the scrub allocates only when it actually matched.
//
// The wrapper rewrites only the composed message. It Unwraps to the original
// error, so errors.Is/errors.As classification (sentinels, typed carriers,
// context.Canceled) is unaffected, and any later fmt.Errorf("…: %w", err) wrap
// builds its message from the scrubbed text. Residual, deliberate: the raw
// message stays reachable by explicitly unwrapping to the leaf and calling
// Error() on it — no composed message ever contains it, which is the boundary
// #306 requires (the credential must not enter an error chain's text).
func Error(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	scrubbed := ConnStringPassword(msg)
	if scrubbed == msg {
		return err
	}
	return &redactedError{msg: scrubbed, err: err}
}

type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }
