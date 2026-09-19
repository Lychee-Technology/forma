package httpapi

import (
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/lychee-technology/forma"
)

// defaultMaxBodyBytes is the request-body cap a Server applies when Options
// names none: forma's declared entity size limit (#465). The cap is applied
// before the decoder reads a byte past it, so an oversized body is refused
// without ever being materialized.
var defaultMaxBodyBytes = int64(forma.DefaultConfig(nil).Entity.MaxEntitySize)

// bodyLimit returns the number of bytes a request body may carry.
func (s *Server) bodyLimit() int64 {
	if s.opts.MaxBodyBytes > 0 {
		return s.opts.MaxBodyBytes
	}
	return defaultMaxBodyBytes
}

// bodyReadError marks a failure that came from r.Body rather than from the
// parser: net/http could not deliver the request body to its end. It is
// decided by provenance (bodyReader tags every non-EOF error the body
// returns), not by guessing at error types, so a timeout on
// http.Server.ReadTimeout or a reset connection is never mistaken for
// malformed JSON and published as caller feedback (#465 review). Which of
// the transport and the caller is at fault is a second question, answered by
// transport and timeout below.
type bodyReadError struct {
	cause error
}

func (e *bodyReadError) Error() string { return "failed to read request body: " + e.cause.Error() }
func (e *bodyReadError) Unwrap() error { return e.cause }

// transport reports whether the failure is the connection's, not the
// request's: a net.Error (a deadline, a reset, a closed socket) or a body
// that ended before the declared length (io.ErrUnexpectedEOF, which net/http
// raises for both content-length and chunked bodies). Everything else
// r.Body.Read can return is net/http refusing to frame what the client sent
// — malformed chunked encoding, a chunk line too long — and that is caller
// input, however it is spelled (#465 review). Transport failures are
// recognised positively because net/http's framing errors are untyped; an
// unrecognised failure therefore lands on the caller's side, where the
// client is at least still connected to read the answer.
func (e *bodyReadError) transport() bool {
	var netErr net.Error
	return errors.As(e.cause, &netErr) || errors.Is(e.cause, io.ErrUnexpectedEOF)
}

// timeout reports whether the transport gave up waiting for the body: the
// server's ReadTimeout expired (net/http surfaces it as a net.Error whose
// Timeout() is true) while the caller was still sending.
func (e *bodyReadError) timeout() bool {
	var netErr net.Error
	return errors.As(e.cause, &netErr) && netErr.Timeout()
}

// bodyReader tags the body's read failures as bodyReadError. It sits inside
// http.MaxBytesReader, so *http.MaxBytesError (which the outer layer authors)
// is never tagged, and io.EOF passes through untouched because both the
// decoder and drainBody treat it as the end of the body.
type bodyReader struct {
	io.ReadCloser
}

func (b bodyReader) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, &bodyReadError{cause: err}
	}
	return n, err
}

// errTrailingBodyValue is the decode failure for a body that carries
// anything but JSON whitespace after its first value; like encoding/json's
// own prose it is caller-addressed and published verbatim by respondBodyError.
var errTrailingBodyValue = errors.New("unexpected data after the JSON body")

// isTerminalBodyError reports whether err already is the capped stream's
// terminal verdict — the cap (*http.MaxBytesError) or the body's own read
// failure (bodyReadError) — after which there is nothing left to read.
func isTerminalBodyError(err error) bool {
	var tooLarge *http.MaxBytesError
	var readErr *bodyReadError
	return errors.As(err, &tooLarge) || errors.As(err, &readErr)
}

// settleDecodeError decides the answer for a body the decoder refused. A
// parse error is a verdict on the bytes read so far, but the cap is a
// verdict on the whole body, so the capped stream is read to its terminal
// result before choosing (#465 review): a malformed prefix under the cap
// followed by bytes past it is 413, not 400, exactly as it would be with a
// well-formed prefix. The same holds for a read failure met while draining.
// Only when the rest of the body arrives whole and under the cap does the
// parse error stand.
func settleDecodeError(parseErr error, rest io.Reader) error {
	if isTerminalBodyError(parseErr) {
		return parseErr
	}
	if _, err := consumeBody(rest); err != nil {
		return err
	}
	return parseErr
}

// drainBody consumes rest, the bytes after the first JSON value, to EOF
// through the same capped reader the decoder used, so that every byte of the
// body counts against the cap. It reads to the end even after seeing
// trailing data, because the cap is the stronger verdict: a body that both
// carries a second value and overruns the limit is answered as over-limit
// (*http.MaxBytesError, a 413), matching the documented contract that a body
// past the cap is always 413 (#465 review). Only when the whole tail fit
// under the cap does trailing non-whitespace become errTrailingBodyValue.
func drainBody(rest io.Reader) error {
	trailing, err := consumeBody(rest)
	if err != nil {
		return err
	}
	if trailing {
		return errTrailingBodyValue
	}
	return nil
}

// consumeBody reads rest to its terminal result: EOF, in which case it
// reports whether anything but JSON whitespace was seen, or the stream's own
// error (the cap or a read failure), which it returns as is.
func consumeBody(rest io.Reader) (bool, error) {
	var buf [512]byte
	trailing := false
	for {
		n, err := rest.Read(buf[:])
		if !trailing && !isJSONWhitespace(buf[:n]) {
			trailing = true
		}
		if errors.Is(err, io.EOF) {
			return trailing, nil
		}
		if err != nil {
			return trailing, err
		}
	}
}

// isJSONWhitespace reports whether b holds only the four whitespace bytes
// RFC 8259 allows between values; encoding/json skips exactly these.
func isJSONWhitespace(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return false
		}
	}
	return true
}

// respondBodyError answers a failed body read or decode. The verdicts, in
// order:
//
//   - The body overran the cap: a 413 whose published message names the
//     limit the caller has to fit under; the message is authored here from
//     the limit alone, never from request text.
//   - The transport failed before the body arrived (a bodyReadError whose
//     cause is the connection's): a redacted answer, since the error is
//     network prose and not JSON feedback. A read timeout —
//     http.Server.ReadTimeout expired while the body was still coming — is
//     a 408 (RFC 9110 §15.5.9: the server did not receive a complete request
//     in the time it was prepared to wait; the client may repeat it), sent
//     with Connection: close because the request stream is unrecoverable.
//     Any other transport failure is the client going away and keeps the
//     500 that context.Canceled earns on the manager path: nobody reads
//     that status.
//   - net/http could not frame the body (every other bodyReadError:
//     malformed chunked encoding and the like): the caller's request is
//     malformed, so a disclosed 400 with a message authored here. net/http's
//     own prose is operator detail — it names no caller-actionable field the
//     way encoding/json's does — and stays on the log line (#465 review).
//   - Every other decode failure keeps the #360 shape: a disclosed 400 built
//     from encoding/json's own prose (or errTrailingBodyValue).
func respondBodyError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		respondErrorWithStatus(w, http.StatusRequestEntityTooLarge, "request body too large",
			forma.InvalidInputf("request body exceeds %d bytes", tooLarge.Limit))
		return
	}
	var readErr *bodyReadError
	if errors.As(err, &readErr) {
		respondBodyReadError(w, readErr)
		return
	}
	respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
}

// respondBodyReadError is respondBodyError's bodyReadError branch: the
// transport's failures take the redacted branch (408 for a read timeout,
// 500 otherwise), a framing failure is the caller's 400.
func respondBodyReadError(w http.ResponseWriter, readErr *bodyReadError) {
	switch {
	case readErr.timeout():
		w.Header().Set("Connection", "close")
		respondErrorWithStatus(w, http.StatusRequestTimeout, "read request body", readErr)
	case readErr.transport():
		respondErrorWithStatus(w, http.StatusInternalServerError, "read request body", readErr)
	default:
		respondErrorWithStatus(w, http.StatusBadRequest, "read request body",
			forma.WithOperatorDetail(forma.InvalidInputf("malformed request body"), readErr))
	}
}
