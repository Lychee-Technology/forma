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

// bodyReadError marks a failure that came from the wire rather than from the
// parser: the request body could not be read to its end. It is decided by
// provenance (bodyReader tags every non-EOF error the transport returns), not
// by guessing at error types, so a timeout on http.Server.ReadTimeout or a
// reset connection is never mistaken for malformed JSON and published as
// caller feedback (#465 review).
type bodyReadError struct {
	cause error
}

func (e *bodyReadError) Error() string { return "failed to read request body: " + e.cause.Error() }
func (e *bodyReadError) Unwrap() error { return e.cause }

// timeout reports whether the transport gave up waiting for the body: the
// server's ReadTimeout expired (net/http surfaces it as a net.Error whose
// Timeout() is true) while the caller was still sending.
func (e *bodyReadError) timeout() bool {
	var netErr net.Error
	return errors.As(e.cause, &netErr) && netErr.Timeout()
}

// bodyReader tags the transport's read failures as bodyReadError. It sits
// inside http.MaxBytesReader, so *http.MaxBytesError (which the outer layer
// authors) is never tagged, and io.EOF passes through untouched because both
// the decoder and drainBody treat it as the end of the body.
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

// drainBody consumes rest, the bytes after the first JSON value, to EOF
// through the same capped reader the decoder used, so that every byte of the
// body counts against the cap. It reads to the end even after seeing
// trailing data, because the cap is the stronger verdict: a body that both
// carries a second value and overruns the limit is answered as over-limit
// (*http.MaxBytesError, a 413), matching the documented contract that a body
// past the cap is always 413 (#465 review). Only when the whole tail fit
// under the cap does trailing non-whitespace become errTrailingBodyValue.
func drainBody(rest io.Reader) error {
	var buf [512]byte
	trailing := false
	for {
		n, err := rest.Read(buf[:])
		if !trailing && !isJSONWhitespace(buf[:n]) {
			trailing = true
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if trailing {
		return errTrailingBodyValue
	}
	return nil
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
//   - The transport failed before the body arrived (bodyReadError): a
//     redacted answer, since the error is network prose and not JSON
//     feedback. A read timeout — http.Server.ReadTimeout expired while the
//     body was still coming — is a 408 (RFC 9110 §15.5.9: the server did not
//     receive a complete request in the time it was prepared to wait; the
//     client may repeat it), sent with Connection: close because the request
//     stream is unrecoverable. Any other transport failure is the client
//     going away and keeps the 500 that context.Canceled earns on the
//     manager path: nobody reads that status.
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
		status := http.StatusInternalServerError
		if readErr.timeout() {
			status = http.StatusRequestTimeout
			w.Header().Set("Connection", "close")
		}
		respondErrorWithStatus(w, status, "read request body", err)
		return
	}
	respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
}
