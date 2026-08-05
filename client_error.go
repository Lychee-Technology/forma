package forma

import (
	"errors"
	"fmt"
)

// Typed public client messages (#313).
//
// A client-facing error carries two messages with different audiences. Error()
// is the operator's copy: the full chain, safe to log, never safe to publish.
// PublicMessage() is the only text that may cross a public transport, and it
// exists only because a wrap site deliberately authored it. The HTTP boundary
// (internal/httpapi) resolves the outermost PublicError in the chain and emits
// nothing else on 4xx — an error built by wrapping a bare sentinel keeps its
// status but loses its body. Deny-by-default on the disclosure axis, matching
// what sentinel classification already does on the status axis (#301).
//
// Constructors return error rather than a concrete type so no call site can
// hold a typed nil or reach past the interface. The carriers stay unexported:
// the contract is PublicError plus errors.Is on the sentinel, nothing more.

// PublicError is implemented by an error that carries a message deliberately
// published to API clients.
type PublicError interface {
	error
	PublicMessage() string
}

// ResolvePublicMessage returns the message err deliberately published, if
// any. It is the canonical resolution used by the HTTP boundary, by
// WrapPublicf/WithOperatorDetail to qualify their input, and by the
// decorators' own PublicMessage delegation — one traversal, so the
// publication a decorator carries is exactly the one the boundary would emit.
//
// The walk is preorder, left-first (errors.As order, so wrap prefixes
// accumulate outermost-wins), and each node is matched the way errors.As
// matches: direct implementation or the node's As(any) bool protocol
// (#363 review, P2). A node qualifies only when a client sentinel is
// reachable from that node's OWN subtree (#362/#363 reviews, P1): two gates
// searching the whole tree independently let a mixed tree borrow — sentinel
// evidence from one branch, PublicMessage() from a foreign sibling — and
// decorating such a tree must not manufacture the same borrow one level up.
// Non-qualifying nodes are stepped over, not terminal, so a real carrier
// behind a foreign publisher still resolves. An empty publication is treated
// as no publication and the walk continues past it.
func ResolvePublicMessage(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if pub, ok := publicErrorAt(err); ok && carriesClientSentinel(err) {
		if msg := pub.PublicMessage(); msg != "" {
			return msg, true
		}
	}
	switch u := err.(type) {
	case interface{ Unwrap() error }:
		return ResolvePublicMessage(u.Unwrap())
	case interface{ Unwrap() []error }:
		for _, cause := range u.Unwrap() {
			if msg, ok := ResolvePublicMessage(cause); ok {
				return msg, true
			}
		}
	}
	return "", false
}

// publicErrorAt matches a single node the way errors.As matches nodes,
// without descending: direct interface satisfaction first, then the node's
// own As(any) bool protocol.
func publicErrorAt(err error) (PublicError, bool) {
	if pub, ok := err.(PublicError); ok {
		return pub, true
	}
	if as, ok := err.(interface{ As(any) bool }); ok {
		var pub PublicError
		if as.As(&pub) && pub != nil {
			return pub, true
		}
	}
	return nil, false
}

// carriesClientSentinel reports whether err's subtree proves caller fault —
// the same three sentinels internal/httpapi classifies status on.
func carriesClientSentinel(err error) bool {
	return errors.Is(err, ErrInvalidInput) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrConflict)
}

// InvalidInputf builds a client error classified by ErrInvalidInput whose
// formatted message is published verbatim. Error() renders
// "<message>: invalid input".
func InvalidInputf(format string, args ...any) error {
	return &clientError{sentinel: ErrInvalidInput, public: fmt.Sprintf(format, args...)}
}

// NotFoundf builds a client error classified by ErrNotFound. Error() renders
// "<message>: not found".
func NotFoundf(format string, args ...any) error {
	return &clientError{sentinel: ErrNotFound, public: fmt.Sprintf(format, args...)}
}

// Conflictf builds a client error classified by ErrConflict. Error() renders
// "<message>: conflict".
func Conflictf(format string, args ...any) error {
	return &clientError{sentinel: ErrConflict, public: fmt.Sprintf(format, args...)}
}

// WrapPublicf adds caller-actionable context to err, prefixing BOTH its
// operator message and its published message: Error() is identical to
// fmt.Errorf("<prefix>: %w", err) either way. When err carries no PublicError
// the result IS exactly that plain wrap — an operator error can never gain a
// publication by being wrapped. Returns nil for a nil err.
//
// Use it only where the wrap adds identification the caller can act on (a
// batch index, a caller-supplied name). A layer that adds operator context
// should stay a plain fmt.Errorf so its prefix stays out of the body.
func WrapPublicf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	prefix := fmt.Sprintf(format, args...)
	// Qualification is the canonical resolution, not a bare errors.As: an
	// input whose publication ResolvePublicMessage would reject — a foreign
	// PublicError in a sibling branch of a bare sentinel — must degrade to
	// the plain wrap, or the decorator would manufacture the very borrow the
	// resolver exists to prevent (#363 review, P1).
	if _, ok := ResolvePublicMessage(err); !ok {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return &publicWrap{prefix: prefix, err: err}
}

// WithOperatorDetail attaches operator-only detail to a client error. The
// detail joins the chain (errors.Is/As still see it) and its text reaches
// Error(), so logs keep it — but it never reaches PublicMessage(). Returns err
// unchanged when detail is nil or err carries no PublicError, and nil for a
// nil err.
func WithOperatorDetail(err error, detail error) error {
	if err == nil {
		return nil
	}
	if detail == nil {
		return err
	}
	// Same qualification rule as WrapPublicf (#363 review, P1).
	if _, ok := ResolvePublicMessage(err); !ok {
		return err
	}
	return &operatorDetail{err: err, detail: detail}
}

// HasOperatorDetail reports whether err withholds detail from its published
// message. internal/httpapi keys a log level on it: a disclosed 4xx whose
// chain still holds operator text makes the log line the only copy of that
// text.
func HasOperatorDetail(err error) bool {
	var d *operatorDetail
	return errors.As(err, &d)
}

// clientError is the leaf carrier: a sentinel for classification and a
// deliberately published message. Error() reproduces the pre-#313 wrap shape
// fmt.Errorf("<message>: %w", sentinel) byte for byte, which is what keeps
// message-text assertions outside internal/httpapi green across the
// conversion.
type clientError struct {
	sentinel error
	public   string
}

func (e *clientError) Error() string         { return e.public + ": " + e.sentinel.Error() }
func (e *clientError) PublicMessage() string { return e.public }
func (e *clientError) Unwrap() error         { return e.sentinel }

// publicWrap prefixes both messages of an error whose publication
// ResolvePublicMessage accepted; WrapPublicf guarantees that at construction.
// Being outermost, it is the node the resolver finds first, so prefixes
// accumulate outside-in exactly as they do in Error(). Delegation re-runs the
// canonical resolution — never a whole-tree errors.As, which would adopt a
// foreign sibling's text (#363 review, P1). The structure is immutable, so
// the empty-string fallback is unreachable; if it were reached, the boundary
// treats an empty publication as none and denies.
type publicWrap struct {
	prefix string
	err    error
}

func (e *publicWrap) Error() string { return e.prefix + ": " + e.err.Error() }
func (e *publicWrap) Unwrap() error { return e.err }

func (e *publicWrap) PublicMessage() string {
	msg, ok := ResolvePublicMessage(e.err)
	if !ok {
		return ""
	}
	return e.prefix + ": " + msg
}

// operatorDetail joins operator-only detail to a publishing error. The detail
// is a real chain member — errors.Is/As descend into it — but PublicMessage()
// delegates to the publishing side only, so the detail's text stays in
// Error() and out of every body.
type operatorDetail struct {
	err    error
	detail error
}

func (e *operatorDetail) Error() string   { return e.err.Error() + ": " + e.detail.Error() }
func (e *operatorDetail) Unwrap() []error { return []error{e.err, e.detail} }

func (e *operatorDetail) PublicMessage() string {
	msg, _ := ResolvePublicMessage(e.err)
	return msg
}
