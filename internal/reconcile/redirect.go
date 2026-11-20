package reconcile

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrRedirectRefused reports an S3 request of a pass that the server
// answered with a redirect. A pass follows none: the storage binding names
// the URL a request is built for (ClientEndpoint), and a redirect would send
// the request, a DELETE included, to a route the binding never named.
var ErrRedirectRefused = errors.New("the S3 request was redirected, and manifest-reconcile follows no redirect")

// boundHTTPClient is the HTTP client a pass's S3 client sends through: it
// follows no redirect. The AWS SDK's own client follows a 307 or a 308 with
// the request's method and body, so a DELETE the bound server redirects
// would reach the redirect's target, and the SDK would report the target's
// answer as the delete's.
//
// Its transport (refuseRedirects) turns every redirect into an error before
// http.Client reads the Location, so http.Client never parses, follows or
// reports it. A Location can carry a credential (a signed URL's query, a
// user-info password), and http.Client would put it whole into the
// *url.Error it answers, which URL.Redacted would not mask (R13-F2).
type boundHTTPClient struct{ c *http.Client }

// Do sends r. A refused redirect answers its *redirectError alone, not the
// *url.Error http.Client wraps it in: that names the URL too, query
// included. Every other error is http.Client's.
func (b boundHTTPClient) Do(r *http.Request) (*http.Response, error) {
	resp, err := b.c.Do(r)
	var refused *redirectError
	if err != nil && errors.As(err, &refused) {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, refused
	}
	return resp, err
}

// refuseRedirects is the transport of a boundHTTPClient. A response that
// http.Client would follow (a 301, 302, 303, 307 or 308 with a Location) is
// closed and answered as a *redirectError, which names neither the Location
// nor the request's query.
//
// The body is closed unread, once the headers are in. Reading even a few
// bytes of it first, to let the transport reuse the connection, bounds the
// bytes and not the time: a server that sends a redirect's headers and
// stalls its body would hold the pass, and the schema's lock, until its
// context ends, the client's timeout being unset by default (R14-F2).
// Closing an unread body closes the connection instead, which costs one
// reconnect on a pass that is failing anyway.
type refuseRedirects struct{ next http.RoundTripper }

func (t refuseRedirects) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(r)
	if err != nil || !isRedirect(resp) {
		return resp, err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	return nil, newRedirectError(r, resp.StatusCode)
}

// isRedirect answers whether http.Client would follow resp.
func isRedirect(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return resp.Header.Get("Location") != ""
	}
	return false
}

// refuseRedirect is the CheckRedirect of a boundHTTPClient, behind its
// transport: should a redirect reach http.Client all the same, it fails
// the request before the redirected one is sent. The transport answers
// every response http.Client would follow, so none should: http.Client
// reads up to 2 KiB of a redirect's body before it reports this refusal.
func refuseRedirect(next *http.Request, via []*http.Request) error {
	status := 0
	if next.Response != nil {
		status = next.Response.StatusCode
	}
	return newRedirectError(via[len(via)-1], status)
}

// newRedirectError answers the refusal of r's redirect. It names r's
// method and its URL without user-info, query or fragment, and never the
// redirect's Location.
func newRedirectError(r *http.Request, status int) *redirectError {
	return &redirectError{msg: fmt.Sprintf("%s %s answered %d with a redirect (its Location is not shown, since it may carry a credential)",
		r.Method, safeURL(r.URL), status)}
}

// safeURL answers u's scheme, host and path only.
func safeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}).String()
}

// redirectError is a refused redirect. It is ErrRedirectRefused, and the
// SDK's retryer does not send the request again: the same server answers
// it with the same redirect.
type redirectError struct{ msg string }

func (e *redirectError) Error() string        { return e.msg + ": " + ErrRedirectRefused.Error() }
func (e *redirectError) Unwrap() error        { return ErrRedirectRefused }
func (e *redirectError) RetryableError() bool { return false }

// bound answers a boundHTTPClient configured as c that sends through rt
// (http.DefaultTransport when nil).
func bound(rt http.RoundTripper, c http.Client) boundHTTPClient {
	if rt == nil {
		rt = http.DefaultTransport
	}
	c.Transport = refuseRedirects{next: rt}
	c.CheckRedirect = refuseRedirect
	return boundHTTPClient{&c}
}

// withoutRedirects answers an HTTP client that sends as h does and follows
// no redirect. h is the client an *s3.Client sends through: the SDK's
// buildable client (the default), a plain *http.Client, or one this package
// built already. Any other cannot be made to refuse redirects, and is
// refused.
func withoutRedirects(h s3.HTTPClient) (boundHTTPClient, error) {
	switch c := h.(type) {
	case boundHTTPClient:
		return c, nil
	case nil:
		b := awshttp.NewBuildableClient()
		return bound(b.GetTransport(), http.Client{Timeout: b.GetTimeout()}), nil
	case *awshttp.BuildableClient:
		return bound(c.GetTransport(), http.Client{Timeout: c.GetTimeout()}), nil
	case *http.Client:
		return bound(c.Transport, *c), nil
	}
	return boundHTTPClient{}, fmt.Errorf("an S3 client sending through an HTTP client of type %T, which may follow redirects: %w",
		h, errUnknownDestination)
}

// NewS3Objects answers the ObjectStore a pass lists, reads and deletes
// through: bucket, through a copy of c whose requests follow no redirect
// (ErrRedirectRefused). Only such a store tells its destination
// (S3Objects.Endpoint): a request that cannot leave the URL it was built
// for goes where the storage binding says, on every hop.
func NewS3Objects(c *s3.Client, bucket string) (*S3Objects, error) {
	o := c.Options()
	h, err := withoutRedirects(o.HTTPClient)
	if err != nil {
		return nil, err
	}
	return &S3Objects{Client: s3.New(o, func(o *s3.Options) { o.HTTPClient = h }), Bucket: bucket}, nil
}

// followsNoRedirect answers whether c sends through a client NewS3Objects
// built.
func followsNoRedirect(c *s3.Client) bool {
	_, ok := c.Options().HTTPClient.(boundHTTPClient)
	return ok
}
