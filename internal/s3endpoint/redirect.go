package s3endpoint

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// defaultResponseHeaderTimeout bounds how long a request waits for S3
// response headers without capping streaming body reads (a collector's
// checksum scrub, a writer's parent manifest).
const defaultResponseHeaderTimeout = 30 * time.Second

// ErrRedirectRefused reports an S3 request answered with a redirect. Neither
// the collector nor a writer follows one, so no request of theirs reaches a
// route other than the bucket URL ClientEndpoint derives: a collector's
// DELETE never lands outside the bound target, and a writer's PUT is never
// answered by another route's success.
var ErrRedirectRefused = errors.New("the S3 request was redirected, and Forma follows no S3 redirect")

// boundHTTPClient sends through refuseRedirects so http.Client never parses,
// follows or wraps a redirect's Location (which may carry credentials) in a
// *url.Error.
type boundHTTPClient struct{ c *http.Client }

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

// refuseRedirects intercepts every redirect response http.Client would follow,
// closes its body unread immediately (so a stalled redirect body cannot hold a
// pass, a publication or a schema lock), and returns a redacted non-retryable redirectError.
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

func isRedirect(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return resp.Header.Get("Location") != ""
	}
	return false
}

func refuseRedirect(next *http.Request, via []*http.Request) error {
	status := 0
	if next.Response != nil {
		status = next.Response.StatusCode
	}
	return newRedirectError(via[len(via)-1], status)
}

func newRedirectError(r *http.Request, status int) *redirectError {
	return &redirectError{msg: fmt.Sprintf("%s %s answered %d with a redirect (Location omitted)",
		r.Method, SafeURL(r.URL), status)}
}

// SafeURL renders u without userinfo, query or fragment, which may carry
// credentials or signatures.
func SafeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}).String()
}

type redirectError struct{ msg string }

func (e *redirectError) Error() string        { return e.msg + ": " + ErrRedirectRefused.Error() }
func (e *redirectError) Unwrap() error        { return ErrRedirectRefused }
func (e *redirectError) RetryableError() bool { return false }

func bound(rt http.RoundTripper, c http.Client) boundHTTPClient {
	if rt == nil {
		rt = http.DefaultTransport
	}
	if tr, ok := rt.(*http.Transport); ok && tr.ResponseHeaderTimeout == 0 {
		cloned := tr.Clone()
		cloned.ResponseHeaderTimeout = defaultResponseHeaderTimeout
		rt = cloned
	}
	c.Transport = refuseRedirects{next: rt}
	c.CheckRedirect = refuseRedirect
	return boundHTTPClient{&c}
}

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
		h, ErrUnknownDestination)
}

// WithoutRedirects answers a copy of c that refuses every HTTP redirect
// before following it (ErrRedirectRefused), and ErrUnknownDestination for a
// client sending through an HTTP client whose redirects it cannot control.
func WithoutRedirects(c *s3.Client) (*s3.Client, error) {
	o := c.Options()
	h, err := withoutRedirects(o.HTTPClient)
	if err != nil {
		return nil, err
	}
	return s3.New(o, func(o *s3.Options) { o.HTTPClient = h }), nil
}

// FollowsNoRedirect reports whether c was built by WithoutRedirects.
func FollowsNoRedirect(c *s3.Client) bool {
	_, ok := c.Options().HTTPClient.(boundHTTPClient)
	return ok
}
