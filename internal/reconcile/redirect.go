package reconcile

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// defaultResponseHeaderTimeout bounds how long a collector waits for S3
// response headers without capping streaming body reads during checksum scrubs.
const defaultResponseHeaderTimeout = 30 * time.Second

// ErrRedirectRefused reports an S3 request answered with a redirect. The
// collector follows no redirects so a destructive request can never be routed
// beyond the bound destination.
var ErrRedirectRefused = errors.New("the S3 request was redirected, and manifest-reconcile follows no redirect")

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
// pass or schema lock), and returns a redacted non-retryable redirectError.
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
		r.Method, safeURL(r.URL), status)}
}

func safeURL(u *url.URL) string {
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
		h, errUnknownDestination)
}

// NewS3Objects returns an S3Objects over a copy of c that refuses every HTTP
// redirect before following it. Only stores constructed here report a bound
// Destination.Endpoint.
func NewS3Objects(c *s3.Client, bucket string) (*S3Objects, error) {
	o := c.Options()
	h, err := withoutRedirects(o.HTTPClient)
	if err != nil {
		return nil, err
	}
	return &S3Objects{Client: s3.New(o, func(o *s3.Options) { o.HTTPClient = h }), Bucket: bucket}, nil
}

func followsNoRedirect(c *s3.Client) bool {
	_, ok := c.Options().HTTPClient.(boundHTTPClient)
	return ok
}
