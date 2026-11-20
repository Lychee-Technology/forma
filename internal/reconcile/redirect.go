package reconcile

import (
	"errors"
	"fmt"
	"net/http"

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
type boundHTTPClient struct{ c *http.Client }

// Do sends r. Its error is http.Client's *url.Error, which names the
// method and the URL already, and which the SDK's retryer classifies.
func (b boundHTTPClient) Do(r *http.Request) (*http.Response, error) { return b.c.Do(r) }

// refuseRedirect is the CheckRedirect of a boundHTTPClient: every redirect
// fails the request before the redirected one is sent.
func refuseRedirect(next *http.Request, via []*http.Request) error {
	status := 0
	if next.Response != nil {
		status = next.Response.StatusCode
	}
	return &redirectError{msg: fmt.Sprintf("%s %s answered %d with a redirect to %s",
		via[0].Method, via[len(via)-1].URL.Redacted(), status, next.URL.Redacted())}
}

// redirectError is a refused redirect. It is ErrRedirectRefused, and the
// SDK's retryer does not send the request again: the same server answers
// it with the same redirect.
type redirectError struct{ msg string }

func (e *redirectError) Error() string        { return e.msg + ": " + ErrRedirectRefused.Error() }
func (e *redirectError) Unwrap() error        { return ErrRedirectRefused }
func (e *redirectError) RetryableError() bool { return false }

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
		return boundHTTPClient{&http.Client{Transport: b.GetTransport(), Timeout: b.GetTimeout(), CheckRedirect: refuseRedirect}}, nil
	case *awshttp.BuildableClient:
		return boundHTTPClient{&http.Client{Transport: c.GetTransport(), Timeout: c.GetTimeout(), CheckRedirect: refuseRedirect}}, nil
	case *http.Client:
		cp := *c
		cp.CheckRedirect = refuseRedirect
		return boundHTTPClient{&cp}, nil
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
