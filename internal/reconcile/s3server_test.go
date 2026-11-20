package reconcile

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
)

// s3Server serves a memStore as one path-style bucket over the S3 REST
// calls an S3Objects makes: ListObjectsV2, GetObject, HeadObject and
// DeleteObject. redirect, when it answers a URL for a request, answers the
// request with a 307 to it instead, as a misconfigured gateway or a hostile
// server would.
type s3Server struct {
	store    *memStore
	bucket   string
	mu       sync.Mutex
	redirect func(r *http.Request) string
}

func (s *s3Server) setRedirect(fn func(r *http.Request) string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redirect = fn
}

type listResult struct {
	XMLName     xml.Name     `xml:"ListBucketResult"`
	Name        string       `xml:"Name"`
	Prefix      string       `xml:"Prefix"`
	KeyCount    int          `xml:"KeyCount"`
	IsTruncated bool         `xml:"IsTruncated"`
	Contents    []listObject `xml:"Contents"`
}

type listObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int    `xml:"Size"`
}

func (s *s3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	redirect := s.redirect
	s.mu.Unlock()
	if redirect != nil {
		if to := redirect(r); to != "" {
			http.Redirect(w, r, to, http.StatusTemporaryRedirect)
			return
		}
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+s.bucket), "/")
	ctx := r.Context()
	switch {
	case r.Method == http.MethodGet && key == "":
		listed, _ := s.store.List(ctx, r.URL.Query().Get("prefix"))
		res := listResult{Name: s.bucket, Prefix: r.URL.Query().Get("prefix"), KeyCount: len(listed)}
		for _, o := range listed {
			raw, _ := s.store.Get(ctx, o.Key)
			res.Contents = append(res.Contents, listObject{Key: o.Key, Size: len(raw),
				LastModified: time.UnixMilli(o.LastModified).UTC().Format("2006-01-02T15:04:05.000Z")})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(res)
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
		raw, err := s.store.Get(ctx, key)
		if errors.Is(err, manifestv2.ErrObjectNotFound) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			if r.Method == http.MethodGet {
				_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			}
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(raw)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(raw)
		}
	case r.Method == http.MethodDelete:
		if err := s.store.Delete(ctx, key); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// otherRoute is a server a redirect points at: it records every request
// that reaches it and answers each as S3 answers a delete.
type otherRoute struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

func newOtherRoute(t *testing.T) *otherRoute {
	o := &otherRoute{}
	o.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.seen = append(o.seen, r.Method+" "+r.URL.Path)
		o.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(o.Close)
	return o
}

func (o *otherRoute) requests() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seen...)
}

// sdkClient answers an S3 client of the bucket at the server's URL,
// path-style, as the AWS SDK builds one from a loaded configuration: its
// default HTTP client, which follows 307 and 308 redirects, included.
func sdkClient(url string) *s3.Client {
	return s3.NewFromConfig(aws.Config{
		Region:     "us-east-1",
		HTTPClient: awshttp.NewBuildableClient(),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
	})
}

// redirectTo answers a redirect rule sending every request of method to
// the same path on other.
func redirectTo(method string, other *otherRoute) func(r *http.Request) string {
	return func(r *http.Request) string {
		if r.Method != method {
			return ""
		}
		return other.URL + r.URL.RequestURI()
	}
}

func newS3Server(t *testing.T, store *memStore, bucket string) (*s3Server, *httptest.Server) {
	s := &s3Server{store: store, bucket: bucket}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	require.NotEmpty(t, srv.URL)
	return s, srv
}
